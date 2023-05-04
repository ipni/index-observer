package task

import (
	"context"
	"errors"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	finderhttpclient "github.com/ipni/go-libipni/find/client/http"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/index-observer/progress_observer"
	"github.com/urfave/cli/v2"
)

const (
	countsObserveFreq = 10 * time.Minute
	lagsObserveFreq   = 20 * time.Minute
	timerFreq         = 10 * time.Minute
)

var log = logging.Logger("task")

func Start(c *cli.Context) error {
	logging.SetLogLevel("*", "warn")
	logging.SetLogLevel("progress_observer", "info")

	if err := StartMetrics(c); err != nil {
		return err
	}

	sources := c.StringSlice("sources")
	targets := c.StringSlice("targets")

	pl := NewProviderList(c.Context)
	indexers := c.StringSlice("indexer")
	wg := sync.WaitGroup{}
	ec := make(chan error)
	errSummary := make(chan error)

	if len(sources) != len(targets) {
		return errors.New("sources and targets slices should be of the same length")
	}

	if len(sources) > 0 {
		for i := range sources {
			source := sources[i]
			target := targets[i]
			wg.Add(1)
			go func() {
				observeLags(c.Context, source, target)
				wg.Done()
			}()
			wg.Add(1)
			go func() {
				observeCounts(c.Context, source, target)
				wg.Done()
			}()
		}
	}
	go summarizeErrors(ec, errSummary)
	for _, indexer := range indexers {
		wg.Add(1)
		go func(i string) {
			err := startTracking(c, i, pl)
			if err != nil {
				ec <- err
			}
			wg.Done()
		}(indexer)
	}
	wg.Wait()
	close(ec)
	errs := <-errSummary
	return errs
}

func summarizeErrors(ec <-chan error, errSummary chan<- error) {
	var errs []error
	for err := range ec {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		errSummary <- errs[0]
	}
	close(errSummary)
}

func observeLags(ctx context.Context, source, target string) {
	var t *time.Timer
	log.Infow("Started observing lags", "source", source, "target", target)

	for {
		start := time.Now()
		select {
		case <-ctx.Done():
			log.Infow("Finished observing lags", "source", source, "target", target)
			return
		default:
			tctx, cancel := context.WithTimeout(ctx, lagsObserveFreq)
			err := progress_observer.ObserveIndexers(tctx, source, target, observerMetrics, true)
			if err != nil {
				log.Error("Error observing lags", "err", err)
			}
			cancel()
		}

		// if lags reporting lasted for too long there is no need to wait
		elapsed := time.Since(start)
		if elapsed < lagsObserveFreq {
			t = time.NewTimer(lagsObserveFreq - elapsed)
			select {
			case <-ctx.Done():
				log.Infow("Finished observing lags", "source", source, "target", target)
				return
			case <-t.C:
			}
		}
	}
}

func observeCounts(ctx context.Context, source, target string) {
	var t *time.Timer
	log.Infow("Started observing counts", "source", source, "target", target)

	for {
		select {
		case <-ctx.Done():
			log.Infow("Finished observing counts", "source", source, "target", target)
			return
		default:
			err := progress_observer.ObserveIndexers(ctx, source, target, observerMetrics, false)
			if err != nil {
				log.Error("Error observing counts", "err", err)
			}
		}

		t = time.NewTimer(countsObserveFreq)
		select {
		case <-ctx.Done():
			log.Infow("Finished observing counts", "source", source, "target", target)
			return
		case <-t.C:
		}
	}
}

func startTracking(c *cli.Context, indexer string, pl *ProviderList) error {
	client, err := finderhttpclient.New(indexer)
	if err != nil {
		return err
	}

	ip := &indexProviders{
		client: client,
	}
	go ip.trackProviders(c.Context, indexer, pl)
	// provider list task
	// provider fall-behind task
	// provider query task
	var t *time.Timer
	for {
		t = time.NewTimer(timerFreq)
		select {
		case <-c.Context.Done():
			return c.Context.Err()
		case <-t.C:
		}
	}
}

type indexProviders struct {
	lk          sync.Mutex
	lastUpdated time.Time
	providers   []*model.ProviderInfo
	client      *finderhttpclient.Client
}

func (ip *indexProviders) trackProviders(ctx context.Context, indexer string, pl *ProviderList) {
	var t *time.Timer
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := ip.update(pl); err != nil {
				continue
			}
			providerCount.WithLabelValues(indexer).Set(float64(len(ip.providers)))
		}

		t = time.NewTimer(timerFreq)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (ip *indexProviders) update(pl *ProviderList) error {
	ctx, cncl := context.WithTimeout(context.Background(), 10*time.Second)
	providers, err := ip.client.ListProviders(ctx)
	cncl()
	if err != nil {
		return err
	}
	pl.ingest(providers)
	ip.lk.Lock()
	defer ip.lk.Unlock()
	ip.providers = providers
	ip.lastUpdated = time.Now()
	return nil
}
