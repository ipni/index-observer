package task

import (
	"context"
	"sync"
	"time"

	finderhttpclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/index-observer/progress_observer"
	"github.com/urfave/cli/v2"
)

const (
	observeFreq = 20 * time.Minute
	timerFreq   = 10 * time.Minute
)

var log = logging.Logger("task")

func Start(c *cli.Context) error {
	if err := StartMetrics(c); err != nil {
		return err
	}

	source := c.String("source")
	target := c.String("target")

	pl := NewProviderList(c.Context)
	indexers := c.StringSlice("indexer")
	wg := sync.WaitGroup{}
	ec := make(chan error)
	errSummary := make(chan error)

	if source != "" && target != "" {
		wg.Add(1)
		go func() {
			observerIndexers(c.Context, source, target)
			wg.Done()
		}()

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

func observerIndexers(ctx context.Context, source, target string) {
	var t *time.Timer
	log.Infow("Started observing indexers", "source", source, "target", target)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := progress_observer.ObserveIndexers(source, target, observerMetrics)
			if err != nil {
				log.Error("Error observing indexers", "err", err)
			}
		}

		t = time.NewTimer(observeFreq)
		select {
		case <-ctx.Done():
			log.Infow("Finished observing indexers", "source", source, "target", target)
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
