package progress_observer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipni/index-observer/progress_observer/internal"
	"github.com/ipni/index-observer/progress_observer/metrics"
	finderhttpclient "github.com/ipni/storetheindex/api/v0/finder/client/http"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	log         = logging.Logger("progress_observer")
	metricsLock sync.Mutex
)

const (
	// depth is a depth of chain segment that is fetched form the publisher
	depth = 1000
	// parallelism is a max number of concurrent goroutines
	parallelism = 50
)

type progressInfo struct {
	source      *model.ProviderInfo
	target      *model.ProviderInfo
	lag         int
	unreachable bool
}

func (s *progressInfo) String() string {
	return fmt.Sprintf("peer: %s, lag: %d, unreachable: %v", s.source.AddrInfo.ID.String(), s.lag, s.unreachable)
}

func ObserveIndexers(ctx context.Context, sourceUrl, targetUrl string, m *metrics.Metrics, reportLags bool) error {
	sourceName, err := extractDomain(sourceUrl)
	if err != nil {
		return err
	}
	targetName, err := extractDomain(targetUrl)
	if err != nil {
		return err
	}

	// create clients
	sourceClient, err := finderhttpclient.New(sourceUrl)
	if err != nil {
		log.Fatal(err)
	}

	targetClient, err := finderhttpclient.New(targetUrl)
	if err != nil {
		log.Fatal(err)
	}

	// fetch source and target providers
	sources, err := sourceClient.ListProviders(ctx)
	if err != nil {
		log.Fatal(err)
	}

	m.RecordCount(len(sources), sourceName, "", metrics.TotalCount)

	targets, err := targetClient.ListProviders(ctx)
	if err != nil {
		log.Fatal(err)
	}

	m.RecordCount(len(targets), targetName, "", metrics.TotalCount)

	// group sources and targets into matches, mismatches, missing at source and missing at target
	targetsMap := make(map[peer.ID]*model.ProviderInfo)
	for _, target := range targets {
		if target.AddrInfo.ID == "" {
			continue
		}
		targetsMap[target.AddrInfo.ID] = target
	}

	mismatches := make([]progressInfo, 0)
	matches := make([]*model.ProviderInfo, 0)
	unknwonByTarget := make([]*model.ProviderInfo, 0)
	for _, source := range sources {
		if source.AddrInfo.ID == "" {
			continue
		}
		if target, ok := targetsMap[source.AddrInfo.ID]; ok {
			if target.LastAdvertisement == source.LastAdvertisement {
				matches = append(matches, source)
			} else {
				mismatches = append(mismatches, progressInfo{
					source: source,
					target: target,
				})
			}
			delete(targetsMap, source.AddrInfo.ID)
		} else {
			unknwonByTarget = append(unknwonByTarget, source)
		}
	}

	unknownBySource := make([]*model.ProviderInfo, 0, len(targetsMap))
	for _, v := range targetsMap {
		unknownBySource = append(unknownBySource, v)
	}

	// report counts
	m.RecordCount(len(matches), sourceName, targetName, metrics.MatchCount)
	m.RecordCount(len(unknwonByTarget), sourceName, targetName, metrics.UnknownCount)
	m.RecordCount(len(unknownBySource), targetName, sourceName, metrics.UnknownCount)

	if !reportLags {
		return nil
	}

	numJobs := len(mismatches)

	jobs := make(chan *progressInfo, numJobs)
	results := make(chan bool, numJobs)

	// do not create more goroutines than there are mismatches
	actualParallelism := int(math.Min(float64(parallelism), float64(numJobs)))

	for i := 1; i < actualParallelism; i++ {
		go worker(ctx, m, sourceName, targetName, jobs, results)
	}

	for i := 1; i < numJobs; i++ {
		jobs <- &mismatches[i]
	}
	close(jobs)

	for i := 1; i <= numJobs; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-results:
		}
	}
	close(results)

	// report unreachables
	unreachable := 0
	for _, s := range mismatches {
		if s.unreachable {
			unreachable++
		}
	}
	m.RecordCount(unreachable, sourceName, targetName, metrics.UnreachableCount)
	return nil
}

func extractDomain(u string) (string, error) {
	url, err := url.Parse(u)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(url.Hostname(), "www."), nil
}

func recordLag(ctx context.Context, m *metrics.Metrics, info *progressInfo, sourceName, targetName string) {
	metricsLock.Lock()
	defer metricsLock.Unlock()

	if info.lag > 0 {
		m.RecordLag(ctx, uint(info.lag), sourceName, targetName)
	} else {
		m.RecordLag(ctx, uint(-info.lag), targetName, sourceName)
	}
}

// worker identifies and records lag between two providers. Gets executed concurrently by many goroutines.
func worker(ctx context.Context, m *metrics.Metrics, sourceName, targetName string, jobs <-chan *progressInfo, results chan<- bool) {
	for j := range jobs {
		select {
		case <-ctx.Done():
			log.Infow("Worker timed out")
		default:
			start := time.Now()
			lag, err := findLag(ctx, *j.source.Publisher, j.source.LastAdvertisement, j.target.LastAdvertisement)

			if err != nil {
				log.Infow("Error reaching out to publisher", "publisher", j.source.Publisher.ID.String(), "err", err)
				j.unreachable = true
			} else {
				j.lag = lag
			}
			recordLag(ctx, m, j, sourceName, targetName)
			log.Infow("Calculated lag", "lag", j.String(), "elapsed", time.Since(start))
			results <- true
		}
	}
}

// findLag finds a lag between source and target cids. If the value is positive then target lags form the source. If it's negative - then the source lags form the target.
// If error is not nil then the value of the lag should be ignored.
func findLag(ctx context.Context, addr peer.AddrInfo, scid, tcid cid.Cid) (int, error) {
	// create client
	client, err := internal.NewProviderClient(addr,
		internal.WithMaxSyncRetry(1),
		internal.WithEntriesRecursionLimit(selector.RecursionLimitDepth(1)))

	if err != nil {
		return 0, err
	}
	defer client.Close()

	// walkCidsFunc fetches cids chain up to the depth and tries to find "lookup" cid in it. If found - the function will return its position in the array.
	// If not found - the function will return -1 and the last cid that it has checked.
	walkCidsFunc := func(head, lookup cid.Cid) (int, cid.Cid, error) {
		cids, err := client.GetCids(ctx, head, depth)
		if err != nil {
			return 0, cid.Undef, err
		}
		for i, c := range cids {
			if c == lookup {
				return i, cid.Undef, nil
			}
		}
		if len(cids) == depth {
			return -1, cids[len(cids)-1], nil
		}
		return -1, cid.Undef, nil
	}

	var lag int
	iter := 0
	shead := scid
	thead := tcid

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			// check if target cid exists in source chain
			if shead != cid.Undef {
				lag, shead, err = walkCidsFunc(shead, tcid)
				if err != nil {
					return 0, err
				}

				if lag >= 0 {
					return iter*depth + lag, nil
				}
			}

			// check if source cid exists in target chain
			if thead != cid.Undef {
				lag, thead, err = walkCidsFunc(thead, scid)
				if err != nil {
					return 0, err
				}

				if lag >= 0 {
					return -(iter*depth + lag), nil
				}
			}
			iter++
			if shead == cid.Undef && thead == cid.Undef {
				return 0, errors.New("could not find neither target or source cids")
			}
		}
	}
}
