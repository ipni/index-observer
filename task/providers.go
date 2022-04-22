package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/gammazero/workerpool"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/prometheus/client_golang/prometheus"
)

type ProviderList struct {
	inchan    chan *model.ProviderInfo
	providers map[peer.ID]*Provider
	pool      *workerpool.WorkerPool
	m         sync.Mutex
}

func NewProviderList(ctx context.Context) *ProviderList {
	pl := &ProviderList{
		inchan:    make(chan *model.ProviderInfo, 5),
		providers: make(map[peer.ID]*Provider),
	}
	pl.pool = workerpool.New(5)
	go pl.background(ctx)
	return pl
}

func (pl *ProviderList) ingest(pi []*model.ProviderInfo) {
	for _, p := range pi {
		pl.inchan <- p
	}
}

func (pl *ProviderList) background(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 30):
			}
			pl.m.Lock()
			// we do this so that each view of the histogram is a consistent snapshot, and doesn't count individual providers multiple times.
			hist := prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "provider_chain_length",
				Help:    "Length of the chain of providers",
				Buckets: prometheus.ExponentialBuckets(2, 2, 40),
			})
			for _, p := range pl.providers {
				if p.ChainLengthFromLastHead > 0 {
					hist.Observe(float64(p.ChainLengthFromLastHead))
				}
			}
			providerChainLengths = hist
			pl.m.Unlock()
		}
	}()
	for {
		select {
		case pi := <-pl.inchan:
			pl.m.Lock()
			if _, ok := pl.providers[pi.AddrInfo.ID]; !ok {
				pl.providers[pi.AddrInfo.ID] = NewProvider(pi.AddrInfo)
				pl.pool.Submit(pl.task(ctx, pl.providers[pi.AddrInfo.ID]))
			}
			pl.m.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (pl *ProviderList) task(ctx context.Context, p *Provider) func() {
	return func() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := p.SyncHead(ctx)
		if err != nil {
			fmt.Printf("error syncing: %s\n", err)
		}
		go pl.reQueue(ctx, p)
	}
}

func (pl *ProviderList) reQueue(ctx context.Context, p *Provider) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Minute * 10):
	}
	pl.m.Lock()
	pl.pool.Submit(pl.task(ctx, p))
	pl.m.Unlock()
}
