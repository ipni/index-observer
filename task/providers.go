package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/gammazero/workerpool"
	"github.com/libp2p/go-libp2p/core/peer"
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
			rmHist := prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "provider_rm_counmt",
				Help:    "Number of removal advertisements",
				Buckets: prometheus.ExponentialBuckets(2, 2, 30),
			})
			changeHist := prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "provider_change_counmt",
				Help:    "Number of advertisements with no entries",
				Buckets: prometheus.ExponentialBuckets(2, 2, 30),
			})
			gsCount := prometheus.NewCounter(prometheus.CounterOpts{
				Name: "provider_graphsync_count",
				Help: "Number of providers offering only graphsync for transport",
			})
			chainChnkHist := prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "provider_entry_chunks",
				Help:    "Length of sampled entry chunks",
				Buckets: prometheus.ExponentialBuckets(1, 2, 20),
			})
			chainCntHist := prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "provider_entry_length",
				Help:    "Length of sampled entries",
				Buckets: prometheus.ExponentialBuckets(2, 4, 15),
			})
			for _, p := range pl.providers {
				if p.ChainLengthFromLastHead > 0 {
					hist.Observe(float64(p.ChainLengthFromLastHead))
					rmHist.Observe(float64(p.RMCnt))
					changeHist.Observe(float64(p.ChangeCnt))
					if p.EntriesSampled > 0 {
						chainChnkHist.Observe(float64(p.AverageEntryChunkCount))
						chainCntHist.Observe(float64(p.AverageEntryCount))
					}
				}
				if p.GSOnly {
					gsCount.Add(1)
				}
			}
			providerChainLengths = hist
			providerRMCounts = rmHist
			providerChangeCounts = changeHist
			providerGSCount = gsCount
			providerEntryChunks = chainChnkHist
			providerEntryLengths = chainCntHist
			pl.m.Unlock()
		}
	}()
	for {
		select {
		case pi := <-pl.inchan:
			pl.m.Lock()
			if _, ok := pl.providers[peer.ID(pi.AddrInfo.ID)]; !ok {
				ai := peer.AddrInfo{
					ID:    peer.ID(pi.AddrInfo.ID),
					Addrs: pi.AddrInfo.Addrs,
				}
				pl.providers[peer.ID(pi.AddrInfo.ID)] = NewProvider(ai)
				pl.pool.Submit(pl.headTask(ctx, pl.providers[peer.ID(pi.AddrInfo.ID)]))
				pl.pool.Submit(pl.sampleTask(ctx, pl.providers[peer.ID(pi.AddrInfo.ID)]))
			}
			pl.m.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (pl *ProviderList) headTask(ctx context.Context, p *Provider) func() {
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
		go pl.reQueue(ctx, p, 10*time.Minute, pl.headTask(ctx, p))
	}
}

func (pl *ProviderList) sampleTask(ctx context.Context, p *Provider) func() {
	return func() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := p.SyncEntries(ctx)
		if err != nil {
			fmt.Printf("error syncing: %s\n", err)
		}
		go pl.reQueue(ctx, p, 60*time.Minute, pl.sampleTask(ctx, p))
	}
}

func (pl *ProviderList) reQueue(ctx context.Context, p *Provider, in time.Duration, what func()) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(in):
	}
	pl.m.Lock()
	pl.pool.Submit(what)
	pl.m.Unlock()
}

func (pl *ProviderList) Get() ([]peer.AddrInfo, []uint64) {
	pl.m.Lock()
	defer pl.m.Unlock()

	addrs := make([]peer.AddrInfo, 0, len(pl.providers))
	dc := make([]uint64, 0, len(pl.providers))
	for _, p := range pl.providers {
		addrs = append(addrs, p.Identity)
		dc = append(dc, p.ChainLengthFromLastHead)
	}
	return addrs, dc
}
