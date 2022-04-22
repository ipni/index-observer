package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/gammazero/workerpool"
	"github.com/libp2p/go-libp2p-core/peer"
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
	case <-time.After(time.Minute * 5):
	}
	pl.pool.Submit(pl.task(ctx, p))
}
