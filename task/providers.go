package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/libp2p/go-libp2p-core/peer"
)

type ProviderList struct {
	inchan    chan *model.ProviderInfo
	providers map[peer.ID]*Provider
	pq        []*Provider
	m         sync.Mutex
}

func NewProviderList(ctx context.Context) *ProviderList {
	pl := &ProviderList{
		inchan:    make(chan *model.ProviderInfo, 5),
		providers: make(map[peer.ID]*Provider),
		pq:        make([]*Provider, 0),
	}
	go pl.background(ctx)
	go pl.providerLoop(ctx)
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
				pl.pq = append(pl.pq, pl.providers[pi.AddrInfo.ID])
			}
			pl.m.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (pl *ProviderList) providerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		has := false
		var next *Provider
		pl.m.Lock()
		if len(pl.pq) > 0 {
			has = true
			next = pl.pq[0]
			// move it to the back
			pl.pq = pl.pq[1:]
			pl.pq = append(pl.pq, next)
		}
		pl.m.Unlock()
		if !has {
			time.Sleep(time.Second)
		} else {
			err := next.SyncHead(ctx)
			if err != nil {
				fmt.Printf("error syncing: %s\n", err)
			}
		}
	}
}
