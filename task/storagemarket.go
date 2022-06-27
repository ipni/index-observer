package task

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/prometheus/client_golang/prometheus"

	finderhttpclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
)

type marketProvider struct {
	filGatewayClient string
	minerCache       map[string]minerInfo
	indexerClient    *finderhttpclient.Client
	lk               sync.Mutex
}

type minerInfo struct {
	ID           peer.ID
	hasMultiaddr bool
}

func NewMarketProvider(filGatewayURL string, indexerClient *finderhttpclient.Client) *marketProvider {
	return &marketProvider{
		filGatewayClient: filGatewayURL,
		minerCache:       make(map[string]minerInfo),
		indexerClient:    indexerClient,
	}
}

func (m *marketProvider) Track(ctx context.Context, pl *ProviderList) {
	node, closer, err := client.NewFullNodeRPCV1(ctx, m.filGatewayClient, http.Header{})
	if err != nil {
		log.Fatalf("failed to connect to gateway: %v", err)
		return
	}
	defer closer()

	for {
		timeout := time.Hour
		{
			rctx, cncl := context.WithTimeout(ctx, 2*time.Minute)
			defer cncl()

			participants, err := node.StateMarketParticipants(rctx, types.EmptyTSK)
			if err != nil {
				timeout = 5 * time.Minute
				log.Printf("failed to get state market participants: %w\n", err)
				goto NEXT
			}

			head, err := node.ChainHead(rctx)
			if err != nil {
				timeout = 5 * time.Minute
				log.Printf("failed to get head: %s\n", err.Error())
				goto NEXT
			}

			deals, err := node.StateMarketDeals(rctx, head.Key())
			if err != nil {
				//				timeout = 5 * time.Minute
				log.Printf("failed to get state market deals: %s\n", err.Error())
				//				goto NEXT
			}

			needed := make(map[string]minerInfo)
			m.lk.Lock()
			for miner := range participants {
				if _, ok := m.minerCache[miner]; !ok {
					needed[miner] = minerInfo{}
				}
			}
			m.lk.Unlock()

			for m := range needed {
				lrctx, cncl := context.WithTimeout(ctx, 15*time.Second)
				defer cncl()
				am, err := address.NewFromString(m)
				if err != nil {
					log.Printf("failed to parse miner address: %w\n", err)

					continue
				}
				mi, err := node.StateMinerInfo(lrctx, am, types.EmptyTSK)
				if err != nil {
					log.Printf("failed to get miner info: %w\n", err)
					continue
				}
				needed[m] = minerInfo{*mi.PeerId, len(mi.Multiaddrs) > 0}
			}

			m.lk.Lock()
			for mi, p := range needed {
				m.minerCache[mi] = p
			}

			localMinerMap := make(map[peer.ID]string)
			for mi, p := range m.minerCache {
				localMinerMap[p.ID] = mi
			}
			activeMinerAddress := make(map[address.Address]struct{})
			pd := 0
			for p, _ := range participants {
				pa, err := address.NewFromString(p)
				if err == nil {
					activeMinerAddress[pa] = struct{}{}
				}
				if m.minerCache[p].hasMultiaddr {
					pd++
				}
			}
			m.lk.Unlock()

			observed := pl.Get()

			pn := 0
			for _, i := range observed {
				if _, ok := localMinerMap[i.ID]; ok {
					pn++
				}
			}

			dn := 0
			dd := 0
			if deals != nil {
				for _, d := range deals {
					if _, ok := activeMinerAddress[d.Proposal.Provider]; ok {
						dn++
					}
					dd++
				}
			}

			// account.
			providerRate := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "fil_provider_rate",
				Help: "Percentage market participants seen",
			})
			dealRate := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "fil_deal_rate",
				Help: "Percentage deals seen",
			})

			providerRate.Set(float64(pn) / float64(pd))
			dealRate.Set(float64(dn) / float64(dd))

			filProviderRate = providerRate
			filDealRate = dealRate
		}
	NEXT:
		select {
		case <-ctx.Done():
			return
		case <-time.After(timeout):
		}
	}
}
