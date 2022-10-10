package task

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"

	finderhttpclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
)

type marketProvider struct {
	filGatewayClient string
	dealsEndpoint    string
	minerCache       map[string]minerInfo
	indexerClient    *finderhttpclient.Client
	lk               sync.Mutex
}

type minerInfo struct {
	ID           peer.ID
	hasMultiaddr bool
}

func NewMarketProvider(filGatewayURL, dealsEndpoint string, indexerClient *finderhttpclient.Client) *marketProvider {
	return &marketProvider{
		filGatewayClient: filGatewayURL,
		dealsEndpoint:    dealsEndpoint,
		minerCache:       make(map[string]minerInfo),
		indexerClient:    indexerClient,
	}
}

type JCID struct {
	Data string `json:"/"`
}

type dealProposal struct {
	PieceCID             JCID
	PieceSize            int
	VerifiedDeal         bool
	Client               string
	Provider             string
	Label                string
	StartEpoch           int
	EndEpoch             int
	StoragePricePerEpoch string
	ProviderCollateral   string
	ClientCollateral     string
}

type dealState struct {
	SectorStartEpoch int
	LastUpdatedEpoch int
	SlashEpoch       int
}

type deal struct {
	Proposal dealProposal
	State    dealState
}

type marketDeals map[string]deal

func (m *marketProvider) getDeals(from string, currentEpoch int64) (map[string]deal, error) {
	resp, err := http.Get(from)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		return nil, err
	}
	deals := make(marketDeals)
	err = json.Unmarshal(buf.Bytes(), &deals)
	if err != nil {
		return nil, err
	}

	// filter to active deals.
	goodDeals := make(marketDeals)

	for d, eal := range deals {
		if eal.State.SectorStartEpoch == -1 {
			continue
		}
		if int64(eal.Proposal.EndEpoch) < currentEpoch {
			continue
		}
		if eal.State.SlashEpoch != -1 {
			continue
		}

		goodDeals[d] = eal
	}

	return goodDeals, nil
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
				log.Printf("failed to get state market participants: %s\n", err)
				goto NEXT
			}

			epoch, err := node.ChainHead(rctx)
			if err != nil {
				timeout = 5 * time.Minute
				log.Printf("failed to get state market participants: %s\n", err)
				goto NEXT
			}

			deals, err := m.getDeals(m.dealsEndpoint, int64(epoch.Height()))
			if err != nil {
				log.Printf("failed to get state market deals: %s\n", err.Error())
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
					log.Printf("failed to parse miner address: %s\n", err)

					continue
				}
				mi, err := node.StateMinerInfo(lrctx, am, types.EmptyTSK)
				if err != nil {
					log.Printf("failed to get miner info: %s\n", err)
					continue
				}
				if mi.PeerId != nil {
					needed[m] = minerInfo{peer.ID(*mi.PeerId), len(mi.Multiaddrs) > 0}
				}
			}

			m.lk.Lock()
			for mi, p := range needed {
				m.minerCache[mi] = p
			}

			localMinerMap := make(map[peer.ID]string)
			for mi, p := range m.minerCache {
				localMinerMap[p.ID] = mi
			}
			pd := 0
			for p := range participants {
				if m.minerCache[p].hasMultiaddr {
					pd++
				}
			}
			m.lk.Unlock()

			observed, oL := pl.Get()
			observedMap := make(map[string]uint64)

			pn := 0
			for i, j := range observed {
				if om, ok := localMinerMap[j.ID]; ok {
					pn++
					observedMap[om] = oL[i]

				}
			}

			actualDn := uint64(0)
			dn := 0
			dd := 0
			if deals != nil {
				ps := make(map[string]struct{})
				for _, d := range deals {
					if _, ok := observedMap[d.Proposal.Provider]; ok {
						dn++
					}
					if _, ok := ps[d.Proposal.Provider]; !ok {
						ps[d.Proposal.Provider] = struct{}{}
					}
					dd++
				}

				// be more agressive about pd as well - it's that the provider has a multiaddr, and has been seen making at least one deal
				pd = 0
				for p := range participants {
					if m.minerCache[p].hasMultiaddr {
						if _, ok := ps[p]; ok {
							actualDn += observedMap[p]
							pd++
						}
					}
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
			filDealCount.With(prometheus.Labels{"type": "onChain"}).Set(float64(dn))
			filDealCount.With(prometheus.Labels{"type": "adCount"}).Set(float64(actualDn))

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
