package internal

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("progress_observer/client")

type ProviderClient struct {
	*options
	sub *dagsync.Subscriber

	store     *ProviderClientStore
	publisher peer.AddrInfo

	adSel ipld.Node
}

func NewProviderClient(provAddr peer.AddrInfo, o ...Option) (*ProviderClient, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New()
	if err != nil {
		return nil, err
	}
	h.Peerstore().AddAddrs(provAddr.ID, provAddr.Addrs, time.Hour)

	store := newProviderClientStore()
	sub, err := dagsync.NewSubscriber(h, store.Batching, store.LinkSystem, opts.topic, nil)
	if err != nil {
		return nil, err
	}

	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSel := ssb.ExploreRecursive(selector.RecursionLimitDepth(1), ssb.ExploreFields(
		func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		})).Node()

	return &ProviderClient{
		options:   opts,
		sub:       sub,
		publisher: provAddr,
		store:     store,
		adSel:     adSel,
	}, nil
}

func (p *ProviderClient) GetCids(ctx context.Context, startCid cid.Cid, depth int) ([]cid.Cid, error) {
	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	recursiveAdSel := ssb.ExploreRecursive(selector.RecursionLimitDepth(int64(depth)), ssb.ExploreFields(
		func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		})).Node()

	id, err := p.syncAdWithRetry(ctx, startCid, recursiveAdSel)
	if err != nil {
		return nil, err
	}

	// Load the synced advertisement from local store.
	ad, err := p.store.getAdvertisement(ctx, id)
	if err != nil {
		return nil, err
	}

	results := make([]cid.Cid, 0, depth)
	results = append(results, id)
	if ad.PreviousID == nil {
		return nil, nil
	}
	currId := ad.PreviousID.(cidlink.Link).Cid

	for i := 0; i < depth; i++ {
		ad, err := p.store.getAdvertisement(ctx, currId)
		if err != nil {
			break
		}
		results = append(results, currId)
		if ad.PreviousID == nil {
			break
		}
		currId = ad.PreviousID.(cidlink.Link).Cid
	}

	return results, nil
}

func (p *ProviderClient) syncAdWithRetry(ctx context.Context, id cid.Cid, sel datamodel.Node) (cid.Cid, error) {
	var attempt uint64
	for {
		id, err := p.sub.Sync(ctx, p.publisher, id, sel)
		if err == nil {
			return id, nil
		}
		if attempt > p.maxSyncRetry {
			log.Errorw("Reached maximum retry attempt while syncing ad", "cid", id, "attempt", attempt, "err", err)
			return cid.Undef, err
		}
		attempt++
		log.Infow("retrying ad sync", "attempt", attempt, "err", err)
		time.Sleep(p.syncRetryBackoff)
	}
}

func (p *ProviderClient) Close() error {
	p.store.Close()

	return p.sub.Close()
}
