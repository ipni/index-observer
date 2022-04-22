package task

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/httpsync"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"
)

// for a discovered provider, we periodically look at the 'head',
// chain of advertisements, and most recent entry list.
// * we track advertisement length to estimate overall volume for the provider.
// * we keep a few snapshots of entries for use in probing indexer and gateway response.

type Provider struct {
	Identity peer.AddrInfo

	LastHead                cid.Cid
	LastSync                time.Time
	ChainLengthFromLastHead uint64

	EntriesSampled    uint64
	AverageEntryCount float64
	SampledEntries    map[time.Time][]cid.Cid

	callback func(c cid.Cid)
}

func NewProvider(id peer.AddrInfo) *Provider {
	p := &Provider{
		Identity:       id,
		LastHead:       cid.Undef,
		SampledEntries: make(map[time.Time][]cid.Cid),
	}
	return p
}

func (p *Provider) makeSyncer(ctx context.Context) (syncer legs.Syncer, ls *ipld.LinkSystem, err error) {
	tls := ipld.LinkSystem{}
	ls = &tls
	rl := rate.NewLimiter(rate.Inf, 0)
	if isHTTP(p.Identity) {
		sync := httpsync.NewSync(tls, &http.Client{}, p.onBlock)
		syncer, err = sync.NewSyncer(p.Identity.ID, p.Identity.Addrs[0], rl)
		go func() {
			<-ctx.Done()
			sync.Close()
		}()
	} else {
		host, libp2pErr := libp2p.New()
		if libp2pErr != nil {
			err = libp2pErr
			return
		}
		host.Peerstore().AddAddrs(p.Identity.ID, p.Identity.Addrs, time.Hour*24*7)
		var sync *dtsync.Sync
		ds := datastore.NewNullDatastore()
		sync, err = dtsync.NewSync(host, ds, tls, p.onBlock, func(_ peer.ID) *rate.Limiter { return rl })
		if err != nil {
			return
		}
		if err := host.Connect(ctx, p.Identity); err != nil {
			return nil, nil, err
		}
		protos, err := host.Peerstore().GetProtocols(p.Identity.ID)
		if err != nil {
			return nil, nil, err
		}
		topic := topicFromSupportedProtocols(protos)
		syncer = sync.NewSyncer(p.Identity.ID, topic, rl)
		go func() {
			<-ctx.Done()
			sync.Close()
			host.Close()
		}()
	}
	return
}

func topicFromSupportedProtocols(protos []string) string {
	defaultTopic := "/indexer/ingest/mainnet"
	re := regexp.MustCompile("^/legs/head/([^/0]+)")
	for _, proto := range protos {
		if re.MatchString(proto) {
			defaultTopic = re.FindStringSubmatch(proto)[1]
			break
		}
	}
	return defaultTopic
}

func (p *Provider) SyncHead(ctx context.Context) error {
	fmt.Printf("beginning sync of %s\n", p.Identity.ID.Pretty())
	syncer, ls, err := p.makeSyncer(ctx)
	if err != nil {
		return err
	}

	newHead, err := syncer.GetHead(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("head at %s\n", newHead.String())

	cnt := 0
	head := newHead
	done := false
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSel := ssb.ExploreFields(
		func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		})
	p.callback = func(c cid.Cid) {
		fmt.Printf("sync loaded block.\n")
		cnt++
		head = c
	}
	for !done {
		fmt.Printf(".\n")
		store := memstore.Store{}
		ls.SetReadStorage(&store)
		ls.SetWriteStorage(&store)

		sel := legs.ExploreRecursiveWithStop(selector.RecursionLimitDepth(5000), adSel, cidlink.Link{Cid: p.LastHead})
		fmt.Printf("calling 'sync'\n")
		err = syncer.Sync(ctx, head, sel)
		if err != nil {
			return err
		}
		fmt.Printf("sync didn't err. seeing if done.\n")
		endNode, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: head}, basicnode.Prototype.Any)
		if err != nil {
			return err
		}
		prev, err := endNode.LookupByString("PreviousID")
		if err != nil {
			return err
		}
		pl, err := prev.AsLink()
		if err != nil {
			return err
		}
		if pl.(cidlink.Link).Cid.Equals(p.LastHead) {
			done = true
		}
	}

	p.ChainLengthFromLastHead += uint64(cnt)
	p.LastHead = newHead
	p.LastSync = time.Now()
	fmt.Printf("finished sync of %s, %d advertisements\n", p.Identity.ID.Pretty(), cnt)
	return nil
}

func (p *Provider) onBlock(_ peer.ID, c cid.Cid) {
	p.callback(c)
}

func isHTTP(p peer.AddrInfo) bool {
	isHttpPeerAddr := false
	for _, addr := range p.Addrs {
		for _, p := range addr.Protocols() {
			if p.Code == multiaddr.P_HTTP || p.Code == multiaddr.P_HTTPS {
				isHttpPeerAddr = true
				break
			}
		}
	}
	return isHttpPeerAddr
}
