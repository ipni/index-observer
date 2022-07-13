package task

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/httpsync"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"github.com/willscott/index-observer/safemapds"
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
	RMCnt                   uint64
	ChangeCnt               uint64
	// todo: cnt amends to existing ctxid

	// is it a filecoin provider? (only provides graphsync)
	GSOnly                 bool
	EntriesSampled         uint64
	LastEntriesSampled     cid.Cid
	AverageEntryCount      float64
	AverageEntryChunkCount float64
	SampledEntries         map[time.Time][]multihash.Multihash

	callbackMtx sync.Mutex
	callback    func(c cid.Cid)
}

func NewProvider(id peer.AddrInfo) *Provider {
	p := &Provider{
		Identity:       id,
		LastHead:       cid.Undef,
		SampledEntries: make(map[time.Time][]multihash.Multihash),
	}
	return p
}

func (p *Provider) makeSyncer(ctx context.Context) (syncer legs.Syncer, ls *ipld.LinkSystem, err error) {
	tls := cidlink.DefaultLinkSystem()
	store := memstore.Store{Bag: map[string][]byte{}}
	tls.SetReadStorage(&store)
	tls.SetWriteStorage(&store)
	tls.TrustedStorage = true

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
		ds := safemapds.NewMapDatastore()
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
	// don't block if another task is currently talking to this provider
	if !p.callbackMtx.TryLock() {
		return nil
	}
	defer p.callbackMtx.Unlock()
	fmt.Printf("beginning sync of %s\n", p.Identity.ID.Pretty())
	cctx, cncl := context.WithCancel(ctx)
	defer cncl()
	syncer, ls, err := p.makeSyncer(cctx)
	if err != nil {
		return err
	}

	newHead, err := syncer.GetHead(cctx)
	if err != nil {
		return err
	}

	cnt := 0
	rms := 0
	changes := 0
	head := newHead
	done := false
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSel := ssb.ExploreFields(
		func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		})
	p.callback = func(c cid.Cid) {
		cnt++
		head = c

		thisNode, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
		if err != nil {
			return
		}
		isRM, err := thisNode.LookupByString("IsRm")
		if err != nil {
			return
		}
		if irm, err := isRM.AsBool(); err == nil && irm {
			rms++
		}
		entries, err := thisNode.LookupByString("Entries")
		if err != nil {
			return
		}
		if en, err := entries.AsLink(); err != nil || en.String() == "" {
			changes++
		}
	}
	for !done {
		sel := legs.ExploreRecursiveWithStop(selector.RecursionLimitDepth(5000), adSel, cidlink.Link{Cid: p.LastHead})
		if p.LastHead.Equals(cid.Undef) {
			mh, _ := multihash.Encode([]byte{}, multihash.IDENTITY)
			sel = legs.ExploreRecursiveWithStop(selector.RecursionLimitDepth(5000), adSel, cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Raw), mh)})
		}
		err = syncer.Sync(cctx, head, sel)
		if err != nil {
			return err
		}
		endNode, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: head}, basicnode.Prototype.Any)
		if err != nil {
			return err
		}
		prev, err := endNode.LookupByString("PreviousID")
		if err != nil {
			done = true
			continue
		}
		pl, err := prev.AsLink()
		if err != nil {
			return err
		}
		if pl.(cidlink.Link).Cid.Equals(p.LastHead) {
			done = true
		}
		protocols, err := endNode.LookupByString("Metadata")
		if err != nil {
			log.Printf("failed to find expected metadata: %v", err)
			continue
		}
		pb, err := protocols.AsBytes()
		if err != nil {
			log.Printf("metadata wasn't bytes type as expected: %v", err)
			continue
		}
		first, _, err := varint.FromUvarint(pb)
		if err != nil {
			log.Printf("metadata didn't start with a varint: %v", err)
			continue
		}
		if first == uint64(multicodec.TransportGraphsyncFilecoinv1) {
			p.GSOnly = true
		}
	}

	p.ChainLengthFromLastHead += uint64(cnt)
	p.RMCnt += uint64(rms)
	p.ChangeCnt += uint64(changes)
	p.LastHead = newHead
	p.LastSync = time.Now()
	fmt.Printf("finished sync of %s, %d advertisements\n", p.Identity.ID.Pretty(), cnt)
	return nil
}

func (p *Provider) SyncEntries(ctx context.Context) error {
	// don't block if another task is currently talking to this provider
	if !p.callbackMtx.TryLock() {
		return nil
	}
	defer p.callbackMtx.Unlock()

	fmt.Printf("beginning entry sample of %s\n", p.Identity.ID.Pretty())
	cctx, cncl := context.WithCancel(ctx)
	defer cncl()
	syncer, ls, err := p.makeSyncer(cctx)
	if err != nil {
		return err
	}

	newHead, err := syncer.GetHead(cctx)
	if err != nil {
		return err
	}

	// don't re-sample
	if p.LastEntriesSampled.Equals(newHead) {
		return nil
	}

	head := newHead
	// get the head advertisement.
	err = syncer.Sync(cctx, head, selectorparse.CommonSelector_MatchPoint)
	if err != nil {
		return err
	}
	ad, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: head}, basicnode.Prototype.Any)
	if err != nil {
		return err
	}
	entryRoot, err := ad.LookupByString("Entries")
	if err != nil {
		return err
	}
	entryLnk, err := entryRoot.AsLink()
	if err != nil {
		return err
	}
	head = entryLnk.(cidlink.Link).Cid

	done := false
	cnt := 0
	cnk := 0
	lpCnt := 0
	processedCid := cid.Undef
	mhl := make([]multihash.Multihash, 0)
	p.callback = func(c cid.Cid) {
		if !c.Equals(processedCid) {
			cnk++
			lpCnt++
			chunk, err := ls.Load(ipld.LinkContext{}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
			if err != nil {
				return
			}
			entries, err := chunk.LookupByString("Entries")
			if err != nil {
				return
			}
			eli := entries.ListIterator()
			for !eli.Done() {
				_, v, _ := eli.Next()
				cnt++
				vb, err := v.AsBytes()
				if err != nil {
					continue
				}
				_, mhv, err := multihash.MHFromBytes(vb)
				if err != nil {
					continue
				}
				if len(mhl) < 100 {
					mhl = append(mhl, mhv)
				} else if rand.Intn(cnt) < 100 {
					mhl[rand.Intn(100)] = mhv
				}
			}
		}
		processedCid = c
		head = c
	}
	// go through entries
	for !done {
		fmt.Printf("E%s\n", head)
		lpCnt = 0
		ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
		sel := ssb.ExploreRecursive(selector.RecursionLimitDepth(500), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()
		err = syncer.Sync(cctx, head, sel)
		if err != nil {
			return err
		}
		fmt.Printf("called sync. read %d entries\n", lpCnt)
		if lpCnt < 1 {
			done = true
		}
	}

	p.LastEntriesSampled = newHead
	p.AverageEntryCount = (float64(p.EntriesSampled)*p.AverageEntryCount + float64(cnt)) / float64(p.EntriesSampled+1)
	p.AverageEntryChunkCount = (float64(p.EntriesSampled)*p.AverageEntryChunkCount + float64(cnk)) / float64(p.EntriesSampled+1)
	p.EntriesSampled++
	p.SampledEntries[time.Now()] = mhl

	fmt.Printf("finished entry sample of %s, %d in chunk\n", p.Identity.ID.Pretty(), cnt)
	return nil
}

func (p *Provider) onBlock(_ peer.ID, c cid.Cid) {
	if p.callback != nil {
		p.callback(c)
	}
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
