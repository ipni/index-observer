package internal

import (
	"bytes"
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipni/storetheindex/api/v0/ingest/schema"
)

type (
	ProviderClientStore struct {
		datastore.Batching
		ipld.LinkSystem
	}
)

func newProviderClientStore() *ProviderClientStore {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := store.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return store.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return &ProviderClientStore{
		Batching:   store,
		LinkSystem: lsys,
	}
}

func (s *ProviderClientStore) getAdvertisement(ctx context.Context, id cid.Cid) (*schema.Advertisement, error) {
	val, err := s.Batching.Get(ctx, datastore.NewKey(id.String()))
	if err != nil {
		return nil, err
	}

	nb := schema.AdvertisementPrototype.NewBuilder()
	decoder, err := multicodec.LookupDecoder(id.Prefix().Codec)
	if err != nil {
		return nil, err
	}

	err = decoder(nb, bytes.NewBuffer(val))
	if err != nil {
		return nil, err
	}
	node := nb.Build()

	ad, err := schema.UnwrapAdvertisement(node)
	if err != nil {
		return nil, err
	}

	return ad, nil
}
