package safemapds

import (
	"context"
	"sync"

	"github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

// MapDatastore uses a standard Go map for internal storage.
type MapDatastore struct {
	lock   sync.RWMutex
	values map[datastore.Key][]byte
}

var _ datastore.Datastore = (*MapDatastore)(nil)
var _ datastore.Batching = (*MapDatastore)(nil)

// NewMapDatastore constructs a MapDatastore. It is _not_ thread-safe by
// default, wrap using sync.MutexWrap if you need thread safety (the answer here
// is usually yes).
func NewMapDatastore() (d *MapDatastore) {
	return &MapDatastore{
		values: make(map[datastore.Key][]byte),
	}
}

// Put implements Datastore.Put
func (d *MapDatastore) Put(ctx context.Context, key datastore.Key, value []byte) (err error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.values[key] = value
	return nil
}

// Sync implements Datastore.Sync
func (d *MapDatastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return nil
}

// Get implements Datastore.Get
func (d *MapDatastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	val, found := d.values[key]
	if !found {
		return nil, datastore.ErrNotFound
	}
	return val, nil
}

// Has implements Datastore.Has
func (d *MapDatastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	_, found := d.values[key]
	return found, nil
}

// GetSize implements Datastore.GetSize
func (d *MapDatastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if v, found := d.values[key]; found {
		return len(v), nil
	}
	return -1, datastore.ErrNotFound
}

// Delete implements Datastore.Delete
func (d *MapDatastore) Delete(ctx context.Context, key datastore.Key) (err error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.values, key)
	return nil
}

// Query implements Datastore.Query
func (d *MapDatastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	re := make([]dsq.Entry, 0, len(d.values))
	for k, v := range d.values {
		e := dsq.Entry{Key: k.String(), Size: len(v)}
		if !q.KeysOnly {
			e.Value = v
		}
		re = append(re, e)
	}
	d.lock.RLock()
	defer d.lock.RUnlock()
	r := dsq.ResultsWithEntries(q, re)
	r = dsq.NaiveQueryApply(q, r)
	return r, nil
}

func (d *MapDatastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return newBasicBatch(d), nil
}

func (d *MapDatastore) Close() error {
	return nil
}

type basicBatch struct {
	ops map[datastore.Key]op

	target *MapDatastore
}

type op struct {
	delete bool
	value  []byte
}

var _ datastore.Batch = (*basicBatch)(nil)

func newBasicBatch(ds *MapDatastore) datastore.Batch {
	return &basicBatch{
		ops:    make(map[datastore.Key]op),
		target: ds,
	}
}

func (bt *basicBatch) Put(ctx context.Context, key datastore.Key, val []byte) error {
	bt.ops[key] = op{value: val}
	return nil
}

func (bt *basicBatch) Delete(ctx context.Context, key datastore.Key) error {
	bt.ops[key] = op{delete: true}
	return nil
}

func (bt *basicBatch) Commit(ctx context.Context) error {
	bt.target.lock.Lock()
	defer bt.target.lock.Unlock()

	for k, op := range bt.ops {
		if op.delete {
			delete(bt.target.values, k)
		} else {
			bt.target.values[k] = op.value
		}
	}

	return nil
}
