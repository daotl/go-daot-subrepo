// Copyright for portions of this fork are held by [Jeromy Johnson, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package leveldb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"

	ds "github.com/daotl/go-datastore"
	key "github.com/daotl/go-datastore/key"
	dsq "github.com/daotl/go-datastore/query"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	ErrUnimplemented   = errors.New("method not implemented")
	ErrKeyTypeNotMatch = errors.New("key type does not match")
)

type Datastore struct {
	*accessor
	DB   *leveldb.DB
	path string
}

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.TxnDatastore = (*Datastore)(nil)

// Options extends syndtr/goleveldb/opt.Options
type Options struct {
	opt.Options

	// SyncWrites is whether to sync underlying writes from the OS buffer cache
	// through to actual disk, if applicable. Setting SyncWrites can result in
	// slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (and the machine does
	// not) then no writes will be lost.
	//
	// In other words, SyncWrites being false has the same semantics as a write
	// system call. SyncWrites being true means write followed by fsync.
	//
	// The default value is true.
	SyncWrites bool
}

// NewDatastore returns a new datastore backed by leveldb
//
// for path == "", an in memory backend will be chosen
func NewDatastore(path string, ktype key.KeyType, opts *Options) (*Datastore, error) {
	if opts == nil {
		opts = &Options{SyncWrites: true}
	}

	nopts := opts.Options

	var err error
	var db *leveldb.DB

	if !(ktype == key.KeyTypeString || ktype == key.KeyTypeBytes) {
		return nil, key.ErrKeyTypeNotSupported
	}

	if path == "" {
		db, err = leveldb.Open(storage.NewMemStorage(), &nopts)
	} else {
		db, err = leveldb.OpenFile(path, &nopts)
		if errors.IsCorrupted(err) && !nopts.GetReadOnly() {
			db, err = leveldb.RecoverFile(path, &nopts)
		}
	}

	if err != nil {
		return nil, err
	}

	ds := Datastore{
		accessor: &accessor{
			ldb:        db,
			syncWrites: opts.SyncWrites,
			ktype:      ktype,
			closeLk:    new(sync.RWMutex),
		},
		DB:   db,
		path: path,
	}
	return &ds, nil
}

// An extraction of the common interface between LevelDB Transactions and the DB itself.
//
// It allows to plug in either inside the `accessor`.
type levelDbOps interface {
	Put(key, value []byte, wo *opt.WriteOptions) error
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
	Delete(key []byte, wo *opt.WriteOptions) error
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

// Datastore operations using either the DB or a transaction as the backend.
type accessor struct {
	ldb        levelDbOps
	syncWrites bool
	ktype      key.KeyType
	closeLk    *sync.RWMutex
}

func (a *accessor) Put(ctx context.Context, key key.Key, value []byte) (err error) {
	if key.KeyType() != a.ktype {
		return ErrKeyTypeNotMatch
	}
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	return a.ldb.Put(key.Bytes(), value, &opt.WriteOptions{Sync: a.syncWrites})
}

func (a *accessor) Sync(ctx context.Context, prefix key.Key) error {
	// goleveldb provides no way to fsync if written with WriteOptions.Sync=false
	if a.syncWrites == false {
		return ErrUnimplemented
	}
	if prefix.KeyType() != a.ktype {
		return ErrKeyTypeNotMatch
	}
	return nil
}

func (a *accessor) Get(ctx context.Context, key key.Key) (value []byte, err error) {
	if key.KeyType() != a.ktype {
		return nil, ErrKeyTypeNotMatch
	}
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	val, err := a.ldb.Get(key.Bytes(), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return val, nil
}

func (a *accessor) Has(ctx context.Context, key key.Key) (exists bool, err error) {
	if key.KeyType() != a.ktype {
		return false, ErrKeyTypeNotMatch
	}
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	return a.ldb.Has(key.Bytes(), nil)
}

func (a *accessor) GetSize(ctx context.Context, key key.Key) (size int, err error) {
	if key.KeyType() != a.ktype {
		return -1, ErrKeyTypeNotMatch
	}
	return ds.GetBackedSize(ctx, a, key)
}

func (a *accessor) Delete(ctx context.Context, key key.Key) (err error) {
	if key.KeyType() != a.ktype {
		return ErrKeyTypeNotMatch
	}
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	return a.ldb.Delete(key.Bytes(), &opt.WriteOptions{Sync: a.syncWrites})
}

func (a *accessor) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()

	// make a copy of the query for the fallback naive query implementation.
	// don't modify the original so res.Query() returns the correct results.
	qNaive := q

	// We compute the intersection of the ranges defined by q.Prefix and q.Range.
	var rnge *util.Range
	// First q.Prefix
	if q.Prefix != nil {
		if q.Prefix.KeyType() != a.ktype {
			return nil, ErrKeyTypeNotMatch
		}
		switch a.ktype {
		case key.KeyTypeString:
			sk := q.Prefix.(key.StrKey)
			sk.Clean()
			prefix := sk.String()
			if prefix != "/" {
				rnge = util.BytesPrefix([]byte(prefix + "/"))
			}
		case key.KeyTypeBytes:
			rnge = util.BytesPrefix(q.Prefix.Bytes())
			rnge.Start = append(rnge.Start, 0x0)
		}
		qNaive.Prefix = nil
	}
	// Then adjust with q.Range
	if q.Range.Start != nil || q.Range.End != nil {
		if rnge == nil {
			rnge = &util.Range{}
		}
		if q.Range.Start != nil {
			if sbytes := q.Range.Start.Bytes(); rnge.Start == nil || bytes.Compare(rnge.Start, sbytes) < 0 {
				rnge.Start = sbytes
			}
		}
		if q.Range.End != nil {
			if ebytes := q.Range.End.Bytes(); rnge.Limit == nil || bytes.Compare(ebytes, rnge.Limit) < 0 {
				rnge.Limit = ebytes
			}
		}
	}

	i := a.ldb.NewIterator(rnge, nil)
	next := i.Next
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case dsq.OrderByKey, *dsq.OrderByKey:
			qNaive.Orders = nil
		case dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			next = func() bool {
				next = i.Prev
				return i.Last()
			}
			qNaive.Orders = nil
		default:
		}
	}
	r := dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			a.closeLk.RLock()
			defer a.closeLk.RUnlock()
			if !next() {
				return dsq.Result{}, false
			}
			var k key.Key
			switch a.ktype {
			case key.KeyTypeString:
				k = key.NewStrKey(string(i.Key()))
			case key.KeyTypeBytes:
				b := make([]byte, len(i.Key()))
				copy(b, i.Key())
				k = key.NewBytesKey(b)
			}
			e := dsq.Entry{Key: k, Size: len(i.Value())}

			if !q.KeysOnly {
				buf := make([]byte, len(i.Value()))
				copy(buf, i.Value())
				e.Value = buf
			}
			return dsq.Result{Entry: e}, true
		},
		Close: func() error {
			a.closeLk.RLock()
			defer a.closeLk.RUnlock()
			i.Release()
			return nil
		},
	})
	return dsq.NaiveQueryApply(qNaive, r), nil
}

// DiskUsage returns the current disk size used by this levelDB.
// For in-mem datastores, it will return 0.
func (d *Datastore) DiskUsage(ctx context.Context) (uint64, error) {
	d.closeLk.RLock()
	defer d.closeLk.RUnlock()
	if d.path == "" { // in-mem
		return 0, nil
	}

	var du uint64

	err := filepath.Walk(d.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		du += uint64(info.Size())
		return nil
	})

	if err != nil {
		return 0, err
	}

	return du, nil
}

// LevelDB needs to be closed.
func (d *Datastore) Close() (err error) {
	d.closeLk.Lock()
	defer d.closeLk.Unlock()
	return d.DB.Close()
}

type leveldbBatch struct {
	b          *leveldb.Batch
	db         *leveldb.DB
	closeLk    *sync.RWMutex
	syncWrites bool
	ktype      key.KeyType
}

func (d *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	return &leveldbBatch{
		b:          new(leveldb.Batch),
		db:         d.DB,
		closeLk:    d.closeLk,
		syncWrites: d.syncWrites,
		ktype:      d.ktype,
	}, nil
}

func (b *leveldbBatch) Put(ctx context.Context, key key.Key, value []byte) error {
	if key.KeyType() != b.ktype {
		return ErrKeyTypeNotMatch
	}
	b.b.Put(key.Bytes(), value)
	return nil
}

func (b *leveldbBatch) Commit(ctx context.Context) error {
	b.closeLk.RLock()
	defer b.closeLk.RUnlock()
	return b.db.Write(b.b, &opt.WriteOptions{Sync: b.syncWrites})
}

func (b *leveldbBatch) Delete(ctx context.Context, key key.Key) error {
	if key.KeyType() != b.ktype {
		return ErrKeyTypeNotMatch
	}
	b.b.Delete(key.Bytes())
	return nil
}

// A leveldb transaction embedding the accessor backed by the transaction.
type transaction struct {
	*accessor
	tx *leveldb.Transaction
}

func (t *transaction) Commit(ctx context.Context) error {
	t.closeLk.RLock()
	defer t.closeLk.RUnlock()
	return t.tx.Commit()
}

func (t *transaction) Discard(ctx context.Context) {
	t.closeLk.RLock()
	defer t.closeLk.RUnlock()
	t.tx.Discard()
}

func (d *Datastore) NewTransaction(ctx context.Context, readOnly bool) (ds.Txn, error) {
	d.closeLk.RLock()
	defer d.closeLk.RUnlock()
	tx, err := d.DB.OpenTransaction()
	if err != nil {
		return nil, err
	}
	accessor := &accessor{
		ldb:        tx,
		syncWrites: false,
		ktype:      d.ktype,
		closeLk:    d.closeLk,
	}
	return &transaction{accessor, tx}, nil
}
