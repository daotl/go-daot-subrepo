package dsbbolt

import (
	"bytes"
	"context"
	"errors"
	"os"

	"github.com/daotl/go-datastore"
	dskey "github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
	"go.etcd.io/bbolt"
)

var ErrKeyTypeNotMatch = errors.New("key type does not match")

var (
	defaultBucket                        = []byte("datastore")
	_             datastore.TxnDatastore = (*Datastore)(nil)
)

// Datastore implements a daotl datastore
// backed by a bbolt db, only byteskey is supported now
type Datastore struct {
	db     *bbolt.DB
	bucket []byte // only use one bucket?
	ktype  dskey.KeyType
}

// Sync is not required for boltdb, so no op
func (d *Datastore) Sync(ctx context.Context, prefix dskey.Key) error {
	return nil
}

// NewDatastore is used to instantiate our datastore
func NewDatastore(path string, opts *bbolt.Options, bucket []byte, keytype dskey.KeyType) (*Datastore, error) {
	if keytype != dskey.KeyTypeBytes {
		return nil, ErrKeyTypeNotMatch
	}
	db, err := bbolt.Open(path, os.FileMode(0640), opts)
	if err != nil {
		return nil, err
	}
	if bucket == nil {
		bucket = defaultBucket
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	ds := &Datastore{db: db, bucket: bucket, ktype: keytype}
	return ds, nil
}

// Put is used to store something in our underlying datastore
func (d *Datastore) Put(ctx context.Context, key dskey.Key, value []byte) error {
	if key.KeyType() != d.ktype {
		return ErrKeyTypeNotMatch
	}
	return d.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(d.bucket).Put(key.Bytes(), value)
	})
}

// Delete removes a key/value pair from our datastore
func (d *Datastore) Delete(ctx context.Context, key dskey.Key) error {
	if key.KeyType() != d.ktype {
		return ErrKeyTypeNotMatch
	}
	return d.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(d.bucket).Delete(key.Bytes())
	})
}

// Get is used to retrieve a value from the datastore
func (d *Datastore) Get(ctx context.Context, key dskey.Key) ([]byte, error) {
	if key.KeyType() != d.ktype {
		return nil, ErrKeyTypeNotMatch
	}
	var result []byte
	if err := d.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(d.bucket).Get(key.Bytes())
		if data == nil {
			return datastore.ErrNotFound
		}
		result = copyBytes(data)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// Has returns whether the key is present in our datastore
func (d *Datastore) Has(ctx context.Context, key dskey.Key) (bool, error) {
	if key.KeyType() != d.ktype {
		return false, ErrKeyTypeNotMatch
	}
	return datastore.GetBackedHas(ctx, d, key)
}

// GetSize returns the size of the value referenced by key
func (d *Datastore) GetSize(ctx context.Context, key dskey.Key) (int, error) {
	if key.KeyType() != d.ktype {
		return -1, ErrKeyTypeNotMatch
	}

	return datastore.GetBackedSize(ctx, d, key)
}

// return true if type mismatch
func keyTypeMismatch(q dskey.Key, keyType dskey.KeyType) bool {
	if q != nil && q.KeyType() != keyType {
		return true
	}
	return false
}

func queryWithCursor(cursor *bbolt.Cursor, q query.Query, ktype dskey.KeyType, closef func() error) (query.Results, error) {
	if keyTypeMismatch(q.Prefix, ktype) ||
		keyTypeMismatch(q.Range.Start, ktype) ||
		keyTypeMismatch(q.Range.End, ktype) {
		return nil, ErrKeyTypeNotMatch
	}

	qNaive := q // copy of q
	var cursorStart []byte
	var cursorEnd []byte

	if q.Prefix != nil {
		switch ktype {
		case dskey.KeyTypeBytes:
			cursorStart, cursorEnd = bytesPrefix(q.Prefix.Bytes())
		case dskey.KeyTypeString:
			// not supported now
			return nil, ErrKeyTypeNotMatch
		}
	}

	// cursor starting from max(prefix, range.start)
	if q.Range.Start != nil {
		rangeStartKey := q.Range.Start
		switch ktype {
		case dskey.KeyTypeBytes:
			rangeStartBytes := rangeStartKey.Bytes()
			if len(cursorStart) == 0 || bytes.Compare(cursorStart, rangeStartBytes) < 0 {
				cursorStart = rangeStartBytes
			}
		case dskey.KeyTypeString:
			// not supported now
			return nil, ErrKeyTypeNotMatch
		}
	}

	// cursor end with min(prefix limit, range.end)
	if q.Range.End != nil {
		rangeEndKey := q.Range.End
		switch ktype {
		case dskey.KeyTypeBytes:
			rangeEndBytes := rangeEndKey.Bytes()
			if len(cursorEnd) == 0 || bytes.Compare(rangeEndBytes, cursorEnd) < 0 {
				cursorEnd = rangeEndBytes
			}
		case dskey.KeyTypeString:
			// not supported now
			return nil, ErrKeyTypeNotMatch
		}
	}

	firstKv := func() ([]byte, []byte) {
		if len(cursorStart) == 0 {
			return cursor.First()
		} else {
			return cursor.Seek(cursorStart)
		}
	}
	validate := func(k []byte) bool {
		if k == nil {
			return false
		}
		if len(cursorEnd) != 0 && bytes.Compare(k, cursorEnd) >= 0 {
			return false
		}
		return true
	}
	next := cursor.Next
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case query.OrderByKey, *query.OrderByKey:
			qNaive.Orders = nil
		case query.OrderByKeyDescending, *query.OrderByKeyDescending:
			next = cursor.Prev
			firstKv = func() ([]byte, []byte) {
				if len(cursorEnd) == 0 {
					return cursor.Last()
				}
				cursor.Seek(cursorEnd)
				return cursor.Prev()
			}
			validate = func(k []byte) bool {
				if k == nil {
					return false
				}
				if len(cursorStart) != 0 && bytes.Compare(k, cursorStart) < 0 {
					return false
				}
				return true
			}
			qNaive.Orders = nil
		default:
		}
	}

	qNaive.Prefix = nil
	qNaive.Range = query.Range{}

	started := false
	results := query.ResultsFromIterator(q, query.Iterator{
		Next: func() (query.Result, bool) {
			var k, v []byte
			if !started {
				k, v = firstKv()
				started = true
			} else {
				k, v = next()
			}
			if validate(k) == false {
				return query.Result{}, false
			}
			return query.Result{
				Entry: toQueryEntry(k, v, q.KeysOnly),
			}, true
		},
		Close: func() error {
			if closef != nil {
				return closef()
			}
			return nil
		},
	})

	results = query.NaiveQueryApply(qNaive, results)
	return results, nil
}

// Query performs a complex search query on the underlying datastore
// For more information see :
// https://github.com/ipfs/go-datastore/blob/aa9190c18f1576be98e974359fd08c64ca0b5a94/examples/fs.go#L96
// https://github.com/etcd-io/bbolt#prefix-scans
func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	var results query.Results
	tx, err := d.db.Begin(false)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket(d.bucket)
	cursor := bucket.Cursor()
	results, err = queryWithCursor(cursor, q, d.ktype, func() error {
		return tx.Rollback()
	})

	return results, err
}

// Batch returns a basic batched bolt datastore wrapper
// it is a temporary method until we implement a proper
// transactional batched datastore
//func (d *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
//	return datastore.NewBasicBatch(d), nil
//}

// Close is used to close the underlying datastore
func (d *Datastore) Close() error {
	return d.db.Close()
}
