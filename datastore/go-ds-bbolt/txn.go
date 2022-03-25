package dsbbolt

import (
	"context"

	"github.com/daotl/go-datastore"
	dskey "github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
	"go.etcd.io/bbolt"
)

func (d *Datastore) NewTransaction(ctx context.Context, readOnly bool) (datastore.Txn, error) {
	tx, err := d.db.Begin(!readOnly)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket(d.bucket)

	return &txn{tx: tx, ktype: d.ktype, bucket: bucket}, nil
}

type txn struct {
	tx     *bbolt.Tx
	bucket *bbolt.Bucket
	ktype  dskey.KeyType
}

func (b *txn) Get(ctx context.Context, key dskey.Key) ([]byte, error) {
	if key.KeyType() != b.ktype {
		return nil, ErrKeyTypeNotMatch
	}

	data := b.bucket.Get(key.Bytes())
	if data == nil {
		return nil, datastore.ErrNotFound
	}
	return copyBytes(data), nil
}

func (b *txn) Has(ctx context.Context, key dskey.Key) (exists bool, err error) {
	if key.KeyType() != b.ktype {
		return false, ErrKeyTypeNotMatch
	}

	data := b.bucket.Get(key.Bytes())
	if data == nil {
		return false, nil
	}
	return true, nil
}

func (b *txn) GetSize(ctx context.Context, key dskey.Key) (int, error) {
	if key.KeyType() != b.ktype {
		return -1, ErrKeyTypeNotMatch
	}

	data := b.bucket.Get(key.Bytes())
	if data == nil {
		return -1, datastore.ErrNotFound
	}
	return len(data), nil
}

func (b *txn) Query(ctx context.Context, q query.Query) (query.Results, error) {
	cursor := b.bucket.Cursor()
	return queryWithCursor(cursor, q, b.ktype, nil)
}

func (b *txn) Put(ctx context.Context, key dskey.Key, value []byte) error {
	if key.KeyType() != b.ktype {
		return ErrKeyTypeNotMatch
	}
	return b.bucket.Put(key.Bytes(), value)
}

func (b *txn) Delete(ctx context.Context, key dskey.Key) error {
	if key.KeyType() != b.ktype {
		return ErrKeyTypeNotMatch
	}
	return b.bucket.Delete(key.Bytes())
}

// Commit calls the underlying bolt Commit
func (b *txn) Commit(ctx context.Context) error {
	return b.tx.Commit()
}

// Discard calls the underlying bolt Rollback. It closes the transaction and ignores all previous updates.
// Read-only transactions must be rolled back and not committed.
func (b *txn) Discard(ctx context.Context) {
	b.tx.Rollback()
	return
}
