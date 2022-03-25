package dsbbolt

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/daotl/go-datastore"
	dskey "github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
	dstest "github.com/daotl/go-datastore/test"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

var bg = context.Background()

func Test_NewDatastore(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"Success", args{filepath.Join(t.TempDir(), "bolt")}, false},
		{"Fail", args{"/root/toor"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if ds, err := NewDatastore(tt.args.path, nil, nil, dskey.KeyTypeBytes); (err != nil) != tt.wantErr {
				t.Fatalf("NewDatastore() err = %v, wantErr %v", err, tt.wantErr)
			} else if !tt.wantErr {
				if err := ds.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func Test_Datastore(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "bolt")
	ds, err := NewDatastore(tmpFile, nil, nil, dskey.KeyTypeBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	ctx := context.Background()
	t.Run("cursor", func(t *testing.T) {
		bucketName := []byte("test_bucket")
		err := ds.db.Update(func(tx *bbolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(bucketName)
			err := b.Put([]byte("sa"), []byte("sa"))
			if err != nil {
				t.Error(err)
			}
			err = b.Put([]byte("sb"), []byte("sb"))
			if err != nil {
				t.Error(err)
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		ds.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(bucketName)
			cur := b.Cursor()
			for k, v := cur.Seek([]byte{}); k != nil; k, v = cur.Next() {
				fmt.Println(k, v)
			}
			return nil
		})
	})

	t.Run("nil key", func(t *testing.T) {
		err := ds.db.Update(func(tx *bbolt.Tx) error {
			return tx.Bucket(ds.bucket).Put([]byte{}, []byte("sd"))
		})
		if err == nil {
			t.Error("expected err")
		}
	})
	t.Run("put size 0 slice", func(t *testing.T) {
		k := dskey.NewBytesKeyFromString("size0slice")
		err := ds.Put(ctx, k, []byte{})
		if err != nil {
			t.Error(err)
		}
		v, err := ds.Get(ctx, k)
		if err != nil {
			t.Error(err)
		}
		if v == nil || len(v) != 0 {
			t.Error("val should have size 0")
		}
	})
	t.Run("put nil", func(t *testing.T) {
		k := dskey.NewBytesKeyFromString("nil")
		err := ds.Put(ctx, k, nil)
		if err != nil {
			t.Error(err)
		}
		v, err := ds.Get(ctx, k)
		if err != nil {
			t.Error(err)
		}
		if v == nil || len(v) != 0 {
			t.Error("val should have size 0")
		}
	})
	t.Run("basic", func(t *testing.T) {

		key := dskey.NewBytesKeyFromString("keks")
		key2 := dskey.NewBytesKeyFromString("keks2")
		if err := ds.Put(context.Background(), key, []byte("hello world")); err != nil {
			t.Fatal(err)
		}
		if err := ds.Put(context.Background(), key2, []byte("hello world")); err != nil {
			t.Fatal(err)
		}
		data, err := ds.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(data, []byte("hello world")) {
			t.Fatal("bad data")
		}
		if has, err := ds.Has(context.Background(), key); err != nil {
			t.Fatal(err)
		} else if !has {
			t.Fatal("should have key")
		}
		if size, err := ds.GetSize(context.Background(), key); err != nil {
			t.Fatal(err)
		} else if size != len([]byte("hello world")) {
			t.Fatal("incorrect data size")
		}
		// test a query where we specify a search key
		rs, err := ds.Query(context.Background(), query.Query{Prefix: key})
		if err != nil {
			t.Fatal(err)
		}
		res, err := rs.Rest()
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 1 {
			fmt.Printf("only found %v results \n", len(res))
			for _, v := range res {
				fmt.Printf("%+v\n", v)
			}
			t.Fatal("bad number of results")
		}
		// test a query where we dont specify a search key
		rs, err = ds.Query(context.Background(), query.Query{Prefix: dskey.EmptyBytesKey})
		if err != nil {
			t.Fatal(err)
		}
		res, err = rs.Rest()
		if err != nil {
			t.Fatal(err)
		}
		if len(res) == 0 {
			t.Fatal("bad number of results")
		}
		// test a query where we specify a partial prefix
		rs, err = ds.Query(context.Background(), query.Query{Prefix: dskey.NewBytesKeyFromString("kek")})
		if err != nil {
			t.Fatal(err)
		}
		res, err = rs.Rest()
		if err != nil {
			t.Fatal(err)
		}
		if len(res) == 0 {
			t.Fatal("bad number of results")
		}
		if err := ds.Delete(nil, key); err != nil {
			t.Fatal(err)
		}
		if has, err := ds.Has(context.Background(), key); err != nil {
			if err != datastore.ErrNotFound {
				t.Fatal(err)
			}
		} else if has {
			t.Fatal("should not have key")
		}
		if size, err := ds.GetSize(context.Background(), key); err != nil {
			if err != datastore.ErrNotFound {
				t.Fatal(err)
			}
		} else if size != 0 {
			t.Fatal("bad size")
		}
	})

}

func TestTxn(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "bolt")
	ds, err := NewDatastore(tmpFile, nil, nil, dskey.KeyTypeBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	ctx := context.Background()
	tx, err := ds.NewTransaction(ctx, false)
	assert.NoError(t, err)

	k := dskey.NewBytesKeyFromString("hi")
	val := []byte("hi")
	err = tx.Put(bg, k, val)
	assert.NoError(t, err)

	if err := tx.Commit(ctx); err != nil {
		t.Error(err)
	}

	tx1, err := ds.NewTransaction(ctx, true)
	assert.NoError(t, err)

	v, err := tx1.Get(bg, k)
	assert.NoError(t, err)

	if !bytes.Equal(v, val) {
		t.Error("get wrong value")
	}
	results, err := tx1.Query(bg, query.Query{Range: query.Range{Start: k}})
	assert.NoError(t, err)

	entries, err := results.Rest()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(entries))
	tx1.Discard(ctx)
}

func TestSuite(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "bolt")
	ds, err := NewDatastore(tmpFile, nil, nil, dskey.KeyTypeBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer func(ds *Datastore) {
		err := ds.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(ds)
	dstest.SubtestAll(t, dskey.KeyTypeBytes, ds)
}
