package lazy

import (
	"testing"

	"github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	dstest "github.com/daotl/go-datastore/test"
)

func TestLazy(t *testing.T) {
	tds := dstest.NewMapDatastoreForTest(t, key.KeyTypeString)
	activateFn := func(d datastore.Datastore) error {
		return nil
	}
	deactivateFn := func(d datastore.Datastore) error {
		return nil
	}
	closeFn := func(d datastore.Datastore) error {
		return d.Close()
	}
	ds, err := NewLazyDataStore(tds, activateFn, deactivateFn, closeFn)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	dstest.SubtestAll(t, key.KeyTypeString, ds)
}
