// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package delayed

import (
	"context"
	"testing"
	"time"

	"github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	dstest "github.com/daotl/go-datastore/test"
	delay "github.com/ipfs/go-ipfs-delay"
)

func TestDelayed(t *testing.T) {
	ctx := context.Background()

	d := New(dstest.NewMapDatastoreForTest(t, key.KeyTypeString), delay.Fixed(time.Second))
	now := time.Now()
	k := key.NewStrKey("test")
	err := d.Put(ctx, k, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Get(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(now) < 2*time.Second {
		t.Fatal("There should have been a delay of 1 second in put and in get")
	}
}

func TestDelayedAll(t *testing.T) {
	ds, err := datastore.NewMapDatastore(key.KeyTypeString)
	if err != nil {
		t.Fatal("error creating MapDatastore: ", err)
	}
	// Don't actually delay, we just want to make sure this works correctly, not that it
	// delays anything.
	dstest.SubtestAll(t, key.KeyTypeString, New(ds, delay.Fixed(time.Millisecond)))
}
