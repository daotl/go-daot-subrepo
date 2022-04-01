// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package mount

import (
	"testing"

	key "github.com/daotl/go-datastore/key"
	dsq "github.com/daotl/go-datastore/query"
	dstest "github.com/daotl/go-datastore/test"
)

func testLookup(t *testing.T, ktype key.KeyType) {
	mapds0 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds1 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds2 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds3 := dstest.NewMapDatastoreForTest(t, ktype)
	m := New([]Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/"), Datastore: mapds0},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/foo"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/bar"), Datastore: mapds2},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/baz"), Datastore: mapds3},
	})
	_, mnts, _, _ := m.lookupAll(key.NewKeyFromTypeAndString(ktype, "/bar"), dsq.Range{})
	if len(mnts) != 1 || !mnts[0].Equal(key.NewKeyFromTypeAndString(ktype, "/bar")) {
		t.Errorf("expected to find the mountpoint /bar, got %v", mnts)
	}

	if ktype == key.KeyTypeString {
		_, mnts, _, _ = m.lookupAll(key.NewKeyFromTypeAndString(ktype, "/fo"), dsq.Range{})
		if len(mnts) != 1 || !mnts[0].Equal(key.NewKeyFromTypeAndString(ktype, "/")) {
			t.Errorf("expected to find the mountpoint /, got %v", mnts)
		}

		_, mnt, _ := m.lookup(key.NewKeyFromTypeAndString(ktype, "/fo"))
		if !mnt.Equal(key.NewKeyFromTypeAndString(ktype, "/")) {
			t.Errorf("expected to find the mountpoint /, got %v", mnt)
		}
	}

	// /foo lives in /, /foo/bar lives in /foo. Most systems don't let us use the key "" or /.
	_, mnt, _ := m.lookup(key.NewKeyFromTypeAndString(ktype, "/foo"))
	if !mnt.Equal(key.NewKeyFromTypeAndString(ktype, "/")) {
		t.Errorf("expected to find the mountpoint /, got %v", mnt)
	}

	_, mnt, _ = m.lookup(key.NewKeyFromTypeAndString(ktype, "/foo/bar"))
	if !mnt.Equal(key.NewKeyFromTypeAndString(ktype, "/foo")) {
		t.Errorf("expected to find the mountpoint /foo, got %v", mnt)
	}
}

func TestLookup(t *testing.T) {
	testLookup(t, key.KeyTypeString)
	testLookup(t, key.KeyTypeBytes)
}
