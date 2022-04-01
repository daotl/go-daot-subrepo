// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package mount_test

import (
	"context"
	"errors"
	"testing"

	"github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/autobatch"
	"github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/mount"
	"github.com/daotl/go-datastore/query"
	"github.com/daotl/go-datastore/sync"
	dstest "github.com/daotl/go-datastore/test"
)

func testPutBadNothing(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	m := mount.New(nil)

	err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "quux"), []byte("foobar"))
	if g, e := err, mount.ErrNoMount; g != e {
		t.Fatalf("Put got wrong error: %v != %v", g, e)
	}
}

func TestPutBadNothing(t *testing.T) {
	testPutBadNothing(t, key.KeyTypeString)
	testPutBadNothing(t, key.KeyTypeBytes)
}

func testPutBadNoMount(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/redherring"), Datastore: mapds},
	})

	err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"), []byte("foobar"))
	if g, e := err, mount.ErrNoMount; g != e {
		t.Fatalf("expected ErrNoMount, got: %v\n", g)
	}
}

func TestPutBadNoMount(t *testing.T) {
	testPutBadNoMount(t, key.KeyTypeString)
	testPutBadNoMount(t, key.KeyTypeBytes)
}

func testPut(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/quux"), Datastore: mapds},
	})

	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"),
		[]byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	buf, err := mapds.Get(ctx, key.NewKeyFromTypeAndString(ktype, "/thud"))
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if g, e := string(buf), "foobar"; g != e {
		t.Errorf("wrong value: %q != %q", g, e)
	}
}

func TestPut(t *testing.T) {
	testPut(t, key.KeyTypeString)
	testPut(t, key.KeyTypeBytes)
}

func testGetBadNothing(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	m := mount.New([]mount.Mount{})

	_, err := m.Get(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetBadNothing(t *testing.T) {
	testGetBadNothing(t, key.KeyTypeString)
	testGetBadNothing(t, key.KeyTypeBytes)
}

func testGetBadNoMount(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/redherring"), Datastore: mapds},
	})

	_, err := m.Get(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetBadNoMount(t *testing.T) {
	testGetBadNoMount(t, key.KeyTypeString)
	testGetBadNoMount(t, key.KeyTypeBytes)
}

func testGetNotFound(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/quux"), Datastore: mapds},
	})

	_, err := m.Get(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetNotFound(t *testing.T) {
	testGetNotFound(t, key.KeyTypeString)
	testGetNotFound(t, key.KeyTypeBytes)
}

func testGet(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/quux"), Datastore: mapds},
	})

	if err := mapds.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/thud"),
		[]byte("foobar")); err != nil {
		t.Fatalf("Get error: %v", err)
	}

	buf, err := m.Get(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"))
	if err != nil {
		t.Fatalf("Put error: %v", err)
	}
	if g, e := string(buf), "foobar"; g != e {
		t.Errorf("wrong value: %q != %q", g, e)
	}
}

func TestGet(t *testing.T) {
	testGet(t, key.KeyTypeString)
	testGet(t, key.KeyTypeBytes)
}

func testHasBadNothing(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	m := mount.New([]mount.Mount{})

	found, err := m.Has(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHasBadNothing(t *testing.T) {
	testHasBadNothing(t, key.KeyTypeString)
	testHasBadNothing(t, key.KeyTypeBytes)
}

func testHasBadNoMount(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/redherring"), Datastore: mapds},
	})

	found, err := m.Has(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHasBadNoMount(t *testing.T) {
	testHasBadNoMount(t, key.KeyTypeString)
	testHasBadNoMount(t, key.KeyTypeBytes)
}

func testHasNotFound(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/quux"), Datastore: mapds},
	})

	found, err := m.Has(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHasNotFound(t *testing.T) {
	testHasNotFound(t, key.KeyTypeString)
	testHasNotFound(t, key.KeyTypeBytes)
}

func testHas(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/quux"), Datastore: mapds},
	})

	if err := mapds.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/thud"),
		[]byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	found, err := m.Has(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestHas(t *testing.T) {
	testHas(t, key.KeyTypeString)
	testHas(t, key.KeyTypeBytes)
}

func testDeleteNotFound(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/quux"), Datastore: mapds},
	})

	err := m.Delete(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"))
	if err != nil {
		t.Fatalf("expected nil, got: %v\n", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	testDeleteNotFound(t, key.KeyTypeString)
	testDeleteNotFound(t, key.KeyTypeBytes)
}

func testDelete(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/quux"), Datastore: mapds},
	})

	if err := mapds.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/thud"),
		[]byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	err := m.Delete(ctx, key.NewKeyFromTypeAndString(ktype, "/quux/thud"))
	if err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	// make sure it disappeared
	found, err := mapds.Has(ctx, key.NewKeyFromTypeAndString(ktype, "/thud"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestDelete(t *testing.T) {
	testDelete(t, key.KeyTypeString)
	testDelete(t, key.KeyTypeBytes)
}

func testQuerySimple(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/quux"), Datastore: mapds},
	})

	myKey := key.NewKeyFromTypeAndString(ktype, "/quux/thud")
	if err := m.Put(ctx, myKey, []byte("foobar")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	res, err := m.Query(ctx, query.Query{Prefix: key.QueryKeyFromTypeAndString(ktype, "/quux")})
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}
	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}
	seen := false
	for _, e := range entries {
		if e.Key.Equal(myKey) {
			seen = true
		} else {
			t.Errorf("saw unexpected key: %q", e.Key)
		}
	}
	if !seen {
		t.Errorf("did not see wanted key %q in %+v", myKey, entries)
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQuerySimple(t *testing.T) {
	testQuerySimple(t, key.KeyTypeString)
	testQuerySimple(t, key.KeyTypeBytes)
}

func testQueryAcrossMounts(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds0 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds1 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds2 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds3 := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/foo"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/bar"), Datastore: mapds2},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/baz"), Datastore: mapds3},
		{Prefix: key.EmptyKeyFromType(ktype), Datastore: mapds0},
	})

	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/foo/lorem"),
		[]byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/bar/ipsum"),
		[]byte("234")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/bar/dolor"),
		[]byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/baz/sit"),
		[]byte("456")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/banana"), []byte("567")); err != nil {
		t.Fatal(err)
	}

	expect := func(prefix string, values map[string]string) {
		t.Helper()
		res, err := m.Query(ctx, query.Query{Prefix: key.QueryKeyFromTypeAndString(ktype, prefix)})
		if err != nil {
			t.Fatalf("Query fail: %v\n", err)
		}
		entries, err := res.Rest()
		if err != nil {
			err = res.Close()
			if err != nil {
				t.Errorf("result.Close failed %d", err)
			}
			t.Fatalf("Query Results.Rest fail: %v\n", err)
		}
		if len(entries) != len(values) {
			t.Errorf("expected %d results, got %d", len(values), len(entries))
		}
		for _, e := range entries {
			v, ok := values[e.Key.String()]
			if !ok {
				t.Errorf("unexpected key %s", e.Key)
				continue
			}

			if v != string(e.Value) {
				t.Errorf("key value didn't match expected %s: '%s' - '%s'", e.Key, v, e.Value)
			}

			values[e.Key.String()] = "seen"
		}
	}

	expect("/ba", nil)
	expect("/bar", map[string]string{
		"/bar/ipsum": "234",
		"/bar/dolor": "345",
	})
	expect("/baz/", map[string]string{
		"/baz/sit": "456",
	})
	expect("/foo", map[string]string{
		"/foo/lorem": "123",
	})
	expect("/", map[string]string{
		"/foo/lorem": "123",
		"/bar/ipsum": "234",
		"/bar/dolor": "345",
		"/baz/sit":   "456",
		"/banana":    "567",
	})
	expect("/banana", nil)
}

func TestQueryAcrossMounts(t *testing.T) {
	testQueryAcrossMounts(t, key.KeyTypeString)
	// Expected to fail for BytesKey
	//testQueryAcrossMounts(t, key.KeyTypeBytes)
}

func testQueryAcrossMountsWithSort(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds0 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds1 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds2 := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/zoo"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/boo/5"), Datastore: mapds2},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/boo"), Datastore: mapds0},
	})

	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/1"), []byte("234")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/boo/9"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/boo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/boo/5/hello"),
		[]byte("789")); err != nil {
		t.Fatal(err)
	}

	res, err := m.Query(ctx, query.Query{Orders: []query.Order{query.OrderByKey{}}})
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}
	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := key.StrsToStrKeys([]string{
		"/boo/3",
		"/boo/5/hello",
		"/boo/9",
		"/zoo/0",
		"/zoo/1",
	})

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if e != entries[i].Key {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryAcrossMountsWithSort(t *testing.T) {
	testQueryAcrossMountsWithSort(t, key.KeyTypeString)
	// Expected to fail for BytesKey
	//testQueryAcrossMountsWithSort(t, key.KeyTypeBytes)
}

func testQueryLimitAcrossMountsWithSort(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	mapds3 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/rok"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/zoo"), Datastore: mapds2},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/noop"), Datastore: mapds3},
	})

	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/0"), []byte("ghi")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/1"), []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/2"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/3"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Limit: 2, Orders: []query.Order{query.OrderByKeyDescending{}}}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := key.StrsToStrKeys([]string{
		"/zoo/3",
		"/zoo/2",
	})

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if !e.Equal(entries[i].Key) {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitAcrossMountsWithSort(t *testing.T) {
	testQueryLimitAcrossMountsWithSort(t, key.KeyTypeString)
	// Expected to fail for BytesKey
	//testQueryLimitAcrossMountsWithSort(t, key.KeyTypeBytes)
}

func testQueryLimitAndOffsetAcrossMountsWithSort(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	mapds3 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/rok"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/zoo"), Datastore: mapds2},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/noop"), Datastore: mapds3},
	})

	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/0"), []byte("ghi")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/1"), []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/2"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/3"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Limit: 3, Offset: 2, Orders: []query.Order{query.OrderByKey{}}}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := key.StrsToStrKeys([]string{
		"/rok/3",
		"/zoo/0",
		"/zoo/1",
	})

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if !e.Equal(entries[i].Key) {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitAndOffsetAcrossMountsWithSort(t *testing.T) {
	testQueryLimitAndOffsetAcrossMountsWithSort(t, key.KeyTypeString)
	// Expected to fail for BytesKey
	//testQueryLimitAndOffsetAcrossMountsWithSort(t, key.KeyTypeBytes)
}

func testQueryFilterAcrossMountsWithSort(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	mapds3 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/rok"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/zoo"), Datastore: mapds2},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/noop"), Datastore: mapds3},
	})

	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/0"), []byte("ghi")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/1"), []byte("def")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/2"), []byte("345")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/3"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/3"), []byte("456")); err != nil {
		t.Fatal(err)
	}

	f := &query.FilterKeyCompare{Op: query.Equal, Key: key.QueryKeyFromTypeAndString(
		ktype, "/rok/3")}
	q := query.Query{Filters: []query.Filter{f}}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := key.StrsToStrKeys([]string{
		"/rok/3",
	})

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	for i, e := range expect {
		if !e.Equal(entries[i].Key) {
			t.Errorf("expected key %s, but got %s", e, entries[i].Key)
		}
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryFilterAcrossMountsWithSort(t *testing.T) {
	testQueryFilterAcrossMountsWithSort(t, key.KeyTypeString)
	// Expected to fail for BytesKey
	//testQueryFilterAcrossMountsWithSort(t, key.KeyTypeBytes)
}

func testQueryLimitAndOffsetWithNoData(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/rok"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/zoo"), Datastore: mapds2},
	})

	q := query.Query{Limit: 4, Offset: 3}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitAndOffsetWithNoData(t *testing.T) {
	testQueryLimitAndOffsetWithNoData(t, key.KeyTypeString)
	testQueryLimitAndOffsetWithNoData(t, key.KeyTypeBytes)
}

func testQueryLimitWithNotEnoughData(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/rok"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/zoo"), Datastore: mapds2},
	})

	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Limit: 4}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{
		"/zoo/0",
		"/rok/1",
	}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryLimitWithNotEnoughData(t *testing.T) {
	testQueryLimitWithNotEnoughData(t, key.KeyTypeString)
	testQueryLimitWithNotEnoughData(t, key.KeyTypeBytes)
}

func testQueryOffsetWithNotEnoughData(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds1 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	mapds2 := sync.MutexWrap(dstest.NewMapDatastoreForTest(t, ktype))
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/rok"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/zoo"), Datastore: mapds2},
	})

	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/zoo/0"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/rok/1"), []byte("167")); err != nil {
		t.Fatal(err)
	}

	q := query.Query{Offset: 4}
	res, err := m.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}

	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}

	expect := []string{}

	if len(entries) != len(expect) {
		t.Fatalf("expected %d entries, but got %d", len(expect), len(entries))
	}

	err = res.Close()
	if err != nil {
		t.Errorf("result.Close failed %d", err)
	}
}

func TestQueryOffsetWithNotEnoughData(t *testing.T) {
	testQueryOffsetWithNotEnoughData(t, key.KeyTypeString)
	testQueryOffsetWithNotEnoughData(t, key.KeyTypeBytes)
}

func testLookupPrio(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds0 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds1 := dstest.NewMapDatastoreForTest(t, ktype)

	m := mount.New([]mount.Mount{
		{Prefix: key.EmptyKeyFromType(ktype), Datastore: mapds0},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/foo"), Datastore: mapds1},
	})

	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/foo/bar"),
		[]byte("123")); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/baz"), []byte("234")); err != nil {
		t.Fatal(err)
	}

	found, err := mapds0.Has(ctx, key.NewKeyFromTypeAndString(ktype, "/baz"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}

	found, err = mapds0.Has(ctx, key.NewKeyFromTypeAndString(ktype, "/foo/bar"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}

	found, err = mapds1.Has(ctx, key.NewKeyFromTypeAndString(ktype, "/bar"))
	if err != nil {
		t.Fatalf("Has error: %v", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong value: %v != %v", g, e)
	}
}

func TestLookupPrio(t *testing.T) {
	testLookupPrio(t, key.KeyTypeString)
	testLookupPrio(t, key.KeyTypeBytes)
}

func testNestedMountSync(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	internalDSRoot := dstest.NewMapDatastoreForTest(t, ktype)
	internalDSFoo := dstest.NewMapDatastoreForTest(t, ktype)
	internalDSFooBar := dstest.NewMapDatastoreForTest(t, ktype)

	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/foo"), Datastore: autobatch.NewAutoBatching(
			internalDSFoo, 10)},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/foo/bar"),
			Datastore: autobatch.NewAutoBatching(
				internalDSFooBar, 10)},
		{Prefix: key.EmptyKeyFromType(ktype), Datastore: autobatch.NewAutoBatching(
			internalDSRoot, 10)},
	})

	// Testing scenarios
	// 1) Make sure child(ren) sync
	// 2) Make sure parent syncs
	// 3) Make sure parent only syncs the relevant subtree (instead of fully syncing)

	addToDS := func(str string) {
		t.Helper()
		if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, str), []byte(str)); err != nil {
			t.Fatal(err)
		}
	}

	checkVal := func(d datastore.Datastore, str string, expectFound bool) {
		t.Helper()
		res, err := d.Has(ctx, key.NewKeyFromTypeAndString(ktype, str))
		if err != nil {
			t.Fatal(err)
		}
		if res != expectFound {
			if expectFound {
				t.Fatal("datastore is missing key")
			}
			t.Fatal("datastore has key it should not have")
		}
	}

	// Add /foo/bar/0, Add /foo/bar/0/1, Add /foo/baz, Add /beep/bop, Sync /foo: all added except last - checks 1 and 2
	addToDS("/foo/bar/0")
	addToDS("/foo/bar/1")
	addToDS("/foo/baz")
	addToDS("/beep/bop")

	if err := m.Sync(ctx, key.NewKeyFromTypeAndString(ktype, "/foo")); err != nil {
		t.Fatal(err)
	}

	checkVal(internalDSFooBar, "/0", true)
	checkVal(internalDSFooBar, "/1", true)
	checkVal(internalDSFoo, "/baz", true)
	checkVal(internalDSRoot, "/beep/bop", false)

	// Add /fwop Add /bloop Sync /fwop, both added - checks 3
	addToDS("/fwop")
	addToDS("/bloop")

	if err := m.Sync(ctx, key.NewKeyFromTypeAndString(ktype, "/fwop")); err != nil {
		t.Fatal(err)
	}

	checkVal(internalDSRoot, "/fwop", true)
	checkVal(internalDSRoot, "/bloop", false)
}

func TestNestedMountSync(t *testing.T) {
	testNestedMountSync(t, key.KeyTypeString)
	testNestedMountSync(t, key.KeyTypeBytes)
}

type errQueryDS struct {
	datastore.NullDatastore
}

func (d *errQueryDS) Query(ctx context.Context, q query.Query) (query.Results, error) {
	return nil, errors.New("test error")
}

func testErrQueryClose(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	eqds := &errQueryDS{}
	mds := dstest.NewMapDatastoreForTest(t, ktype)

	m := mount.New([]mount.Mount{
		{Prefix: key.EmptyKeyFromType(ktype), Datastore: mds},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/foo"), Datastore: eqds},
	})

	if err := m.Put(ctx, key.NewKeyFromTypeAndString(ktype, "/baz"), []byte("123")); err != nil {
		t.Fatal(err)
	}

	_, err := m.Query(ctx, query.Query{})
	if err == nil {
		t.Fatal("expected query to fail")
		return
	}
}

func TestErrQueryClose(t *testing.T) {
	testErrQueryClose(t, key.KeyTypeString)
	testErrQueryClose(t, key.KeyTypeBytes)
}

func testMaintenanceFunctions(t *testing.T, ktype key.KeyType) {
	ctx := context.Background()

	mapds := dstest.NewTestDatastore(key.KeyTypeString, true)
	m := mount.New([]mount.Mount{
		{Prefix: key.EmptyKeyFromType(ktype), Datastore: mapds},
	})

	var path string
	switch ktype {
	case key.KeyTypeString:
		path ="/"
	case key.KeyTypeBytes:
		path =""
	default:
		panic(key.ErrKeyTypeNotSupported)
	}

	if err := m.Check(ctx); err.Error() != "checking datastore at " + path + ": test error" {
		t.Errorf("Unexpected Check() error: %s", err)
	}

	if err := m.CollectGarbage(ctx); err.Error() != "gc on datastore at " + path + ": test error" {
		t.Errorf("Unexpected CollectGarbage() error: %s", err)
	}

	if err := m.Scrub(ctx); err.Error() != "scrubbing datastore at " + path + ": test error" {
		t.Errorf("Unexpected Scrub() error: %s", err)
	}
}

func TestMaintenanceFunctions(t *testing.T) {
	testMaintenanceFunctions(t, key.KeyTypeString)
	testMaintenanceFunctions(t, key.KeyTypeBytes)
}

func testSuite(t *testing.T, ktype key.KeyType) {
	mapds0 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds1 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds2 := dstest.NewMapDatastoreForTest(t, ktype)
	mapds3 := dstest.NewMapDatastoreForTest(t, ktype)
	m := mount.New([]mount.Mount{
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/prefix"), Datastore: mapds1},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/prefix/sub"), Datastore: mapds2},
		{Prefix: key.NewKeyFromTypeAndString(ktype, "/0"), Datastore: mapds3},
		{Prefix: key.EmptyKeyFromType(ktype), Datastore: mapds0},
	})
	dstest.SubtestAll(t, ktype, m)
}

func TestSuite(t *testing.T) {
	testSuite(t, key.KeyTypeString)
	testSuite(t, key.KeyTypeBytes)
}
