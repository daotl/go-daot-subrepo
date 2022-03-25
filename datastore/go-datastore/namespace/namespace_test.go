// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package namespace_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	. "gopkg.in/check.v1"

	ds "github.com/daotl/go-datastore"
	key "github.com/daotl/go-datastore/key"
	ns "github.com/daotl/go-datastore/namespace"
	dsq "github.com/daotl/go-datastore/query"
	dstest "github.com/daotl/go-datastore/test"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DSSuite struct{}

var _ = Suite(&DSSuite{})

func (ks *DSSuite) TestBasic(c *C) {
	ks.testBasic(c, "abc")
	ks.testBasic(c, "")
}

func (ks *DSSuite) testBasic(c *C, prefix string) {
	ctx := context.Background()

	mpds, _ := ds.NewMapDatastore(key.KeyTypeString)
	nsds := ns.Wrap(mpds, key.NewStrKey(prefix))

	keys := key.StrsToStrKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	for _, k := range keys {
		err := nsds.Put(ctx, k, k.Bytes())
		c.Check(err, Equals, nil)
	}

	for _, k := range keys {
		v1, err := nsds.Get(ctx, k)
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v1, k.Bytes()), Equals, true)

		v2, err := mpds.Get(ctx, key.NewStrKey(prefix).Child(k))
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v2, k.Bytes()), Equals, true)
	}

	run := func(d ds.Datastore, q dsq.Query) []key.Key {
		r, err := d.Query(ctx, q)
		c.Check(err, Equals, nil)

		e, err := r.Rest()
		c.Check(err, Equals, nil)

		return dsq.EntryKeys(e)
	}

	listA := run(mpds, dsq.Query{})
	listB := run(nsds, dsq.Query{})
	c.Check(len(listA), Equals, len(listB))

	// sort them cause yeah.
	sort.Sort(key.KeySlice(listA))
	sort.Sort(key.KeySlice(listB))

	for i, kA := range listA {
		kB := listB[i]
		c.Check(nsds.InvertKey(kA), Equals, kB)
		c.Check(kA, Equals, nsds.ConvertKey(kB))
	}
}

func (ks *DSSuite) TestQuery(c *C) {
	ctx := context.Background()

	mpds := dstest.NewTestDatastore(key.KeyTypeString, true)
	nsds := ns.Wrap(mpds, key.NewStrKey("/foo"))

	keys := key.StrsToStrKeys([]string{
		"abc/foo",
		"bar/foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/baz/abc",
		"xyz/foo",
	})

	for _, k := range keys {
		err := mpds.Put(ctx, k, k.Bytes())
		c.Check(err, Equals, nil)
	}

	qres, err := nsds.Query(ctx, dsq.Query{})
	c.Check(err, Equals, nil)

	expect := []dsq.Entry{
		{Key: key.NewStrKey("/bar"), Size: len([]byte("/foo/bar")), Value: []byte("/foo/bar")},
		{Key: key.NewStrKey("/bar/baz"), Size: len([]byte("/foo/bar/baz")), Value: []byte("/foo/bar/baz")},
		{Key: key.NewStrKey("/baz/abc"), Size: len([]byte("/foo/baz/abc")), Value: []byte("/foo/baz/abc")},
	}

	results, err := qres.Rest()
	c.Check(err, Equals, nil)
	sort.Slice(results, func(i, j int) bool { return results[i].Key.Less(results[j].Key) })

	for i, ent := range results {
		c.Check(ent.Key, Equals, expect[i].Key)
		c.Check(string(ent.Value), Equals, string(expect[i].Value))
	}

	err = qres.Close()
	c.Check(err, Equals, nil)

	qres, err = nsds.Query(ctx, dsq.Query{Prefix: key.QueryStrKey("/bar")})
	c.Check(err, Equals, nil)

	expect = []dsq.Entry{
		{Key: key.NewStrKey("/bar/baz"), Size: len([]byte("/foo/bar/baz")), Value: []byte("/foo/bar/baz")},
	}

	results, err = qres.Rest()
	c.Check(err, Equals, nil)
	sort.Slice(results, func(i, j int) bool { return results[i].Key.Less(results[j].Key) })

	for i, ent := range results {
		c.Check(ent.Key, Equals, expect[i].Key)
		c.Check(string(ent.Value), Equals, string(expect[i].Value))
	}

	if err := nsds.Check(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected Check() error: %s", err)
	}

	if err := nsds.CollectGarbage(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected CollectGarbage() error: %s", err)
	}

	if err := nsds.Scrub(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected Scrub() error: %s", err)
	}
}

func TestSuite(t *testing.T) {
	mpds := dstest.NewTestDatastore(key.KeyTypeString, true)
	nsds := ns.Wrap(mpds, key.NewStrKey("/foo"))
	dstest.SubtestAll(t, key.KeyTypeString, nsds)
}
