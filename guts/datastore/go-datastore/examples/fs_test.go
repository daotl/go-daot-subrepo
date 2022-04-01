// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package examples

import (
	"bytes"
	"context"
	"testing"

	. "gopkg.in/check.v1"

	ds "github.com/daotl/go-datastore"
	key "github.com/daotl/go-datastore/key"
	query "github.com/daotl/go-datastore/query"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type DSSuite struct {
	dir string
	ds  ds.Datastore
}

var _ = Suite(&DSSuite{})

func (ks *DSSuite) SetUpTest(c *C) {
	ks.dir = c.MkDir()
	ks.ds, _ = NewDatastore(ks.dir)
}

func (ks *DSSuite) TestOpen(c *C) {
	_, err := NewDatastore("/tmp/foo/bar/baz")
	c.Assert(err, Not(Equals), nil)

	// setup ds
	_, err = NewDatastore(ks.dir)
	c.Assert(err, Equals, nil)
}

func (ks *DSSuite) TestBasic(c *C) {
	ctx := context.Background()

	keys := key.StrsToStrKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	for _, k := range keys {
		err := ks.ds.Put(ctx, k, k.Bytes())
		c.Check(err, Equals, nil)
	}

	for _, k := range keys {
		v, err := ks.ds.Get(ctx, k)
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v, k.Bytes()), Equals, true)
	}

	r, err := ks.ds.Query(ctx, query.Query{Prefix: key.QueryStrKey("/foo/bar/")})
	if err != nil {
		c.Check(err, Equals, nil)
	}

	expect := key.StrsToStrKeys([]string{
		"/foo/bar/baz",
		"/foo/bar/bazb",
		"/foo/bar/baz/barb",
	})
	all, err := r.Rest()
	if err != nil {
		c.Fatal(err)
	}
	c.Check(len(all), Equals, len(expect))

	for _, k := range expect {
		found := false
		for _, e := range all {
			if e.Key.Equal(k) {
				found = true
			}
		}

		if !found {
			c.Error("did not find expected key: ", k)
		}
	}
}

func (ks *DSSuite) TestDiskUsage(c *C) {
	ctx := context.Background()

	keys := key.StrsToStrKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	totalBytes := 0
	for _, k := range keys {
		value := k.Bytes()
		totalBytes += len(value)
		err := ks.ds.Put(ctx, k, value)
		c.Check(err, Equals, nil)
	}

	if ps, ok := ks.ds.(ds.PersistentDatastore); ok {
		if s, err := ps.DiskUsage(ctx); s != uint64(totalBytes) || err != nil {
			c.Error("unexpected size is: ", s)
		}
	} else {
		c.Error("should implement PersistentDatastore")
	}
}
