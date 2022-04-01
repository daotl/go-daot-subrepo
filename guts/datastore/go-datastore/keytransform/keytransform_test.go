// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package keytransform_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	. "gopkg.in/check.v1"

	ds "github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	kt "github.com/daotl/go-datastore/keytransform"
	dsq "github.com/daotl/go-datastore/query"
	dstest "github.com/daotl/go-datastore/test"
)

// Hook up gocheck into the "go test" runner.
func TestStrKey(t *testing.T) { TestingT(t) }

type (
	StrKeySuite   struct{}
	BytesKeySuite struct{}
)

var (
	_ = Suite(&StrKeySuite{})
	_ = Suite(&BytesKeySuite{})
)

var strKeyPair = &kt.Pair{
	Convert: func(k key.Key) key.Key {
		return key.NewStrKey("/abc").Child(k)
	},
	Invert: func(k key.Key) key.Key {
		// remove abc prefix
		l := k.(key.StrKey).List()
		if l[0] != "abc" {
			panic("key does not have prefix. convert failed?")
		}
		return key.KeyWithNamespaces(l[1:])
	},
}

var bytesKeyPair = &kt.Pair{
	Convert: func(k key.Key) key.Key {
		return key.NewBytesKeyFromString("abc").Child(k)
	},
	Invert: func(k key.Key) key.Key {
		prefix := key.NewBytesKeyFromString("abc")
		// remove abc prefix
		if !k.HasPrefix(prefix) {
			panic("key does not have prefix. convert failed?")
		}
		return k.TrimPrefix(prefix)
	},
}

func (ks *StrKeySuite) TestStrKeyBasic(c *C) {
	ctx := context.Background()

	mpds := dstest.NewTestDatastore(key.KeyTypeString, true)
	ktds := kt.Wrap(mpds, strKeyPair)

	keys := key.StrsToStrKeys([]string{
		"foo",
		"foo/bar",
		"foo/bar/baz",
		"foo/barb",
		"foo/bar/bazb",
		"foo/bar/baz/barb",
	})

	for _, k := range keys {
		err := ktds.Put(ctx, k, k.Bytes())
		c.Check(err, Equals, nil)
	}

	for _, k := range keys {
		v1, err := ktds.Get(ctx, k)
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v1, k.Bytes()), Equals, true)

		v2, err := mpds.Get(ctx, key.NewStrKey("abc").Child(k))
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
	listB := run(ktds, dsq.Query{})
	c.Check(len(listA), Equals, len(listB))

	// sort them cause yeah.
	sort.Sort(key.KeySlice(listA))
	sort.Sort(key.KeySlice(listB))

	for i, kA := range listA {
		kB := listB[i]
		c.Check(strKeyPair.Invert(kA), Equals, kB)
		c.Check(kA, Equals, strKeyPair.Convert(kB))
	}

	c.Log("listA: ", listA)
	c.Log("listB: ", listB)

	if err := ktds.Check(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected Check() error: %s", err)
	}

	if err := ktds.CollectGarbage(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected CollectGarbage() error: %s", err)
	}

	if err := ktds.Scrub(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected Scrub() error: %s", err)
	}
}

func (ks *BytesKeySuite) TestBytesKeyBasic(c *C) {
	ctx := context.Background()

	mpds := dstest.NewTestDatastore(key.KeyTypeBytes, true)
	ktds := kt.Wrap(mpds, bytesKeyPair)

	keys := key.StrsToBytesKeys([]string{
		"foo",
		"foobar",
		"foobarbaz",
		"foobarb",
		"foobarbazb",
		"foobarbazbarb",
	})

	for _, k := range keys {
		err := ktds.Put(ctx, k, k.Bytes())
		c.Check(err, Equals, nil)
	}

	for _, k := range keys {
		v1, err := ktds.Get(ctx, k)
		c.Check(err, Equals, nil)
		c.Check(bytes.Equal(v1, k.Bytes()), Equals, true)

		v2, err := mpds.Get(ctx, key.NewBytesKeyFromString("abc").Child(k))
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
	listB := run(ktds, dsq.Query{})
	c.Check(len(listA), Equals, len(listB))

	// sort them cause yeah.
	sort.Sort(key.KeySlice(listA))
	sort.Sort(key.KeySlice(listB))

	for i, kA := range listA {
		kB := listB[i]
		c.Check(bytesKeyPair.Invert(kA).Equal(kB), Equals, true)
		c.Check(kA.Equal(bytesKeyPair.Convert(kB)), Equals, true)
	}

	c.Log("listA: ", listA)
	c.Log("listB: ", listB)

	if err := ktds.Check(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected Check() error: %s", err)
	}

	if err := ktds.CollectGarbage(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected CollectGarbage() error: %s", err)
	}

	if err := ktds.Scrub(ctx); err != dstest.ErrTest {
		c.Errorf("Unexpected Scrub() error: %s", err)
	}
}

func TestSuiteStrKeyDefaultPair(t *testing.T) {
	mpds := dstest.NewTestDatastore(key.KeyTypeString, true)
	ktds := kt.Wrap(mpds, strKeyPair)
	dstest.SubtestAll(t, key.KeyTypeString, ktds)
}

func TestSuiteBytesKeyDefaultPair(t *testing.T) {
	mpds := dstest.NewTestDatastore(key.KeyTypeBytes, true)
	ktds := kt.Wrap(mpds, bytesKeyPair)
	dstest.SubtestAll(t, key.KeyTypeBytes, ktds)
}

func testSuiteStrKeyPrefixTransform(t *testing.T, ktype key.KeyType) {
	mpds := dstest.NewTestDatastore(ktype, true)
	ktds := kt.Wrap(mpds, kt.PrefixTransform{Prefix: key.NewKeyFromTypeAndString(ktype, "foo")})
	dstest.SubtestAll(t, ktype, ktds)
}

func TestSuiteBytesKeyPrefixTransform(t *testing.T) {
	testSuiteStrKeyPrefixTransform(t, key.KeyTypeString)
	testSuiteStrKeyPrefixTransform(t, key.KeyTypeBytes)
}
