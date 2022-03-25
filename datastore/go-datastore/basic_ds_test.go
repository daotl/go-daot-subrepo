// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package datastore_test

import (
	"io/ioutil"
	"log"
	"testing"

	dstore "github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	dstest "github.com/daotl/go-datastore/test"
)

func testMapDatastore(t *testing.T, ktype key.KeyType) {
	ds := dstest.NewMapDatastoreForTest(t, ktype)
	dstest.SubtestAll(t, ktype, ds)
}

func TestMapDatastore(t *testing.T) {
	testMapDatastore(t, key.KeyTypeString)
	testMapDatastore(t, key.KeyTypeBytes)
}

func testNullDatastore(t *testing.T, ktype key.KeyType) {
	ds := dstore.NewNullDatastore()
	// The only test that passes. Nothing should be found.
	dstest.SubtestNotFounds(t, ktype, ds)
}

func TestNullDatastore(t *testing.T) {
	testNullDatastore(t, key.KeyTypeString)
	testNullDatastore(t, key.KeyTypeBytes)
}

func testLogDatastore(t *testing.T, ktype key.KeyType) {
	defer log.SetOutput(log.Writer())
	log.SetOutput(ioutil.Discard)
	ds := dstore.NewLogDatastore(dstest.NewMapDatastoreForTest(t, ktype), "")
	dstest.SubtestAll(t, ktype, ds)
}

func TestLogDatastore(t *testing.T) {
	testLogDatastore(t, key.KeyTypeString)
	testLogDatastore(t, key.KeyTypeBytes)
}
