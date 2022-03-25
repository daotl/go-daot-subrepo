// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package query

import (
	"testing"

	key "github.com/daotl/go-datastore/key"
)

func testKeyFilter(t *testing.T, f Filter, keys []key.Key, expect key.KeySlice) {
	t.Helper()
	e := make([]Entry, len(keys))
	for i, k := range keys {
		e[i] = Entry{Key: k}
	}

	res := ResultsWithEntries(Query{}, e)
	res = NaiveFilter(res, f)
	actualE, err := res.Rest()
	if err != nil {
		t.Fatal(err)
	}
	actual := key.KeySlice(make([]key.Key, len(actualE)))
	for i, e := range actualE {
		actual[i] = e.Key
	}

	if len(actual) != len(expect) {
		t.Error("expect != actual.", expect, actual)
	}

	if !actual.Join().Equal(expect.Join()) {
		t.Error("expect != actual.", expect, actual)
	}
}

func TestFilterKeyCompare(t *testing.T) {

	// StrKey
	testKeyFilter(t, FilterKeyCompare{Equal, key.QueryStrKey("/ab")}, sampleStrKeys, key.StrsToStrKeys([]string{"/ab"}))
	testKeyFilter(t, FilterKeyCompare{GreaterThan, key.QueryStrKey("/ab")}, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
		"/abce",
		"/abcf",
	}))
	testKeyFilter(t, FilterKeyCompare{LessThanOrEqual, key.QueryStrKey("/ab")}, sampleStrKeys, key.StrsToStrKeys([]string{
		"/a",
		"/ab",
	}))

	// BytesKey
	testKeyFilter(t, FilterKeyCompare{Equal, key.NewBytesKeyFromString("ab")}, sampleBytesKeys, key.StrsToBytesKeys([]string{"ab"}))
	testKeyFilter(t, FilterKeyCompare{GreaterThan, key.NewBytesKeyFromString("ab")}, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abc",
		"abcd",
		"abef",
		"abfg",
		"abce",
		"abcf",
	}))
	testKeyFilter(t, FilterKeyCompare{LessThanOrEqual, key.NewBytesKeyFromString("ab")}, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"a",
		"ab",
	}))
}

func TestFilterKeyPrefix(t *testing.T) {

	// StrKey
	testKeyFilter(t, FilterKeyPrefix{key.QueryStrKey("/a")}, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
		"/abce",
		"/abcf",
		"/ab",
	}))
	testKeyFilter(t, FilterKeyPrefix{key.QueryStrKey("/ab/")}, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
	}))

	// BytesKey
	testKeyFilter(t, FilterKeyPrefix{key.NewBytesKeyFromString("a")}, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abc",
		"abcd",
		"abef",
		"abfg",
		"abce",
		"abcf",
		"ab",
	}))
	testKeyFilter(t, FilterKeyPrefix{key.NewBytesKeyFromString("ab")}, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abc",
		"abcd",
		"abef",
		"abfg",
		"abce",
		"abcf",
	}))
}

func TestFilterKeyRange(t *testing.T) {

	// StrKey
	testKeyFilter(t, FilterKeyRange{Range{key.QueryStrKey("/ab/c"), nil}}, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
		"/abce",
		"/abcf",
	}))
	testKeyFilter(t, FilterKeyRange{Range{nil, key.QueryStrKey("/ab/fg")}}, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/a",
		"/ab",
	}))
	testKeyFilter(t, FilterKeyRange{Range{key.QueryStrKey("/ab/c"), key.QueryStrKey("/ab/fg")}}, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
	}))

	// BytesKey
	testKeyFilter(t, FilterKeyRange{Range{key.NewBytesKeyFromString("abc"), nil}}, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abc",
		"abcd",
		"abef",
		"abfg",
		"abce",
		"abcf",
	}))
	testKeyFilter(t, FilterKeyRange{Range{nil, key.NewBytesKeyFromString("abce")}}, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abc",
		"abcd",
		"a",
		"ab",
	}))
	testKeyFilter(t, FilterKeyRange{Range{key.NewBytesKeyFromString("abc"), key.NewBytesKeyFromString("abce")}}, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abc",
		"abcd",
	}))
}
