// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package query

import (
	"reflect"
	"testing"

	key "github.com/daotl/go-datastore/key"
)

var sampleStrKeys = key.StrsToStrKeys([]string{
	"/ab/c",
	"/ab/cd",
	"/ab/ef",
	"/ab/fg",
	"/a",
	"/abce",
	"/abcf",
	"/ab",
})

var sampleBytesKeys = key.StrsToBytesKeys([]string{
	"abc",
	"abcd",
	"abef",
	"abfg",
	"a",
	"abce",
	"abcf",
	"ab",
})

func testResults(t *testing.T, res Results, expect key.KeySlice) {
	t.Helper()

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

func TestNaiveQueryApply(t *testing.T) {
	testNaiveQueryApply := func(t *testing.T, query Query, keys []key.Key, expect []key.Key) {
		t.Helper()
		e := make([]Entry, len(keys))
		for i, k := range keys {
			e[i] = Entry{Key: k}
		}

		res := ResultsWithEntries(query, e)
		res = NaiveQueryApply(query, res)

		testResults(t, res, expect)
	}

	q := Query{Limit: 2}
	testNaiveQueryApply(t, q, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/c",
		"/ab/cd",
	}))
	testNaiveQueryApply(t, q, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abc",
		"abcd",
	}))

	q = Query{Offset: 3, Limit: 2}
	testNaiveQueryApply(t, q, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/fg",
		"/a",
	}))
	testNaiveQueryApply(t, q, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abfg",
		"a",
	}))

	f := &FilterKeyCompare{Op: Equal, Key: key.QueryStrKey("/ab")}
	q = Query{Filters: []Filter{f}}
	testNaiveQueryApply(t, q, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab",
	}))
	f = &FilterKeyCompare{Op: Equal, Key: key.NewBytesKeyFromString("ab")}
	q = Query{Filters: []Filter{f}}
	testNaiveQueryApply(t, q, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"ab",
	}))

	q = Query{Prefix: key.QueryStrKey("/ab")}
	testNaiveQueryApply(t, q, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
	}))
	q = Query{Prefix: key.NewBytesKeyFromString("ab")}
	testNaiveQueryApply(t, q, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abc",
		"abcd",
		"abef",
		"abfg",
		"abce",
		"abcf",
	}))

	q = Query{Orders: []Order{OrderByKeyDescending{}}}
	testNaiveQueryApply(t, q, sampleStrKeys, key.StrsToStrKeys([]string{
		"/abcf",
		"/abce",
		"/ab/fg",
		"/ab/ef",
		"/ab/cd",
		"/ab/c",
		"/ab",
		"/a",
	}))
	testNaiveQueryApply(t, q, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abfg",
		"abef",
		"abcf",
		"abce",
		"abcd",
		"abc",
		"ab",
		"a",
	}))

	q = Query{
		Limit:  2,
		Offset: 1,
		Prefix: key.QueryStrKey("/ab"),
		Orders: []Order{OrderByKey{}},
	}
	testNaiveQueryApply(t, q, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/cd",
		"/ab/ef",
	}))
	q = Query{
		Limit:  2,
		Offset: 1,
		Prefix: key.NewBytesKeyFromString("ab"),
		Orders: []Order{OrderByKey{}},
	}
	testNaiveQueryApply(t, q, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abcd",
		"abce",
	}))
}

func TestLimit(t *testing.T) {
	testKeyLimit := func(t *testing.T, limit int, keys []key.Key, expect []key.Key) {
		t.Helper()
		e := make([]Entry, len(keys))
		for i, k := range keys {
			e[i] = Entry{Key: k}
		}

		res := ResultsWithEntries(Query{}, e)
		res = NaiveLimit(res, limit)
		testResults(t, res, expect)
	}

	testKeyLimit(t, 0, sampleStrKeys, key.StrsToStrKeys([]string{ // none
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
		"/a",
		"/abce",
		"/abcf",
		"/ab",
	}))
	testKeyLimit(t, 0, sampleBytesKeys, key.StrsToBytesKeys([]string{ // none
		"abc",
		"abcd",
		"abef",
		"abfg",
		"a",
		"abce",
		"abcf",
		"ab",
	}))

	testKeyLimit(t, 10, sampleStrKeys, key.StrsToStrKeys([]string{ // large
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
		"/a",
		"/abce",
		"/abcf",
		"/ab",
	}))
	testKeyLimit(t, 10, sampleBytesKeys, key.StrsToBytesKeys([]string{ // large
		"abc",
		"abcd",
		"abef",
		"abfg",
		"a",
		"abce",
		"abcf",
		"ab",
	}))

	testKeyLimit(t, 2, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/c",
		"/ab/cd",
	}))
	testKeyLimit(t, 2, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abc",
		"abcd",
	}))
}

func TestOffset(t *testing.T) {

	testOffset := func(t *testing.T, offset int, keys []key.Key, expect []key.Key) {
		t.Helper()
		e := make([]Entry, len(keys))
		for i, k := range keys {
			e[i] = Entry{Key: k}
		}

		res := ResultsWithEntries(Query{}, e)
		res = NaiveOffset(res, offset)
		testResults(t, res, expect)
	}

	testOffset(t, 0, sampleStrKeys, key.StrsToStrKeys([]string{ // none
		"/ab/c",
		"/ab/cd",
		"/ab/ef",
		"/ab/fg",
		"/a",
		"/abce",
		"/abcf",
		"/ab",
	}))
	testOffset(t, 0, sampleBytesKeys, key.StrsToBytesKeys([]string{ // none
		"abc",
		"abcd",
		"abef",
		"abfg",
		"a",
		"abce",
		"abcf",
		"ab",
	}))

	testOffset(t, 10, sampleStrKeys, key.StrsToStrKeys([]string{ // large
	}))
	testOffset(t, 10, sampleBytesKeys, key.StrsToBytesKeys([]string{ // large
	}))

	testOffset(t, 2, sampleStrKeys, key.StrsToStrKeys([]string{
		"/ab/ef",
		"/ab/fg",
		"/a",
		"/abce",
		"/abcf",
		"/ab",
	}))
	testOffset(t, 2, sampleBytesKeys, key.StrsToBytesKeys([]string{
		"abef",
		"abfg",
		"a",
		"abce",
		"abcf",
		"ab",
	}))
}

func TestResultsFromIterator(t *testing.T) {
	testResultsFromIteratorWClose(t, getKeysViaNextSync)
}

func TestResultsFromIteratorUsingChan(t *testing.T) {
	testResultsFromIteratorWClose(t, getKeysViaChan)
}

func TestResultsFromIteratorUsingRest(t *testing.T) {
	testResultsFromIteratorWClose(t, getKeysViaRest)
}

func TestResultsFromIteratorNoClose(t *testing.T) {
	testResultsFromIterator(t, getKeysViaNextSync, nil)
	testResultsFromIterator(t, getKeysViaChan, nil)
}

func testResultsFromIterator(t *testing.T, getKeys func(rs Results) []key.Key, close func() error) {
	i := 0
	results := ResultsFromIterator(Query{}, Iterator{
		Next: func() (Result, bool) {
			if i >= len(sampleStrKeys) {
				return Result{}, false
			}
			res := Result{Entry: Entry{Key: sampleStrKeys[i]}}
			i++
			return res, true
		},
		Close: close,
	})
	keys := getKeys(results)
	if !reflect.DeepEqual(sampleStrKeys, keys) {
		t.Errorf("did not get the same set of keys")
	}
}

func testResultsFromIteratorWClose(t *testing.T, getKeys func(rs Results) []key.Key) {
	closeCalled := 0
	testResultsFromIterator(t, getKeys, func() error {
		closeCalled++
		return nil
	})
	if closeCalled != 1 {
		t.Errorf("close called %d times, expect it to be called just once", closeCalled)
	}
}

func getKeysViaNextSync(rs Results) []key.Key {
	ret := make([]key.Key, 0)
	for {
		r, ok := rs.NextSync()
		if !ok {
			break
		}
		ret = append(ret, r.Key)
	}
	return ret
}

func getKeysViaRest(rs Results) []key.Key {
	rest, _ := rs.Rest()
	ret := make([]key.Key, 0)
	for _, e := range rest {
		ret = append(ret, e.Key)
	}
	return ret
}

func getKeysViaChan(rs Results) []key.Key {
	ret := make([]key.Key, 0)
	for r := range rs.Next() {
		ret = append(ret, r.Key)
	}
	return ret
}

func TestStringer(t *testing.T) {
	q := Query{}

	expected := `SELECT keys,vals`
	actual := q.String()
	if actual != expected {
		t.Fatalf("expected\n\t%s\ngot\n\t%s", expected, actual)
	}

	q.Offset = 10
	q.Limit = 10
	expected = `SELECT keys,vals OFFSET 10 LIMIT 10`
	actual = q.String()
	if actual != expected {
		t.Fatalf("expected\n\t%s\ngot\n\t%s", expected, actual)
	}

	q.Orders = []Order{OrderByValue{}, OrderByKey{}}
	expected = `SELECT keys,vals ORDER [VALUE, KEY] OFFSET 10 LIMIT 10`
	actual = q.String()
	if actual != expected {
		t.Fatalf("expected\n\t%s\ngot\n\t%s", expected, actual)
	}

	q.Filters = []Filter{
		FilterKeyCompare{Op: GreaterThan, Key: key.QueryStrKey("/foo/bar")},
		FilterKeyCompare{Op: LessThan, Key: key.QueryStrKey("/foo/bar")},
	}
	expected = `SELECT keys,vals FILTER [KEY > "/foo/bar", KEY < "/foo/bar"] ORDER [VALUE, KEY] OFFSET 10 LIMIT 10`
	actual = q.String()
	if actual != expected {
		t.Fatalf("expected\n\t%s\ngot\n\t%s", expected, actual)
	}

	q.Prefix = key.QueryStrKey("/foo")
	expected = `SELECT keys,vals FROM "/foo" FILTER [KEY > "/foo/bar", KEY < "/foo/bar"] ORDER [VALUE, KEY] OFFSET 10 LIMIT 10`
	actual = q.String()
	if actual != expected {
		t.Fatalf("expected\n\t%s\ngot\n\t%s", expected, actual)
	}

	q.ReturnExpirations = true
	expected = `SELECT keys,vals,exps FROM "/foo" FILTER [KEY > "/foo/bar", KEY < "/foo/bar"] ORDER [VALUE, KEY] OFFSET 10 LIMIT 10`
	actual = q.String()
	if actual != expected {
		t.Fatalf("expected\n\t%s\ngot\n\t%s", expected, actual)
	}

	q.KeysOnly = true
	expected = `SELECT keys,exps FROM "/foo" FILTER [KEY > "/foo/bar", KEY < "/foo/bar"] ORDER [VALUE, KEY] OFFSET 10 LIMIT 10`
	actual = q.String()
	if actual != expected {
		t.Fatalf("expected\n\t%s\ngot\n\t%s", expected, actual)
	}
	q.ReturnExpirations = false
	expected = `SELECT keys FROM "/foo" FILTER [KEY > "/foo/bar", KEY < "/foo/bar"] ORDER [VALUE, KEY] OFFSET 10 LIMIT 10`
	actual = q.String()
	if actual != expected {
		t.Fatalf("expected\n\t%s\ngot\n\t%s", expected, actual)
	}
}
