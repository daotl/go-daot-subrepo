// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package flatfs_test

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/mount"
	"github.com/daotl/go-datastore/query"
	dstest "github.com/daotl/go-datastore/test"

	"github.com/daotl/go-ds-flatfs"
)

var ctxBg = context.Background()

func checkTemp(t *testing.T, dir string) {
	tempDir, err := os.Open(filepath.Join(dir, ".temp"))
	if err != nil {
		t.Errorf("failed to open temp dir: %s", err)
		return
	}

	names, err := tempDir.Readdirnames(-1)
	tempDir.Close()

	if err != nil {
		t.Errorf("failed to read temp dir: %s", err)
		return
	}

	for _, name := range names {
		t.Errorf("found leftover temporary file: %s", name)
	}
}

func tempdir(t testing.TB) (path string, cleanup func()) {
	path, err := ioutil.TempDir("", "test-datastore-flatfs-")
	if err != nil {
		t.Fatalf("cannot create temp directory: %v", err)
	}

	cleanup = func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("tempdir cleanup failed: %v", err)
		}
	}
	return path, cleanup
}

func tryAllShardFuncs(t *testing.T, ktype key.KeyType,
	testFunc func(key.KeyType, mkShardFunc, *testing.T)) {
	t.Run("prefix", func(t *testing.T) { testFunc(ktype, flatfs.Prefix, t) })
	t.Run("suffix", func(t *testing.T) { testFunc(ktype, flatfs.Suffix, t) })
	t.Run("next-to-last", func(t *testing.T) { testFunc(ktype, flatfs.NextToLast, t) })
}

type mkShardFunc func(int) *flatfs.ShardIdV1

func testBatch(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	batches := make([]datastore.Batch, 9)
	for i := range batches {
		batch, err := fs.Batch(ctxBg)
		if err != nil {
			t.Fatal(err)
		}

		batches[i] = batch

		err = batch.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"), []byte("foo"))
		if err != nil {
			t.Fatal(err)
		}
		err = batch.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, fmt.Sprintf("Q%dX", i)),
			[]byte(fmt.Sprintf("bar%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(batches))
	for _, batch := range batches {
		batch := batch
		go func() {
			defer wg.Done()
			err := batch.Commit(ctxBg)
			if err != nil {
				t.Error(err)
			}
		}()
	}

	check := func(k, expected string) {
		actual, err := fs.Get(ctxBg, key.NewKeyFromTypeAndString(ktype, k))
		if err != nil {
			t.Fatalf("get for key %s, error: %s", k, err)
		}
		if string(actual) != expected {
			t.Fatalf("for key %s, expected %s, got %s", k, expected, string(actual))
		}
	}

	wg.Wait()

	check("QUUX", "foo")
	for i := range batches {
		check(fmt.Sprintf("Q%dX", i), fmt.Sprintf("bar%d", i))
	}
}

func TestBatch(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testBatch)
	tryAllShardFuncs(t, key.KeyTypeBytes, testBatch)
}

func testPut(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	err = fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	if ktype == key.KeyTypeString {
		err := fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "foo"), []byte("nonono"))
		if err == nil {
			t.Fatalf("did not expect to put a lowercase key")
		}
	}
}

func TestPut(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testPut)
	tryAllShardFuncs(t, key.KeyTypeBytes, testPut)
}

func testGet(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	const input = "foobar"
	err = fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"), []byte(input))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	buf, err := fs.Get(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if g, e := string(buf), input; g != e {
		t.Fatalf("Get gave wrong content: %q != %q", g, e)
	}

	_, err = fs.Get(ctxBg, key.NewKeyFromTypeAndString(ktype, "/FOO/BAR"))
	if err != datastore.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %s", err)
	}
}

func TestGet(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testGet)
	tryAllShardFuncs(t, key.KeyTypeBytes, testGet)
}

func testPutOverwrite(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	const (
		loser  = "foobar"
		winner = "xyzzy"
	)
	err = fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"), []byte(loser))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	err = fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"), []byte(winner))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	data, err := fs.Get(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if g, e := string(data), winner; g != e {
		t.Fatalf("Get gave wrong content: %q != %q", g, e)
	}
}

func TestPutOverwrite(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testPutOverwrite)
	tryAllShardFuncs(t, key.KeyTypeBytes, testPutOverwrite)
}

func testGetNotFoundError(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	_, err = fs.Get(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetNotFoundError(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testGetNotFoundError)
	tryAllShardFuncs(t, key.KeyTypeBytes, testGetNotFoundError)
}

type params struct {
	shard *flatfs.ShardIdV1
	dir   string
	key   string
}

func testStorage(p *params, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	target := p.dir + string(os.PathSeparator) + p.key + ".data"
	fs, err := flatfs.CreateOrOpen(temp, key.KeyTypeString, p.shard, false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	err = fs.Put(ctxBg, key.NewStrKey(p.key), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	fs.Close()
	seen := false
	haveREADME := false
	walk := func(absPath string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		path, err := filepath.Rel(temp, absPath)
		if err != nil {
			return err
		}
		switch path {
		case ".", "..", "SHARDING", flatfs.DiskUsageFile, ".temp":
			// ignore
		case "_README":
			_, err := ioutil.ReadFile(absPath)
			if err != nil {
				t.Error("could not read _README file")
			}
			haveREADME = true
		case p.dir:
			if !fi.IsDir() {
				t.Errorf("directory is not a file? %v", fi.Mode())
			}
			// we know it's there if we see the file, nothing more to
			// do here
		case target:
			seen = true
			if !fi.Mode().IsRegular() {
				t.Errorf("expected a regular file, mode: %04o", fi.Mode())
			}
			if runtime.GOOS != "windows" {
				if g, e := fi.Mode()&os.ModePerm&0007, os.FileMode(0000); g != e {
					t.Errorf("file should not be world accessible: %04o", fi.Mode())
				}
			}
		default:
			t.Errorf("saw unexpected directory entry: %q %v", path, fi.Mode())
		}
		return nil
	}
	if err := filepath.Walk(temp, walk); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if !seen {
		t.Error("did not see the data file")
	}
	if fs.ShardStr() == flatfs.IPFS_DEF_SHARD_STR && !haveREADME {
		t.Error("expected _README file")
	} else if fs.ShardStr() != flatfs.IPFS_DEF_SHARD_STR && haveREADME {
		t.Error("did not expect _README file")
	}
}

func TestStorage(t *testing.T) {
	t.Run("prefix", func(t *testing.T) {
		testStorage(&params{
			shard: flatfs.Prefix(2),
			dir:   "QU",
			key:   "QUUX",
		}, t)
	})
	t.Run("suffix", func(t *testing.T) {
		testStorage(&params{
			shard: flatfs.Suffix(2),
			dir:   "UX",
			key:   "QUUX",
		}, t)
	})
	t.Run("next-to-last", func(t *testing.T) {
		testStorage(&params{
			shard: flatfs.NextToLast(2),
			dir:   "UU",
			key:   "QUUX",
		}, t)
	})
}

func testHasNotFound(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	found, err := fs.Has(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"))
	if err != nil {
		t.Fatalf("Has fail: %v\n", err)
	}
	if found {
		t.Fatal("Has should have returned false")
	}
}

func TestHasNotFound(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testHasNotFound)
	tryAllShardFuncs(t, key.KeyTypeBytes, testHasNotFound)
}

func testHasFound(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	err = fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	found, err := fs.Has(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"))
	if err != nil {
		t.Fatalf("Has fail: %v\n", err)
	}
	if !found {
		t.Fatal("Has should have returned true")
	}
}

func TestHasFound(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testHasFound)
	tryAllShardFuncs(t, key.KeyTypeBytes, testHasFound)
}

func testGetSizeFound(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	_, err = fs.GetSize(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"))
	if err != datastore.ErrNotFound {
		t.Fatalf("GetSize should have returned ErrNotFound, got: %v\n", err)
	}
}

func TestGetSizeFound(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testGetSizeFound)
	tryAllShardFuncs(t, key.KeyTypeBytes, testGetSizeFound)
}

func testGetSizeNotFound(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	err = fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	size, err := fs.GetSize(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"))
	if err != nil {
		t.Fatalf("GetSize failed with: %v\n", err)
	}
	if size != len("foobar") {
		t.Fatalf("GetSize returned wrong size: got %d, expected %d", size, len("foobar"))
	}
}

func TestGetSizeNotFound(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testGetSizeNotFound)
	tryAllShardFuncs(t, key.KeyTypeBytes, testGetSizeNotFound)
}

func testDeleteNotFound(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	err = fs.Delete(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"))
	if err != nil {
		t.Fatalf("expected nil, got: %v\n", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testDeleteNotFound)
	tryAllShardFuncs(t, key.KeyTypeBytes, testDeleteNotFound)
}

func testDeleteFound(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	err = fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	err = fs.Delete(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"))
	if err != nil {
		t.Fatalf("Delete fail: %v\n", err)
	}

	// check that it's gone
	_, err = fs.Get(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected Get after Delete to give ErrNotFound, got: %v\n", g)
	}
}

func TestDeleteFound(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testDeleteFound)
	tryAllShardFuncs(t, key.KeyTypeBytes, testDeleteFound)
}

func testQuerySimple(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	myKey := key.NewKeyFromTypeAndString(ktype, "QUUX")
	err = fs.Put(ctxBg, myKey, []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	res, err := fs.Query(ctxBg, query.Query{KeysOnly: true})
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
}

func TestQuerySimple(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testQuerySimple)
	tryAllShardFuncs(t, key.KeyTypeBytes, testQuerySimple)
}

func testDiskUsage(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	time.Sleep(100 * time.Millisecond)
	duNew, err := fs.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("duNew:", duNew)

	count := 200
	for i := 0; i < count; i++ {
		k := key.NewKeyFromTypeAndString(ktype, fmt.Sprintf("TEST-%d", i))
		v := []byte("10bytes---")
		err = fs.Put(ctxBg, k, v)
		if err != nil {
			t.Fatalf("Put fail: %v\n", err)
		}
	}

	time.Sleep(100 * time.Millisecond)
	duElems, err := fs.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("duPostPut:", duElems)

	for i := 0; i < count; i++ {
		k := key.NewKeyFromTypeAndString(ktype, fmt.Sprintf("TEST-%d", i))
		err = fs.Delete(ctxBg, k)
		if err != nil {
			t.Fatalf("Delete fail: %v\n", err)
		}
	}

	time.Sleep(100 * time.Millisecond)
	duDelete, err := fs.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("duPostDelete:", duDelete)

	du, err := fs.DiskUsage()
	t.Log("duFinal:", du)
	if err != nil {
		t.Fatal(err)
	}
	fs.Close()

	// Check that disk usage file is correct
	duB, err := ioutil.ReadFile(filepath.Join(temp, flatfs.DiskUsageFile))
	if err != nil {
		t.Fatal(err)
	}
	contents := make(map[string]interface{})
	err = json.Unmarshal(duB, &contents)
	if err != nil {
		t.Fatal(err)
	}

	// Make sure diskUsage value is correct
	if val, ok := contents["diskUsage"].(float64); !ok || uint64(val) != du {
		t.Fatalf("Unexpected value for diskUsage in %s: %v (expected %d)",
			flatfs.DiskUsageFile, contents["diskUsage"], du)
	}

	// Make sure the accuracy value is correct
	if val, ok := contents["accuracy"].(string); !ok || val != "initial-exact" {
		t.Fatalf("Unexpected value for accuracyin %s: %v",
			flatfs.DiskUsageFile, contents["accuracy"])
	}

	// Make sure size is correctly calculated on re-open
	os.Remove(filepath.Join(temp, flatfs.DiskUsageFile))
	fs, err = flatfs.Open(temp, ktype, false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	duReopen, err := fs.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("duReopen:", duReopen)

	// Checks
	if duNew == 0 {
		t.Error("new datastores should have some size")
	}

	if duElems <= duNew {
		t.Error("size should grow as new elements are added")
	}

	if duElems-duDelete != uint64(count*10) {
		t.Error("size should be reduced exactly as size of objects deleted")
	}

	if duReopen < duNew {
		t.Error("Reopened datastore should not be smaller")
	}
}

func TestDiskUsage(t *testing.T) {

	tryAllShardFuncs(t, key.KeyTypeString, testDiskUsage)
	tryAllShardFuncs(t, key.KeyTypeBytes, testDiskUsage)

}

func TestDiskUsageDoubleCount(t *testing.T) {

	tryAllShardFuncs(t, key.KeyTypeString, testDiskUsageDoubleCount)
	tryAllShardFuncs(t, key.KeyTypeBytes, testDiskUsageDoubleCount)

}

// test that concurrently writing and deleting the same key/value
// does not throw any errors and disk usage does not do
// any double-counting.
func testDiskUsageDoubleCount(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	var count int
	var wg sync.WaitGroup
	testKey := key.NewKeyFromTypeAndString(ktype, "TEST")

	put := func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			v := []byte("10bytes---")
			err := fs.Put(ctxBg, testKey, v)
			if err != nil {
				t.Errorf("Put fail: %v\n", err)
			}
		}
	}

	del := func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			err := fs.Delete(ctxBg, testKey)
			if err != nil && !strings.Contains(err.Error(), "key not found") {
				t.Errorf("Delete fail: %v\n", err)
			}
		}
	}

	// Add one element and then remove it and check disk usage
	// makes sense
	count = 1
	wg.Add(2)
	put()
	du, _ := fs.DiskUsage()
	del()
	du2, _ := fs.DiskUsage()
	if du-10 != du2 {
		t.Error("should have deleted exactly 10 bytes:", du, du2)
	}

	// Add and remove many times at the same time
	count = 200
	wg.Add(4)
	go put()
	go del()
	go put()
	go del()
	wg.Wait()

	du3, _ := fs.DiskUsage()
	has, err := fs.Has(ctxBg, testKey)
	if err != nil {
		t.Fatal(err)
	}

	if has { // put came last
		if du3 != du {
			t.Error("du should be the same as after first put:", du, du3)
		}
	} else { //delete came last
		if du3 != du2 {
			t.Error("du should be the same as after first delete:", du2, du3)
		}
	}
}

func testDiskUsageBatch(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	fsBatch, err := fs.Batch(ctxBg)
	if err != nil {
		t.Fatal(err)
	}

	count := 200
	var wg sync.WaitGroup
	testKeys := []key.Key{}
	for i := 0; i < count; i++ {
		k := key.NewKeyFromTypeAndString(ktype, fmt.Sprintf("TEST%d", i))
		testKeys = append(testKeys, k)
	}

	put := func() {
		for i := 0; i < count; i++ {
			err := fsBatch.Put(ctxBg, testKeys[i], []byte("10bytes---"))
			if err != nil {
				t.Error(err)
			}
		}
	}
	commit := func() {
		defer wg.Done()
		err := fsBatch.Commit(ctxBg)
		if err != nil {
			t.Errorf("Batch Put fail: %v\n", err)
		}
	}

	del := func() {
		defer wg.Done()
		for _, k := range testKeys {
			err := fs.Delete(ctxBg, k)
			if err != nil && !strings.Contains(err.Error(), "key not found") {
				t.Errorf("Delete fail: %v\n", err)
			}
		}
	}

	// Put many elements and then delete them and check disk usage
	// makes sense
	wg.Add(2)
	put()
	commit()
	du, err := fs.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	del()
	du2, err := fs.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	if du-uint64(10*count) != du2 {
		t.Errorf("should have deleted exactly %d bytes: %d %d", 10*count, du, du2)
	}

	// Do deletes while doing putManys concurrently
	wg.Add(2)
	put()
	go commit()
	go del()
	wg.Wait()

	du3, err := fs.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	// Now query how many keys we have
	results, err := fs.Query(ctxBg, query.Query{
		KeysOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	rest, err := results.Rest()
	if err != nil {
		t.Fatal(err)
	}

	expectedSize := uint64(len(rest) * 10)

	if exp := du2 + expectedSize; exp != du3 {
		t.Error("diskUsage has skewed off from real size:",
			exp, du3)
	}
}

func TestDiskUsageBatch(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testDiskUsageBatch)
	tryAllShardFuncs(t, key.KeyTypeBytes, testDiskUsageBatch)
}

func testDiskUsageEstimation(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	count := 50000
	for i := 0; i < count; i++ {
		k := key.NewKeyFromTypeAndString(ktype, fmt.Sprintf("%d-TEST-%d", i, i))
		v := make([]byte, 1000)
		err = fs.Put(ctxBg, k, v)
		if err != nil {
			t.Fatalf("Put fail: %v\n", err)
		}
	}

	// Delete checkpoint
	fs.Close()
	os.Remove(filepath.Join(temp, flatfs.DiskUsageFile))

	// This will do a full du
	flatfs.DiskUsageFilesAverage = -1
	fs, err = flatfs.Open(temp, ktype, false)
	if err != nil {
		t.Fatalf("Open fail: %v\n", err)
	}

	duReopen, err := fs.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}

	fs.Close()
	os.Remove(filepath.Join(temp, flatfs.DiskUsageFile))

	// This will estimate the size. Since all files are the same
	// length we can use a low file average number.
	flatfs.DiskUsageFilesAverage = 100
	// Make sure size is correctly calculated on re-open
	fs, err = flatfs.Open(temp, ktype, false)
	if err != nil {
		t.Fatalf("Open fail: %v\n", err)
	}

	duEst, err := fs.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("RealDu:", duReopen)
	t.Log("Est:", duEst)

	diff := int(math.Abs(float64(int(duReopen) - int(duEst))))
	maxDiff := int(0.05 * float64(duReopen)) // %5 of actual

	if diff > maxDiff {
		t.Fatalf("expected a better estimation within 5%%")
	}

	// Make sure the accuracy value is correct
	if fs.Accuracy() != "initial-approximate" {
		t.Errorf("Unexpected value for fs.Accuracy(): %s", fs.Accuracy())
	}

	fs.Close()

	// Reopen into a new variable
	fs2, err := flatfs.Open(temp, ktype, false)
	if err != nil {
		t.Fatalf("Open fail: %v\n", err)
	}

	// Make sure the accuracy value is preserved
	if fs2.Accuracy() != "initial-approximate" {
		t.Errorf("Unexpected value for fs.Accuracy(): %s", fs2.Accuracy())
	}
}

func TestDiskUsageEstimation(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testDiskUsageEstimation)
	tryAllShardFuncs(t, key.KeyTypeString, testDiskUsageEstimation)
}

func testBatchPut(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	dstest.RunBatchTest(t, ktype, fs)
}

func TestBatchPut(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testBatchPut)
	tryAllShardFuncs(t, key.KeyTypeBytes, testBatchPut)
}

func testBatchDelete(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	dstest.RunBatchDeleteTest(t, ktype, fs)
}

func TestBatchDelete(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testBatchDelete)
	tryAllShardFuncs(t, key.KeyTypeBytes, testBatchDelete)
}

func testClose(ktype key.KeyType, dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	err = fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "QUUX"), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	fs.Close()

	err = fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, "QAAX"), []byte("foobar"))
	if err == nil {
		t.Fatal("expected put on closed datastore to fail")
	}
}

func TestClose(t *testing.T) {
	tryAllShardFuncs(t, key.KeyTypeString, testClose)
	tryAllShardFuncs(t, key.KeyTypeBytes, testClose)
}

func testSHARDINGFile(t *testing.T, ktype key.KeyType) {
	tempdir, cleanup := tempdir(t)
	defer cleanup()

	fun := flatfs.IPFS_DEF_SHARD

	err := flatfs.Create(tempdir, fun)
	if err != nil {
		t.Fatalf("Create: %v\n", err)
	}

	fs, err := flatfs.Open(tempdir, key.KeyTypeString, false)
	if err != nil {
		t.Fatalf("Open fail: %v\n", err)
	}
	if fs.ShardStr() != flatfs.IPFS_DEF_SHARD_STR {
		t.Fatalf("Expected '%s' for shard function got '%s'", flatfs.IPFS_DEF_SHARD_STR, fs.ShardStr())
	}
	fs.Close()

	fs, err = flatfs.CreateOrOpen(tempdir, key.KeyTypeString, fun, false)
	if err != nil {
		t.Fatalf("Could not reopen repo: %v\n", err)
	}
	fs.Close()

	fs, err = flatfs.CreateOrOpen(tempdir, key.KeyTypeString,
		flatfs.Prefix(5), false)
	if err == nil {
		fs.Close()
		t.Fatalf("Was able to open repo with incompatible sharding function")
	}
}

func TestSHARDINGFile(t *testing.T) {
	testSHARDINGFile(t, key.KeyTypeString)
	testSHARDINGFile(t, key.KeyTypeBytes)
}

func TestInvalidPrefix(t *testing.T) {
	_, err := flatfs.ParseShardFunc("/bad/prefix/v1/next-to-last/2")
	if err == nil {
		t.Fatalf("Expected an error while parsing a shard identifier with a bad prefix")
	}
}

func TestNonDatastoreDir(t *testing.T) {
	tempdir, cleanup := tempdir(t)
	defer cleanup()

	err := ioutil.WriteFile(filepath.Join(tempdir, "afile"), []byte("Some Content"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	err = flatfs.Create(tempdir, flatfs.NextToLast(2))
	if err == nil {
		t.Fatalf("Expected an error when creating a datastore in a non-empty directory")
	}
}

func testNoCluster(t *testing.T, ktype key.KeyType) {

	tempdir, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, tempdir)

	fs, err := flatfs.CreateOrOpen(tempdir, ktype, flatfs.NextToLast(1), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	r := rand.New(rand.NewSource(0))
	N := 3200 // should be divisible by 32 so the math works out
	for i := 0; i < N; i++ {
		blk := make([]byte, 1000)
		r.Read(blk)

		var err error
		switch ktype {
		case key.KeyTypeString:
			k := "CIQ" + base32.StdEncoding.EncodeToString(blk[:10])
			err = fs.Put(ctxBg, key.NewStrKey(k), blk)
		case key.KeyTypeBytes:
			err = fs.Put(ctxBg, key.NewBytesKey(blk[:10]), blk)
		}

		if err != nil {
			t.Fatalf("Put fail: %v\n", err)
		}
	}

	fs.Close()
	dirs, err := ioutil.ReadDir(tempdir)
	if err != nil {
		t.Fatalf("ReadDir fail: %v\n", err)
	}
	idealFilesPerDir := float64(N) / 32.0
	tolerance := math.Floor(idealFilesPerDir * 0.25)
	count := 0
	for _, dir := range dirs {
		switch dir.Name() {
		case flatfs.SHARDING_FN, flatfs.README_FN, flatfs.DiskUsageFile, ".temp":
			continue
		}
		count += 1
		files, err := ioutil.ReadDir(filepath.Join(tempdir, dir.Name()))
		if err != nil {
			t.Fatalf("ReadDir fail: %v\n", err)
		}
		num := float64(len(files))
		if math.Abs(num-idealFilesPerDir) > tolerance {
			t.Fatalf("Dir %s has %.0f files, expected between %.f and %.f files",
				filepath.Join(tempdir, dir.Name()), num, idealFilesPerDir-tolerance,
				idealFilesPerDir+tolerance)
		}
	}
	if count != 32 {
		t.Fatalf("Expected 32 directories and one file in %s", tempdir)
	}
}

func TestNoCluster(t *testing.T) {
	testNoCluster(t, key.KeyTypeString)
	testNoCluster(t, key.KeyTypeBytes)
}

func BenchmarkConsecutivePut(b *testing.B) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var blocks [][]byte
	var keys []key.Key
	for i := 0; i < b.N; i++ {
		blk := make([]byte, 256*1024)
		r.Read(blk)
		blocks = append(blocks, blk)

		k := base32.StdEncoding.EncodeToString(blk[:8])
		keys = append(keys, key.NewStrKey(k))
	}
	temp, cleanup := tempdir(b)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, key.KeyTypeString, flatfs.Prefix(2), false)
	if err != nil {
		b.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := fs.Put(ctxBg, keys[i], blocks[i])
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer() // avoid counting cleanup
}

func BenchmarkBatchedPut(b *testing.B) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var blocks [][]byte
	var keys []key.Key
	for i := 0; i < b.N; i++ {
		blk := make([]byte, 256*1024)
		r.Read(blk)
		blocks = append(blocks, blk)

		k := base32.StdEncoding.EncodeToString(blk[:8])
		keys = append(keys, key.NewStrKey(k))
	}
	temp, cleanup := tempdir(b)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, key.KeyTypeString, flatfs.Prefix(2), false)
	if err != nil {
		b.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	b.ResetTimer()

	for i := 0; i < b.N; {
		batch, err := fs.Batch(ctxBg)
		if err != nil {
			b.Fatal(err)
		}

		for n := i; i-n < 512 && i < b.N; i++ {
			err := batch.Put(ctxBg, keys[i], blocks[i])
			if err != nil {
				b.Fatal(err)
			}
		}
		err = batch.Commit(ctxBg)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer() // avoid counting cleanup
}

func testQueryLeak(t *testing.T, ktype key.KeyType) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, ktype, flatfs.Prefix(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	defer fs.Close()

	for i := 0; i < 1000; i++ {
		err = fs.Put(ctxBg, key.NewKeyFromTypeAndString(ktype, fmt.Sprint(i)), []byte("foobar"))
		if err != nil {
			t.Fatalf("Put fail: %v\n", err)
		}
	}

	before := runtime.NumGoroutine()
	for i := 0; i < 200; i++ {
		res, err := fs.Query(ctxBg, query.Query{KeysOnly: true})
		if err != nil {
			t.Errorf("Query fail: %v\n", err)
		}
		res.Close()
	}
	after := runtime.NumGoroutine()
	if after-before > 100 {
		t.Errorf("leaked %d goroutines", after-before)
	}
}

func TestQueryLeak(t *testing.T) {
	testQueryLeak(t, key.KeyTypeString)
	testQueryLeak(t, key.KeyTypeBytes)
}

func testSuite(t *testing.T, ktype key.KeyType) {
	temp, cleanup := tempdir(t)
	defer cleanup()
	defer checkTemp(t, temp)

	fs, err := flatfs.CreateOrOpen(temp, ktype, flatfs.Prefix(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	ds := mount.New([]mount.Mount{{
		Prefix:    key.EmptyKeyFromType(ktype),
		Datastore: dstest.NewMapDatastoreForTest(t, ktype),
	}, {
		Prefix:    key.NewKeyFromTypeAndString(ktype, "capital"),
		Datastore: fs,
	}})
	defer func() {
		err := ds.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	dstest.SubtestAll(t, ktype, ds)
}

func TestSuite(t *testing.T) {
	testSuite(t, key.KeyTypeString)
	testSuite(t, key.KeyTypeBytes)
}
