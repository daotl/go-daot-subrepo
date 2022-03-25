// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package flatfs_test

import (
	"bytes"
	"context"
	"encoding/base32"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/daotl/go-datastore/key"

	flatfs "github.com/daotl/go-ds-flatfs"
)

func testMove(t *testing.T, ktype key.KeyType) {
	tempdir, cleanup := tempdir(t)
	defer cleanup()

	v1dir := filepath.Join(tempdir, "v1")
	createDatastore(t, v1dir, flatfs.Prefix(3))

	err := ioutil.WriteFile(filepath.Join(v1dir, "README_ALSO"), []byte("something"), 0666)
	if err != nil {
		t.Fatalf("WriteFile fail: %v\n", err)
	}

	keys, blocks := populateDatastore(t, ktype, v1dir)

	v2dir := filepath.Join(tempdir, "v2")
	createDatastore(t, v2dir, flatfs.NextToLast(2))

	err = flatfs.Move(v1dir, v2dir, nil)
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	// make sure the directory empty
	rmEmptyDatastore(t, v1dir)

	// make sure the README file moved
	_, err = os.Stat(filepath.Join(v2dir, "README_ALSO"))
	if err != nil {
		t.Fatalf(err.Error())
	}

	// check that all keys are available
	checkKeys(t, ktype, v2dir, keys, blocks)

	// check that a key is in the correct format
	shard := filepath.Join(v2dir, flatfs.NextToLast(2).Func()(keys[0].String()))
	_, err = os.Stat(shard)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestMove(t *testing.T) {
	testMove(t, key.KeyTypeString)
	testMove(t, key.KeyTypeBytes)
}

func testMoveRestart(t *testing.T, ktype key.KeyType) {
	tempdir, cleanup := tempdir(t)
	defer cleanup()

	v1dir := filepath.Join(tempdir, "v1")
	v2dir := filepath.Join(tempdir, "v2")

	createDatastore(t, v1dir, flatfs.Prefix(3))

	createDatastore(t, v2dir, flatfs.NextToLast(5))

	keys, blocks := populateDatastore(t, ktype, v1dir)
	checkKeys(t, ktype, v1dir, keys, blocks)

	// get a directory in the datastore
	noslash := keys[0].String()[1:]
	aDir := filepath.Join(tempdir, "v1", flatfs.Prefix(3).Func()(noslash))

	// create a permission problem on the directory
	err := os.Chmod(aDir, 0500)
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	// try the move it should fail partly through
	err = flatfs.Move(v1dir, v2dir, nil)
	if err == nil {
		t.Fatal("Move should have failed.", err)
	}

	// okay try to undo should be okay
	err = flatfs.Move(v2dir, v1dir, nil)
	if err != nil {
		t.Fatal("Could not undo the move.", err)
	}
	checkKeys(t, ktype, v1dir, keys, blocks)

	// there should be nothing left in the new datastore
	rmEmptyDatastore(t, v2dir)

	// try the move again, again should fail
	createDatastore(t, v2dir, flatfs.NextToLast(2))
	err = flatfs.Move(v1dir, v2dir, nil)
	if err == nil {
		t.Fatal("Move should have failed.", err)
	}

	// fix the permission problem
	err = os.Chmod(aDir, 0700)
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	// restart the move, it should be okay now
	err = flatfs.Move(v1dir, v2dir, nil)
	if err != nil {
		t.Fatalf("Move not okay: %v\n", err)
	}

	// make sure everything moved by removing the old directory
	rmEmptyDatastore(t, v1dir)

	// make sure everything moved by checking all keys
	checkKeys(t, ktype, v2dir, keys, blocks)

	// check that a key is in the correct format
	shard := filepath.Join(v2dir, flatfs.NextToLast(2).Func()(keys[0].String()))
	_, err = os.Stat(shard)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestMoveRestart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	testMoveRestart(t, key.KeyTypeString)
	testMoveRestart(t, key.KeyTypeBytes)
}

func testUpgradeDownload(t *testing.T, ktype key.KeyType) {
	tempdir, cleanup := tempdir(t)
	defer cleanup()

	createDatastore(t, tempdir, flatfs.Prefix(3))

	keys, blocks := populateDatastore(t, ktype, tempdir)
	checkKeys(t, ktype, tempdir, keys, blocks)

	err := flatfs.UpgradeV0toV1(tempdir, 3)
	if err == nil {
		t.Fatalf("UpgradeV0toV1 on already v1 should fail.")
	}

	err = flatfs.DowngradeV1toV0(tempdir)
	if err != nil {
		t.Fatalf("DowngradeV1toV0 fail: %v\n", err)
	}

	_, err = os.Stat(filepath.Join(tempdir, flatfs.SHARDING_FN))
	if err == nil {
		t.Fatalf("%v not in v0 format, SHARDING FILE exists", tempdir)
	} else if !os.IsNotExist(err) {
		t.Fatalf("Stat fail: %v\n", err)
	}

	err = flatfs.UpgradeV0toV1(tempdir, 3)
	if err != nil {
		t.Fatalf("UpgradeV0toV1 fail %v\n", err)
	}

	// This will fail unless the repository is in the new version
	checkKeys(t, ktype, tempdir, keys, blocks)
}

func TestUpgradeDownload(t *testing.T) {
	testUpgradeDownload(t, key.KeyTypeString)
	testUpgradeDownload(t, key.KeyTypeBytes)
}

func TestDownloadNonPrefix(t *testing.T) {
	tempdir, cleanup := tempdir(t)
	defer cleanup()

	createDatastore(t, tempdir, flatfs.NextToLast(2))

	err := flatfs.DowngradeV1toV0(tempdir)
	if err == nil {
		t.Fatal("DowngradeV1toV0 should have failed", err)
	}
}

func createDatastore(t *testing.T, dir string, fun *flatfs.ShardIdV1) {
	err := flatfs.Create(dir, fun)
	if err != nil {
		t.Fatalf("Create fail: %s: %v\n", dir, err)
	}
}

func rmEmptyDatastore(t *testing.T, dir string) {
	err := os.Remove(dir)
	if err != nil {
		t.Fatalf("Remove fail: %v\n", err)
	}
}

func populateDatastore(t *testing.T, ktype key.KeyType, dir string) ([]key.Key, [][]byte) {
	ds, err := flatfs.Open(dir, ktype, false)
	if err != nil {
		t.Fatalf("Open fail: %v\n", err)
	}
	defer ds.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var blocks [][]byte
	var keys []key.Key
	for i := 0; i < 256; i++ {
		blk := make([]byte, 1000)
		r.Read(blk)
		blocks = append(blocks, blk)

		k := "X" + base32.StdEncoding.EncodeToString(blk[:8])
		keys = append(keys, key.NewKeyFromTypeAndString(ktype, k))
		err := ds.Put(context.Background(), keys[i], blocks[i])
		if err != nil {
			t.Fatalf("Put fail: %v\n", err)
		}
	}

	return keys, blocks
}

func checkKeys(t *testing.T, ktype key.KeyType, dir string, keys []key.Key, blocks [][]byte) {
	ds, err := flatfs.Open(dir, ktype, false)
	if err != nil {
		t.Fatalf("Open fail: %v\n", err)
	}
	defer ds.Close()

	for i, key := range keys {
		data, err := ds.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Get fail: %v\n", err)
		}
		if !bytes.Equal(data, blocks[i]) {
			t.Fatalf("block context differ for key %s\n", key.String())
		}
	}
}
