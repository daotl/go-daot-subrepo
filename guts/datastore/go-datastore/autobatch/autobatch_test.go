// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package autobatch

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	ds "github.com/daotl/go-datastore"
	key "github.com/daotl/go-datastore/key"
	dstest "github.com/daotl/go-datastore/test"
)

func TestAutobatch(t *testing.T) {
	dstest.SubtestAll(t, key.KeyTypeString,
		NewAutoBatching(dstest.NewMapDatastoreForTest(t, key.KeyTypeString), 16))
}

func TestFlushing(t *testing.T) {
	ctx := context.Background()

	child := dstest.NewMapDatastoreForTest(t, key.KeyTypeString)
	d := NewAutoBatching(child, 16)

	var keys []key.Key
	for i := 0; i < 16; i++ {
		keys = append(keys, key.NewStrKey(fmt.Sprintf("test%d", i)))
	}
	v := []byte("hello world")

	for _, k := range keys {
		err := d.Put(ctx, k, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get works normally.
	for _, k := range keys {
		val, err := d.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(val, v) {
			t.Fatal("wrong value")
		}
	}

	// Not flushed
	_, err := child.Get(ctx, keys[0])
	if err != ds.ErrNotFound {
		t.Fatal("shouldnt have found value")
	}

	// Delete works.
	err = d.Delete(ctx, keys[14])
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.Get(ctx, keys[14])
	if err != ds.ErrNotFound {
		t.Fatal(err)
	}

	// Still not flushed
	_, err = child.Get(ctx, keys[0])
	if err != ds.ErrNotFound {
		t.Fatal("shouldnt have found value")
	}

	// Final put flushes.
	err = d.Put(ctx, key.NewStrKey("test16"), v)
	if err != nil {
		t.Fatal(err)
	}

	// should be flushed now, try to get keys from child datastore
	for _, k := range keys[:14] {
		val, err := child.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(val, v) {
			t.Fatal("wrong value")
		}
	}

	// Never flushed the deleted key.
	_, err = child.Get(ctx, keys[14])
	if err != ds.ErrNotFound {
		t.Fatal("shouldnt have found value")
	}

	// Delete doesn't flush
	err = d.Delete(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}

	val, err := child.Get(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(val, v) {
		t.Fatal("wrong value")
	}
}

func TestSync(t *testing.T) {
	ctx := context.Background()

	child := dstest.NewMapDatastoreForTest(t, key.KeyTypeString)
	d := NewAutoBatching(child, 100)

	put := func(key key.Key) {
		if err := d.Put(ctx, key, key.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	del := func(key key.Key) {
		if err := d.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
	}

	get := func(d ds.Datastore, key key.Key) {
		val, err := d.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(val, key.Bytes()) {
			t.Fatal("wrong value")
		}
	}
	invalidGet := func(d ds.Datastore, key key.Key) {
		if _, err := d.Get(ctx, key); err != ds.ErrNotFound {
			t.Fatal("should not have found value")
		}
	}

	// Test if Syncing Puts works
	internalSyncTest(t, d, child, put, del, get, invalidGet)

	// Test if Syncing Deletes works
	internalSyncTest(t, d, child, del, put, invalidGet, get)
}

// This function can be used to test Sync Puts and Deletes
// For clarity comments are written as if op = Put and undoOp = Delete
func internalSyncTest(t *testing.T, d, child ds.Datastore, op, undoOp func(key.Key),
	checkOp, checkUndoOp func(ds.Datastore, key.Key)) {

	ctx := context.Background()

	var keys []key.Key
	keymap := make(map[string]int)
	for i := 0; i < 4; i++ {
		k := key.NewStrKey(fmt.Sprintf("%d", i))
		keymap[k.String()] = len(keys)
		keys = append(keys, k)
		for j := 0; j < 2; j++ {
			k := key.NewStrKey(fmt.Sprintf("%d/%d", i, j))
			keymap[k.String()] = len(keys)
			keys = append(keys, k)
			for k := 0; k < 2; k++ {
				k := key.NewStrKey(fmt.Sprintf("%d/%d/%d", i, j, k))
				keymap[k.String()] = len(keys)
				keys = append(keys, k)
			}
		}
	}

	for _, k := range keys {
		op(k)
	}

	// Get works normally.
	for _, k := range keys {
		checkOp(d, k)
	}

	// Put not flushed
	checkUndoOp(child, key.NewStrKey("0"))

	// Delete works.
	deletedKey := key.NewStrKey("2/1/1")
	undoOp(deletedKey)
	checkUndoOp(d, deletedKey)

	// Put still not flushed
	checkUndoOp(child, key.NewStrKey("0"))

	// Sync the tree "0/*/*"
	if err := d.Sync(ctx, key.NewStrKey("0")); err != nil {
		t.Fatal(err)
	}

	// Try to get keys "0/*/*" from the child datastore
	checkKeyRange(t, keymap, keys, d, [][]string{{"0", "0/1/1"}}, checkOp)

	// Verify no other keys were synchronized
	checkKeyRange(t, keymap, keys, child, [][]string{{"1", "3/1/1"}}, checkUndoOp)

	// Sync the tree "1/1/*"
	if err := d.Sync(ctx, key.NewStrKey("1/1")); err != nil {
		t.Fatal(err)
	}

	// Try to get keys "0/*/*" and "1/1/*" from the child datastore
	checkKeyRange(t, keymap, keys, d, [][]string{{"0", "0/1/1"}, {"1/1", "1/1/1"}}, checkOp)

	// Verify no other keys were synchronized
	checkKeyRange(t, keymap, keys, child, [][]string{{"1", "1/0/1"}, {"2", "3/1/1"}}, checkUndoOp)

	// Sync the tree "3/1/1"
	if err := d.Sync(ctx, key.NewStrKey("3/1/1")); err != nil {
		t.Fatal(err)
	}

	// Try to get keys "0/*/*", "1/1/*", "3/1/1" from the child datastore
	checkKeyRange(t, keymap, keys, d, [][]string{{"0", "0/1/1"}, {"1/1", "1/1/1"}, {"3/1/1", "3/1/1"}}, checkOp)

	// Verify no other keys were synchronized
	checkKeyRange(t, keymap, keys, child, [][]string{{"1", "1/0/1"}, {"2", "3/1/0"}}, checkUndoOp)

	if err := d.Sync(ctx, key.EmptyStrKey); err != nil {
		t.Fatal(err)
	}

	// Never flushed the deleted key.
	checkUndoOp(child, deletedKey)

	// Try to get all keys except the deleted key from the child datastore
	checkKeyRange(t, keymap, keys, d, [][]string{{"0", "2/1/0"}, {"3", "3/1/1"}}, checkOp)

	// Add the deleted key into the datastore
	op(deletedKey)

	// Sync it
	if err := d.Sync(ctx, deletedKey); err != nil {
		t.Fatal(err)
	}

	// Check it
	checkOp(d, deletedKey)
}

func checkKeyRange(t *testing.T, keymap map[string]int, keys []key.Key,
	d ds.Datastore, validKeyRanges [][]string, checkFn func(ds.Datastore, key.Key)) {
	t.Helper()
	for _, validKeyBoundaries := range validKeyRanges {
		start, end := keymap[validKeyBoundaries[0]], keymap[validKeyBoundaries[1]]
		for _, k := range keys[start:end] {
			checkFn(d, k)
		}
	}
}
