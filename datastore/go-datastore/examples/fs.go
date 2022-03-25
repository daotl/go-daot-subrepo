// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

// Package fs is a simple Datastore implementation that stores keys
// as directories and files, mirroring the key. That is, the key
// "/foo/bar" is stored as file "PATH/foo/bar/.dsobject".
//
// This means key some segments will not work. For example, the
// following keys will result in unwanted behavior:
//
//     - "/foo/./bar"
//     - "/foo/../bar"
//     - "/foo\x00bar"
//
// Keys that only differ in case may be confused with each other on
// case insensitive file systems, for example in OS X.
//
// This package is intended for exploratory use, where the user would
// examine the file system manually, and should only be used with
// human-friendly, trusted keys. You have been warned.
package examples

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	ds "github.com/daotl/go-datastore"
	key "github.com/daotl/go-datastore/key"
	query "github.com/daotl/go-datastore/query"
)

var ObjectKeySuffix = ".dsobject"

// Datastore uses a uses a file per key to store values.
type Datastore struct {
	path string
}

// NewDatastore returns a new fs Datastore at given `path`
func NewDatastore(path string) (ds.Datastore, error) {
	if !isDir(path) {
		return nil, fmt.Errorf("failed to find directory at: %v (file? perms?)", path)
	}

	return &Datastore{path: path}, nil
}

// KeyFilename returns the filename associated with `key`
func (d *Datastore) KeyFilename(key key.Key) string {
	return filepath.Join(d.path, key.String(), ObjectKeySuffix)
}

// Put stores the given value.
func (d *Datastore) Put(ctx context.Context, key key.Key, value []byte) (err error) {
	fn := d.KeyFilename(key)

	// mkdirall above.
	err = os.MkdirAll(filepath.Dir(fn), 0o755)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(fn, value, 0o666)
}

// Sync would ensure that any previous Puts under the prefix are written to disk.
// However, they already are.
func (d *Datastore) Sync(ctx context.Context, prefix key.Key) error {
	return nil
}

// Get returns the value for given key
func (d *Datastore) Get(ctx context.Context, key key.Key) (value []byte, err error) {
	fn := d.KeyFilename(key)
	if !isFile(fn) {
		return nil, ds.ErrNotFound
	}

	return ioutil.ReadFile(fn)
}

// Has returns whether the datastore has a value for a given key
func (d *Datastore) Has(ctx context.Context, key key.Key) (exists bool, err error) {
	return ds.GetBackedHas(ctx, d, key)
}

func (d *Datastore) GetSize(ctx context.Context, key key.Key) (size int, err error) {
	return ds.GetBackedSize(ctx, d, key)
}

// Delete removes the value for given key
func (d *Datastore) Delete(ctx context.Context, key key.Key) (err error) {
	fn := d.KeyFilename(key)
	if !isFile(fn) {
		return nil
	}

	err = os.Remove(fn)
	if os.IsNotExist(err) {
		err = nil // idempotent
	}
	return err
}

// Query implements Datastore.Query
func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	results := make(chan query.Result)

	walkFn := func(path string, info os.FileInfo, _ error) error {
		// remove ds path prefix
		relPath, err := filepath.Rel(d.path, path)
		if err == nil {
			path = filepath.ToSlash(relPath)
		}

		if !info.IsDir() {
			path = strings.TrimSuffix(path, ObjectKeySuffix)
			// Only selects keys that are strict children of the prefix.
			if path[0] != '/' {
				path = "/" + path
			}
			if q.Prefix.KeyType() == key.KeyTypeString && path == q.Prefix.String() {
				return nil
			}
			var result query.Result
			key := key.NewStrKey(path)
			result.Entry.Key = key
			if !q.KeysOnly {
				result.Entry.Value, result.Error = d.Get(ctx, key)
			}
			results <- result
		}
		return nil
	}

	go func() {
		if q.Prefix.KeyType() == key.KeyTypeString && q.Prefix.String() != "" {
			filepath.Walk(filepath.Join(d.path, q.Prefix.String()), walkFn)
		} else {
			filepath.Walk(d.path, walkFn)
		}
		close(results)
	}()
	r := query.ResultsWithChan(q, results)
	q1 := q
	q1.Prefix = nil
	r = query.NaiveQueryApply(q1, r)
	return r, nil
}

// isDir returns whether given path is a directory
func isDir(path string) bool {
	finfo, err := os.Stat(path)
	if err != nil {
		return false
	}

	return finfo.IsDir()
}

// isFile returns whether given path is a file
func isFile(path string) bool {
	finfo, err := os.Stat(path)
	if err != nil {
		return false
	}

	return !finfo.IsDir()
}

func (d *Datastore) Close() error {
	return nil
}

func (d *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(d), nil
}

// DiskUsage returns the disk size used by the datastore in bytes.
func (d *Datastore) DiskUsage(ctx context.Context) (uint64, error) {
	var du uint64
	err := filepath.Walk(d.path, func(p string, f os.FileInfo, err error) error {
		if err != nil {
			log.Println(err)
			return err
		}
		if f != nil && f.Mode().IsRegular() {
			du += uint64(f.Size())
		}
		return nil
	})
	return du, err
}
