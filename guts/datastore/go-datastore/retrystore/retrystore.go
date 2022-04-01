// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

// Package retrystore provides a datastore wrapper which
// allows to retry operations.
package retrystore

import (
	"context"
	"time"

	xerrors "golang.org/x/xerrors"

	ds "github.com/daotl/go-datastore"
	key "github.com/daotl/go-datastore/key"
)

// Datastore wraps a Batching datastore with a
// user-provided TempErrorFunc -which determines if an error
// is a temporal error and thus, worth retrying-, an amount of Retries
// -which specify how many times to retry an operation after
// a temporal error- and a base Delay, which is multiplied by the
// current retry and performs a pause before attempting the operation again.
type Datastore struct {
	TempErrFunc func(error) bool
	Retries     int
	Delay       time.Duration

	ds.Batching
}

var errFmtString = "ran out of retries trying to get past temporary error: %w"

func (d *Datastore) runOp(op func() error) error {
	err := op()
	if err == nil || !d.TempErrFunc(err) {
		return err
	}

	for i := 0; i < d.Retries; i++ {
		time.Sleep(time.Duration(i+1) * d.Delay)

		err = op()
		if err == nil || !d.TempErrFunc(err) {
			return err
		}
	}

	return xerrors.Errorf(errFmtString, err)
}

// DiskUsage implements the PersistentDatastore interface.
func (d *Datastore) DiskUsage(ctx context.Context) (uint64, error) {
	var size uint64
	err := d.runOp(func() error {
		var err error
		size, err = ds.DiskUsage(ctx, d.Batching)
		return err
	})
	return size, err
}

// Get retrieves a value given a key.
func (d *Datastore) Get(ctx context.Context, k key.Key) ([]byte, error) {
	var val []byte
	err := d.runOp(func() error {
		var err error
		val, err = d.Batching.Get(ctx, k)
		return err
	})

	return val, err
}

// Put stores a key/value.
func (d *Datastore) Put(ctx context.Context, k key.Key, val []byte) error {
	return d.runOp(func() error {
		return d.Batching.Put(ctx, k, val)
	})
}

// Sync implements Datastore.Sync
func (d *Datastore) Sync(ctx context.Context, prefix key.Key) error {
	return d.runOp(func() error {
		return d.Batching.Sync(ctx, prefix)
	})
}

// Has checks if a key is stored.
func (d *Datastore) Has(ctx context.Context, k key.Key) (bool, error) {
	var has bool
	err := d.runOp(func() error {
		var err error
		has, err = d.Batching.Has(ctx, k)
		return err
	})
	return has, err
}

// GetSize returns the size of the value in the datastore, if present.
func (d *Datastore) GetSize(ctx context.Context, k key.Key) (int, error) {
	var size int
	err := d.runOp(func() error {
		var err error
		size, err = d.Batching.GetSize(ctx, k)
		return err
	})
	return size, err
}
