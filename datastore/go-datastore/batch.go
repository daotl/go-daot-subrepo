// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package datastore

import (
	"context"

	key "github.com/daotl/go-datastore/key"
)

type op struct {
	key    key.Key
	delete bool
	value  []byte
}

// basicBatch implements the transaction interface for datastores who do
// not have any sort of underlying transactional support
type basicBatch struct {
	ops map[string]op

	target Datastore
}

func NewBasicBatch(ds Datastore) Batch {
	return &basicBatch{
		ops:    make(map[string]op),
		target: ds,
	}
}

func (bt *basicBatch) Put(ctx context.Context, key key.Key, val []byte) error {
	bt.ops[key.String()] = op{key: key, value: val}
	return nil
}

func (bt *basicBatch) Delete(ctx context.Context, key key.Key) error {
	bt.ops[key.String()] = op{key: key, delete: true}
	return nil
}

func (bt *basicBatch) Commit(ctx context.Context) error {
	var err error
	for _, op := range bt.ops {
		if op.delete {
			err = bt.target.Delete(ctx, op.key)
		} else {
			err = bt.target.Put(ctx, op.key, op.value)
		}
		if err != nil {
			break
		}
	}

	return err
}
