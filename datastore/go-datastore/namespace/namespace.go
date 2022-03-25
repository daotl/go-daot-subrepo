// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package namespace

import (
	ds "github.com/daotl/go-datastore"
	key "github.com/daotl/go-datastore/key"
	ktds "github.com/daotl/go-datastore/keytransform"
)

// PrefixTransform constructs a KeyTransform with a pair of functions that
// add or remove the given prefix key.
//
// Warning: will panic if prefix not found when it should be there. This is
// to avoid insidious data inconsistency errors.
//
// DEPRECATED: Use ktds.PrefixTransform directly.
func PrefixTransform(prefix key.Key) ktds.PrefixTransform {
	return ktds.PrefixTransform{Prefix: prefix}
}

// Wrap wraps a given datastore with a key-prefix.
func Wrap(child ds.Datastore, prefix key.Key) *ktds.Datastore {
	if child == nil {
		panic("child (ds.Datastore) is nil")
	}

	return ktds.Wrap(child, PrefixTransform(prefix))
}
