// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package keytransform

import key "github.com/daotl/go-datastore/key"

// KeyMapping is a function that maps one key to annother
type KeyMapping func(key.Key) key.Key

// KeyTransform is an object with a pair of functions for (invertibly)
// transforming keys
type KeyTransform interface {
	ConvertKey(key.Key) key.Key
	InvertKey(key.Key) key.Key
}
