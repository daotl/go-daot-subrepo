// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package keytransform

import key "github.com/daotl/go-datastore/key"

// Pair is a convince struct for constructing a key transform.
type Pair struct {
	Convert KeyMapping
	Invert  KeyMapping
}

func (t *Pair) ConvertKey(k key.Key) key.Key {
	return t.Convert(k)
}

func (t *Pair) InvertKey(k key.Key) key.Key {
	return t.Invert(k)
}

var _ KeyTransform = (*Pair)(nil)

// PrefixTransform constructs a KeyTransform with a pair of functions that
// add or remove the given prefix key.
//
// Warning: will panic if prefix not found when it should be there. This is
// to avoid insidious data inconsistency errors.
type PrefixTransform struct {
	Prefix key.Key
}

// ConvertKey adds the prefix.
func (p PrefixTransform) ConvertKey(k key.Key) key.Key {
	return p.Prefix.Child(k)
}

// InvertKey removes the prefix. panics if prefix not found.
func (p PrefixTransform) InvertKey(k key.Key) key.Key {
	if p.Prefix.String() == "/" {
		return k
	}

	if !p.Prefix.IsAncestorOf(k) {
		panic("expected prefix not found")
	}

	return k.TrimPrefix(p.Prefix)
}

var _ KeyTransform = (*PrefixTransform)(nil)
