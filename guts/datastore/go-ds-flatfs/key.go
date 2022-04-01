// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package flatfs

import (
	"github.com/daotl/go-datastore/key"
)

// keyIsValid returns true if the key is valid for flatfs.

func keyIsValid(k key.Key) bool {
	switch k.KeyType() {
	// Allows StrKeys that match [0-9A-Z+-_=].
	case key.KeyTypeString:
		ks := k.String()
		if len(ks) < 2 || ks[0] != '/' {
			return false
		}
		for _, b := range ks[1:] {
			if '0' <= b && b <= '9' {
				continue
			}
			if 'A' <= b && b <= 'Z' {
				continue
			}
			switch b {
			case '+', '-', '_', '=':
				continue
			}
			return false
		}
		return true
	// Allows all BytesKeys, will be base64url encoded
	case key.KeyTypeBytes:
		return true
	default:
		return false
	}
}
