// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package flatfs

import (
	"testing"

	"github.com/daotl/go-datastore/key"
)

var (
	validKeys = []string{
		"/FOO",
		"/1BAR1",
		"/=EMACS-IS-KING=",
	}
	invalidKeys = []string{
		"/foo/bar",
		`/foo\bar`,
		"/foo\000bar",
		"/=Vim-IS-KING=",
	}
)

func TestKeyIsValid(t *testing.T) {
	for _, kstr := range validKeys {
		k := key.NewStrKey(kstr)
		if !keyIsValid(k) {
			t.Errorf("expected key %s to be valid", k)
		}
	}
	for _, kstr := range invalidKeys {
		k := key.NewStrKey(kstr)
		if keyIsValid(k) {
			t.Errorf("expected key %s to be invalid", k)
		}
	}
}
