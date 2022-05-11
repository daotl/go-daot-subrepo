// Author: https://github.com/wiryls

package bytes_test

import (
	"bytes"
	"testing"

	. "github.com/daotl/guts/bytes"
)

func TestConcat(t *testing.T) {
	cases := [...][2][]byte{
		{[]byte(""), Concat()},
		{[]byte(""), Concat(nil)},
		{[]byte(""), Concat([]byte(""))},
		{[]byte(""), Concat([]byte(""), []byte(""))},
		{[]byte(""), Concat([]byte(""), nil)},
		{[]byte("Q"), Concat([]byte("Q"), nil)},
		{[]byte("A"), Concat([]byte(""), []byte("A"), nil)},
		{[]byte("Q"), Concat(nil, []byte("Q"), []byte(""))},
		{[]byte("OTZ"), Concat([]byte("OT"), []byte("Z"))},
		{[]byte("OTZ"), Concat([]byte(""), []byte("OTZ"))},
		{[]byte("OTZ"), Concat([]byte("O"), []byte("T"), []byte("Z"))},
	}

	for i := range cases {
		if bytes.Equal(cases[i][0], cases[i][1]) == false {
			t.Errorf("Expect '%s' but get '%s'", cases[i][0], cases[i][1])
		}
	}
}
