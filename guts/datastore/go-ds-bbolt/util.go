package dsbbolt

import (
	dskey "github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
)

func copyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func toQueryEntry(k []byte, v []byte, KeysOnly bool) query.Entry {
	var entry query.Entry
	entry.Key = dskey.NewBytesKey(copyBytes(k))
	if !KeysOnly {
		entry.Value = copyBytes(v)
	}
	entry.Size = len(v)
	return entry
}

// bytesPrefix returns key range that satisfy the given prefix,
// the bytes that equals to prefix is not included.
// start: prefix + 0x00
// limit: [1,2,ff,ff] -> [1,3,ff,ff]; [ff,ff] -> nil
func bytesPrefix(prefix []byte) ([]byte, []byte) {
	var limit []byte // the minimum bytes that greater than all bytes with the prefix
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	start := append(copyBytes(prefix), 0x00)

	return start, limit
}
