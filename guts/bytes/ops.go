// Author: https://github.com/wiryls

package bytes

// Concat an array of bytes.
func Concat(src ...[]byte) []byte {
	siz := 0
	for _, raw := range src {
		siz += len(raw)
	}

	buf := make([]byte, siz)
	pos := 0
	for _, bin := range src {
		pos += copy(buf[pos:], bin)
	}

	return buf
}
