// Author: https://github.com/wiryls

package bytes

import (
	"encoding/binary"
	"encoding/hex"
	"unsafe"
)

/////////////////////////////////////////////////////////////////////////////

// FromU08 converts uint8 to bytes(little endian).
func FromU08(val uint8) []byte {
	return []byte{val}
}

// FromU16 converts uint16 to bytes(little endian).
func FromU16(val uint16) []byte {
	buffer := make([]byte, unsafe.Sizeof(uint16(0)))
	binary.LittleEndian.PutUint16(buffer, val)
	return buffer

	/* References:
	[Convert an integer to a byte array]
	(https://stackoverflow.com/a/16889357)
	*/
}

// FromU32 converts uint32 to bytes(little endian).
func FromU32(val uint32) []byte {
	buffer := make([]byte, unsafe.Sizeof(uint32(0)))
	binary.LittleEndian.PutUint32(buffer, val)
	return buffer
}

// FromU64 converts uint64 to bytes(little endian).
func FromU64(val uint64) []byte {
	buffer := make([]byte, unsafe.Sizeof(uint64(0)))
	binary.LittleEndian.PutUint64(buffer, val)
	return buffer
}

// FromI08 converts int8 to bytes(little endian).
func FromI08(val int8) []byte {
	return FromU08(uint8(val))
}

// FromI16 converts int16 to bytes(little endian).
func FromI16(val int16) []byte {
	return FromU16(uint16(val))
}

// FromI32 converts int32 to bytes(little endian).
func FromI32(val int32) []byte {
	return FromU32(uint32(val))
}

// FromI64 converts int64 to bytes(little endian).
func FromI64(val int64) []byte {
	return FromU64(uint64(val))
}

/////////////////////////////////////////////////////////////////////////////

// ToU08 convert bytes(little endian) to uint8.
func ToU08(val []byte) uint8 {
	switch {
	case len(val) == 0:
		return uint8(0)
	default:
		return val[0]
	}
}

// ToU16 convert bytes(little endian) to uint16.
func ToU16(val []byte) uint16 {
	return binary.LittleEndian.Uint16(val)
}

// ToU32 convert bytes(little endian) to uint32.
func ToU32(val []byte) uint32 {
	return binary.LittleEndian.Uint32(val)
}

// ToU64 convert bytes(little endian) to uint32.
func ToU64(val []byte) uint64 {
	return binary.LittleEndian.Uint64(val)
}

// ToI08 converts bytes to int8 (little endian).
func ToI08(val []byte) int8 {
	return int8(ToU08(val))
}

// ToI16 converts bytes to int16 (little endian).
func ToI16(val []byte) int16 {
	return int16(ToU16(val))
}

// ToI32 converts bytes to int32 (little endian).
func ToI32(val []byte) int32 {
	return int32(ToU32(val))
}

// ToI64 converts bytes to int64 (little endian).
func ToI64(val []byte) int64 {
	return int64(ToU64(val))
}

/////////////////////////////////////////////////////////////////////////////

// ToHexString convert a `src` to string and intercept the first n
// characters. If `lim` is 0, there is no limits.
func ToHexString(src []byte, lim int) (dst string) {

	switch {
	case lim == 0:
		fallthrough
	case hex.EncodedLen(len(src)) <= lim:
		dst = hex.EncodeToString(src)

	default:
		buf := make([]byte, lim)
		hex.Encode(buf, src[:hex.DecodedLen(lim)])

		for i := 1; i <= 3 && i <= lim; i++ {
			buf[lim-i] = byte('.')
		}
		dst = string(buf)
	}

	return
}
