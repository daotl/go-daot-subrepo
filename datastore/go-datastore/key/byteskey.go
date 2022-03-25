// Copyright (c) 2020 DAOT Labs. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package key

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"unsafe"

	"github.com/google/uuid"
)

var (
	EmptyBytesKey = NewBytesKey([]byte{})

	ErrNotBytesKey = errors.New("argument is not of type BytesKey")
)

// BytesKey is a Key implementation backed by byte slice. It could improve
// performance in some cases compared to StrKey by preventing type conversion
// and reducing key size.
type BytesKey struct {
	bytes []byte
}

// NewBytesKey constructs a BytesKey from byte slice.
func NewBytesKey(bytes []byte) BytesKey {
	return BytesKey{bytes}
}

// NewBytesKeyFromString constructs a BytesKey from s.
func NewBytesKeyFromString(s string) BytesKey {
	k := BytesKey{[]byte(s)}
	return k
}

// NewBytesKeyFromStringUnsafe constructs a BytesKey from `s` using "unsafe"
// package to avoid copying. Be cautious that `s` should be discarded right
// after calling this method because its value may change.
func NewBytesKeyFromStringUnsafe(s string) BytesKey {
	// Method from: https://stackoverflow.com/a/66218124
	var bs = *(*[]byte)(unsafe.Pointer(&s))
	(*reflect.SliceHeader)(unsafe.Pointer(&bs)).Cap = len(s)

	return BytesKey{bs}
}

// KeyWithNamespaces constructs a key out of a namespace slice.
func BytesKeyWithNamespaces(ns [][]byte) BytesKey {
	return BytesKey{bytes.Join(ns, nil)}
}

// KeyType returns the key type (KeyTypeBytes)
func (k BytesKey) KeyType() KeyType {
	return KeyTypeBytes
}

// String gets the string value of Key
func (k BytesKey) String() string {
	return string(k.bytes)
}

// StringUnsafe gets the string value of Key using "unsafe" package to avoid
// copying. Be cautious that the string returned may change if the underlying
// byte slice is modified elsewhere, e.g. after being returned from BytesUnsafe.
func (k BytesKey) StringUnsafe() string {
	// Method from: https://github.com/golang/go/issues/25484#issuecomment-391415660
	return *(*string)(unsafe.Pointer(&k.bytes))
}

// Bytes returns a copy of the underlying byte slice of Key.
func (k BytesKey) Bytes() []byte {
	bs := make([]byte, len(k.bytes))
	copy(bs, k.bytes)
	return bs
}

// Bytes returns the underlying byte slice of Key. You should probably not
// modify the returned byte slice as it may have unintended side effects.
func (k BytesKey) BytesUnsafe() []byte {
	return k.bytes
}

// Equal checks equality of two keys
func (k BytesKey) Equal(k2 Key) bool {
	if k2 == nil {
		return false
	}
	bk2, ok := k2.(BytesKey)
	return ok && bytes.Equal(k.bytes, bk2.bytes)
}

// Less checks whether this key is sorted lower than another.
// Panic if `k2` is not a BytesKey.
func (k BytesKey) Less(k2 Key) bool {
	if k2 == nil {
		return false
	}
	if k2.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return bytes.Compare(k.bytes, k2.(BytesKey).bytes) == -1
}

// Child returns the `child` Key of this Key.
//   NewBytesKey({{BYTES1}}).Child(NewBytesKey({{BYTES2}}))
//   NewBytesKey({{BYTES1 || BYTES2}})
// Panic if `k2` is not a BytesKey.
func (k BytesKey) Child(k2 Key) Key {
	if k2 == nil {
		return k
	}
	if k2.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return k.ChildBytes(k2.(BytesKey).bytes)
}

// ChildBytes returns the `child` Key of this Key -- bytes helper.
//   NewBytesKey({{BYTES1}}).Child({{BYTES2}}))
//   NewBytesKey({{BYTES1 || BYTES2}})
func (k BytesKey) ChildBytes(b []byte) Key {
	kb := make([]byte, len(k.bytes)+len(b))
	copy(kb, k.bytes)
	copy(kb[len(k.bytes):], b)
	return BytesKey{kb}
}

// IsAncestorOf returns whether this key is a prefix of `other` (excluding equals).
//   NewBytesKey({{BYTES1}}).IsAncestorOf(NewBytesKey({{BYTES1 || BYTES2}}))
//   true
// Panic if `other` is not a BytesKey.
func (k BytesKey) IsAncestorOf(other Key) bool {
	if other == nil {
		return false
	}
	if other.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	bother := other.(BytesKey)
	return len(bother.bytes) > len(k.bytes) && bytes.HasPrefix(bother.bytes, k.bytes)
}

// IsDescendantOf returns whether this key contains another as a prefix (excluding equals).
//   NewBytesKey({{BYTES1 || BYTES2}}).IsDescendantOf({{BYTES1}})
//   true
// Panic if `other` is not a BytesKey.
func (k BytesKey) IsDescendantOf(other Key) bool {
	if other == nil {
		return true
	}
	if other.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return other.(BytesKey).IsAncestorOf(k)
}

// HasPrefix returns whether this key contains another as a prefix (including equals).
// Panic if `prefix` is not a BytesKey.
func (k BytesKey) HasPrefix(prefix Key) bool {
	if prefix == nil {
		return true
	}
	if prefix.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return bytes.HasPrefix(k.bytes, prefix.(BytesKey).bytes)
}

// HasPrefix returns whether this key contains another as a suffix (including equals).
// Panic if `suffix` is not a BytesKey.
func (k BytesKey) HasSuffix(suffix Key) bool {
	if suffix == nil {
		return true
	}
	if suffix.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return bytes.HasSuffix(k.bytes, suffix.(BytesKey).bytes)
}

// TrimPrefix returns a new key equals to this key without the provided leading prefix key.
// If `s` doesn't start with prefix, this key is returned unchanged.
// Panic if `prefix` is not a BytesKey.
func (k BytesKey) TrimPrefix(prefix Key) Key {
	if prefix == nil {
		return k
	}
	if prefix.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return NewBytesKey(bytes.TrimPrefix(k.bytes, prefix.(BytesKey).bytes))
}

// TrimSuffix returns a new key equals to this key without the provided trailing suffix key.
// If `s` doesn't end with suffix, this key is returned unchanged.
// Panic if `suffix` is not a BytesKey.
func (k BytesKey) TrimSuffix(suffix Key) Key {
	if suffix == nil {
		return k
	}
	if suffix.KeyType() != KeyTypeBytes {
		panic(ErrNotBytesKey)
	}
	return NewBytesKey(bytes.TrimSuffix(k.bytes, suffix.(BytesKey).bytes))
}

// MarshalJSON implements the json.Marshaler interface,
// keys are represented as base64-encoded JSON string
func (k BytesKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.bytes)
}

// UnmarshalJSON implements the json.Unmarshaler interface,
// keys will decode base64-encoded JSON string to raw bytes
func (k *BytesKey) UnmarshalJSON(data []byte) (err error) {
	k.bytes = []byte{}
	err = json.Unmarshal(data, &k.bytes)
	return
}

// RandomBytesKey returns a randomly (uuid) generated key.
//   RandomBytesKey()
//   NewBytesKey([]byte("f98719ea086343f7b71f32ea9d9d521d"))
func RandomBytesKey() Key {
	return BytesKey{[]byte(strings.Replace(uuid.New().String(), "-", "", -1))}
}

func StrsToBytesKeys(strs []string) []Key {
	keys := make([]Key, len(strs))
	for i, s := range strs {
		keys[i] = NewBytesKeyFromString(s)
	}
	return keys
}
