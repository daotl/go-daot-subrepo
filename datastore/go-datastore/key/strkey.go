// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package key

import (
	"encoding/json"
	"errors"
	"path"
	"reflect"
	"strings"
	"unsafe"

	"github.com/google/uuid"
)

var (
	EmptyStrKey = RawStrKey("/")

	ErrNotStrKey = errors.New("argument is not of type StrKey")
)

/*
A StrKey represents the unique identifier of an object.
StrKeys are meant to be unique across a system.

Our Key scheme is inspired by file systems and Google App Engine key model.
StrKeys are hierarchical, incorporating more and more specific namespaces.
Thus keys can be deemed 'children' or 'ancestors' of other keys::

    StrKey("/Comedy")
    StrKey("/Comedy/MontyPython")

Also, every namespace can be parametrized to embed relevant object
information. For example, the StrKey `name` (most specific namespace) could
include the object type::

    StrKey("/Comedy/MontyPython/Actor:JohnCleese")
    StrKey("/Comedy/MontyPython/Sketch:CheeseShop")
    StrKey("/Comedy/MontyPython/Sketch:CheeseShop/Character:Mousebender")

*/
type StrKey struct {
	string
}

// NewStrKey constructs a StrKey from string. it will clean the value.
func NewStrKey(s string) StrKey {
	k := StrKey{s}
	k.Clean()
	return k
}

// RawStrKey creates a new StrKey without safety checking the input. Use with care.
func RawStrKey(s string) StrKey {
	// accept an empty string and fix it to avoid special cases
	// elsewhere
	if len(s) == 0 {
		return StrKey{"/"}
	}

	// perform a quick sanity check that the key is in the correct
	// format, if it is not then it is a programmer error and it is
	// okay to panic
	if len(s) == 0 || s[0] != '/' || (len(s) > 1 && s[len(s)-1] == '/') {
		panic("invalid datastore key: " + s)
	}

	return StrKey{s}
}

// QueryStrKey creates a new StrKey without safety checking the input, intended to be used for query. Use with care.
func QueryStrKey(s string) StrKey {
	// accept an empty string and fix it to avoid special cases
	// elsewhere
	if len(s) == 0 {
		return StrKey{"/"}
	}

	// perform a quick sanity check that the key is in the correct
	// format, if it is not then it is a programmer error and it is
	// okay to panic
	if s[0] != '/' {
		panic("invalid datastore key: " + s)
	}

	return StrKey{s}
}

// Deprecated: NewKey just proxies calls to NewStrKey for backward compatibility.
func NewKey(s string) StrKey {
	return NewStrKey(s)
}

// Deprecated: RawKey just proxies calls to RawStrKey for backward compatibility.
func RawKey(s string) StrKey {
	return RawStrKey(s)
}

func NewStrKeyFromBytes(bytes []byte) StrKey {
	return NewStrKey(string(bytes))
}

// KeyWithNamespaces constructs a key out of a namespace slice.
func KeyWithNamespaces(ns []string) StrKey {
	return NewStrKey(strings.Join(ns, "/"))
}

// Clean up a StrKey, using path.Clean.
func (k *StrKey) Clean() {
	switch {
	case len(k.string) == 0:
		k.string = "/"
	case k.string[0] == '/':
		k.string = path.Clean(k.string)
	default:
		k.string = path.Clean("/" + k.string)
	}
}

// KeyType returns the key type (KeyTypeString)
func (k StrKey) KeyType() KeyType {
	return KeyTypeString
}

// String returns the string value of Key
func (k StrKey) String() string {
	return k.string
}

// StringUnsafe is the same as String
func (k StrKey) StringUnsafe() string {
	return k.string
}

// Bytes gets the value of Key as a []byte
func (k StrKey) Bytes() []byte {
	return []byte(k.string)
}

// BytesUnsafe gets the value of Key as a []byte using "unsafe" package to
// avoid copying. You should probably not modify the returned byte slice as
// it may have unintended side effects.
func (k StrKey) BytesUnsafe() []byte {
	// Method from: https://stackoverflow.com/a/66218124
	var bs = *(*[]byte)(unsafe.Pointer(&k.string))
	(*reflect.SliceHeader)(unsafe.Pointer(&bs)).Cap = len(k.string)

	return bs
}

// Equal checks equality of two keys
func (k StrKey) Equal(k2 Key) bool {
	if k2 == nil {
		return false
	}
	sk2, ok := k2.(StrKey)
	return ok && k.string == sk2.string
}

// Less checks whether this key is sorted lower than another.
// Panic if `k2` is not a StrKey.
func (k StrKey) Less(k2 Key) bool {
	if k2 == nil {
		return false
	}
	if k2.KeyType() != KeyTypeString {
		panic(ErrNotStrKey)
	}
	list1 := k.List()
	list2 := k2.(StrKey).List()
	for i, c1 := range list1 {
		if len(list2) < (i + 1) {
			return false
		}

		c2 := list2[i]
		if c1 < c2 {
			return true
		} else if c1 > c2 {
			return false
		}
		// c1 == c2, continue
	}

	// list1 is shorter or exactly the same.
	return len(list1) < len(list2)
}

// List returns the `list` representation of this Key.
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").List()
//   ["Comedy", "MontyPythong", "Actor:JohnCleese"]
func (k StrKey) List() []string {
	return strings.Split(k.string, "/")[1:]
}

// Reverse returns the reverse of this Key.
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Reverse()
//   NewStrKey("/Actor:JohnCleese/MontyPython/Comedy")
func (k StrKey) Reverse() Key {
	l := k.List()
	r := make([]string, len(l))
	for i, e := range l {
		r[len(l)-i-1] = e
	}
	return KeyWithNamespaces(r)
}

// Namespaces returns the `namespaces` making up this Key.
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Namespaces()
//   ["Comedy", "MontyPython", "Actor:JohnCleese"]
func (k StrKey) Namespaces() []string {
	return k.List()
}

// BaseNamespace returns the "base" namespace of this key (path.Base(filename))
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").BaseNamespace()
//   "Actor:JohnCleese"
func (k StrKey) BaseNamespace() string {
	n := k.Namespaces()
	return n[len(n)-1]
}

// Type returns the "type" of this key (value of last namespace).
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Type()
//   "Actor"
func (k StrKey) Type() string {
	return NamespaceType(k.BaseNamespace())
}

// Name returns the "name" of this key (field of last namespace).
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Name()
//   "JohnCleese"
func (k StrKey) Name() string {
	return NamespaceValue(k.BaseNamespace())
}

// Instance returns an "instance" of this type key (appends value to namespace).
//   NewStrKey("/Comedy/MontyPython/Actor").Instance("JohnClesse")
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese")
func (k StrKey) Instance(s string) Key {
	return NewStrKey(k.string + ":" + s)
}

// Path returns the "path" of this key (parent + type).
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Path()
//   NewStrKey("/Comedy/MontyPython/Actor")
func (k StrKey) Path() Key {
	s := k.Parent().string + "/" + NamespaceType(k.BaseNamespace())
	return NewStrKey(s)
}

// Parent returns the `parent` Key of this Key.
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese").Parent()
//   NewStrKey("/Comedy/MontyPython")
func (k StrKey) Parent() StrKey {
	n := k.List()
	if len(n) == 1 {
		return EmptyStrKey
	}
	return NewStrKey(strings.Join(n[:len(n)-1], "/"))
}

// Child returns the `child` Key of this Key.
//   NewStrKey("/Comedy/MontyPython").Child(NewStrKey("Actor:JohnCleese"))
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese")
// Panic if `k2` is not a StrKey.
func (k StrKey) Child(k2 Key) Key {
	return k.ChildStrKey(k2)
}

// ChildStrKey is the same as Child but returns a StrKey.
func (k StrKey) ChildStrKey(k2 Key) StrKey {
	if k2 == nil {
		return k
	}
	if k2.KeyType() != KeyTypeString {
		panic(ErrNotStrKey)
	}
	sk2 := k2.(StrKey)
	switch {
	case k.string == "/":
		return sk2
	case sk2.string == "/":
		return k
	default:
		return RawStrKey(k.string + sk2.string)
	}
}

// ChildString returns the `child` Key of this Key -- string helper.
//   NewStrKey("/Comedy/MontyPython").ChildString("Actor:JohnCleese")
//   NewStrKey("/Comedy/MontyPython/Actor:JohnCleese")
func (k StrKey) ChildString(s string) Key {
	return k.ChildStringStrKey(s)
}

// ChildStringStrKey is the same as ChildString but returns a StrKey.
func (k StrKey) ChildStringStrKey(s string) StrKey {
	return NewStrKey(k.string + "/" + s)
}

// IsAncestorOf returns whether this key is a prefix of `other` (excluding equals).
//   NewStrKey("/Comedy").IsAncestorOf("/Comedy/MontyPython")
//   true
// Panic if `other` is not a StrKey.
func (k StrKey) IsAncestorOf(other Key) bool {
	if other == nil {
		return false
	}
	// equivalent to HasPrefix(other, k.string + "/")
	if other.KeyType() != KeyTypeString {
		panic(ErrNotStrKey)
	}
	sother := other.(StrKey)

	if len(sother.string) <= len(k.string) {
		// We're not long enough to be a child.
		return false
	}

	if k.string == "/" {
		// We're the root and the other key is longer.
		return true
	}

	// "other" starts with /k.string/
	return sother.string[len(k.string)] == '/' && sother.string[:len(k.string)] == k.string
}

// IsDescendantOf returns whether this key contains another as a prefix (excluding equals).
//   NewStrKey("/Comedy/MontyPython").IsDescendantOf("/Comedy")
//   true
// Panic if `other` is not a StrKey.
func (k StrKey) IsDescendantOf(other Key) bool {
	if other == nil {
		return true
	}
	if other.KeyType() != KeyTypeString {
		panic(ErrNotStrKey)
	}
	return other.(StrKey).IsAncestorOf(k)
}

// IsTopLevel returns whether this key has only one namespace.
func (k StrKey) IsTopLevel() bool {
	return len(k.List()) == 1
}

// HasPrefix returns whether this key contains another as a prefix (including equals).
// Panic if `prefix` is not a StrKey.
func (k StrKey) HasPrefix(prefix Key) bool {
	if prefix == nil {
		return true
	}
	if prefix.KeyType() != KeyTypeString {
		panic(ErrNotStrKey)
	}
	return strings.HasPrefix(k.string, prefix.(StrKey).string)
}

// HasPrefix returns whether this key contains another as a suffix (including equals).
// Panic if `suffix` is not a StrKey.
func (k StrKey) HasSuffix(suffix Key) bool {
	if suffix == nil {
		return true
	}
	if suffix.KeyType() != KeyTypeString {
		panic(ErrNotStrKey)
	}
	return strings.HasSuffix(k.string, suffix.(StrKey).string)
}

// TrimPrefix returns a new key equals to this key without the provided leading prefix key.
// If s doesn't start with prefix, this key is returned unchanged.
// Panic if `prefix` is not a StrKey.
func (k StrKey) TrimPrefix(prefix Key) Key {
	if prefix == nil {
		return k
	}
	if prefix.KeyType() != KeyTypeString {
		panic(ErrNotStrKey)
	}
	return NewStrKey(strings.TrimPrefix(k.string, prefix.(StrKey).string))
}

// TrimSuffix returns a new key equals to this key without the provided trailing suffix key.
// If s doesn't end with suffix, this key is returned unchanged.
// Panic if `suffix` is not a StrKey.
func (k StrKey) TrimSuffix(suffix Key) Key {
	if suffix == nil {
		return k
	}
	if suffix.KeyType() != KeyTypeString {
		panic(ErrNotStrKey)
	}
	return NewStrKey(strings.TrimSuffix(k.string, suffix.(StrKey).string))
}

// MarshalJSON implements the json.Marshaler interface,
// keys are represented as JSON strings
func (k StrKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface,
// keys will parse any value specified as a key to a string
func (k *StrKey) UnmarshalJSON(data []byte) error {
	var key string
	if err := json.Unmarshal(data, &key); err != nil {
		return err
	}
	*k = NewStrKey(key)
	return nil
}

// RandomStrKey returns a randomly (uuid) generated key.
//   RandomStrKey()
//   NewStrKey("/f98719ea086343f7b71f32ea9d9d521d")
func RandomStrKey() Key {
	return NewStrKey(strings.Replace(uuid.New().String(), "-", "", -1))
}

// Deprecated: RandomKey just proxy calls to RandomStrKey for backward compatibility.
func RandomKey() Key {
	return RandomStrKey()
}

/*
A Key Namespace is like a path element.
A namespace can optionally include a type (delimited by ':')

    > NamespaceValue("Song:PhilosopherSong")
    PhilosopherSong
    > NamespaceType("Song:PhilosopherSong")
    Song
    > NamespaceType("Music:Song:PhilosopherSong")
    Music:Song
*/

// NamespaceType is the first component of a namespace. `foo` in `foo:bar`
func NamespaceType(namespace string) string {
	parts := strings.Split(namespace, ":")
	if len(parts) < 2 {
		return ""
	}
	return strings.Join(parts[0:len(parts)-1], ":")
}

// NamespaceValue returns the last component of a namespace. `baz` in `f:b:baz`
func NamespaceValue(namespace string) string {
	parts := strings.Split(namespace, ":")
	return parts[len(parts)-1]
}

func StrsToStrKeys(strs []string) []Key {
	keys := make([]Key, len(strs))
	for i, s := range strs {
		keys[i] = NewStrKey(s)
	}
	return keys
}

// StrsToKeys is an alias that proxies calls to StrsToStrKeys for backwards compatibility.
func StrsToKeys(strs []string) []Key {
	return StrsToStrKeys(strs)
}

func StrsToQueryKeys(strs []string) []Key {
	keys := make([]Key, len(strs))
	for i, s := range strs {
		keys[i] = QueryStrKey(s)
	}
	return keys
}
