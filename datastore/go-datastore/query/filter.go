// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package query

import (
	"bytes"
	"fmt"

	key "github.com/daotl/go-datastore/key"
)

// Filter is an object that tests ResultEntries
type Filter interface {
	// Filter returns whether an entry passes the filter
	Filter(e Entry) bool
}

// Op is a comparison operator
type Op string

var (
	Equal              = Op("==")
	NotEqual           = Op("!=")
	GreaterThan        = Op(">")
	GreaterThanOrEqual = Op(">=")
	LessThan           = Op("<")
	LessThanOrEqual    = Op("<=")
)

// FilterValueCompare is used to signal to datastores they
// should apply internal comparisons. unfortunately, there
// is no way to apply comparisons* to interface{} types in
// Go, so if the datastore doesnt have a special way to
// handle these comparisons, you must provided the
// TypedFilter to actually do filtering.
//
// [*] other than == and !=, which use reflect.DeepEqual.
type FilterValueCompare struct {
	Op    Op
	Value []byte
}

func (f FilterValueCompare) Filter(e Entry) bool {
	cmp := bytes.Compare(e.Value, f.Value)
	switch f.Op {
	case Equal:
		return cmp == 0
	case NotEqual:
		return cmp != 0
	case LessThan:
		return cmp < 0
	case LessThanOrEqual:
		return cmp <= 0
	case GreaterThan:
		return cmp > 0
	case GreaterThanOrEqual:
		return cmp >= 0
	default:
		panic(fmt.Errorf("unknown operation: %s", f.Op))
	}
}

func (f FilterValueCompare) String() string {
	return fmt.Sprintf("VALUE %s %q", f.Op, string(f.Value))
}

type FilterKeyCompare struct {
	Op  Op
	Key key.Key
}

func (f FilterKeyCompare) Filter(e Entry) bool {
	switch f.Op {
	case Equal:
		return e.Key.Equal(f.Key)
	case NotEqual:
		return !e.Key.Equal(f.Key)
	case GreaterThan:
		return f.Key.Less(e.Key)
	case GreaterThanOrEqual:
		return !e.Key.Less(f.Key)
	case LessThan:
		return e.Key.Less(f.Key)
	case LessThanOrEqual:
		return !f.Key.Less(e.Key)
	default:
		panic(fmt.Errorf("unknown op '%s'", f.Op))
	}
}

func (f FilterKeyCompare) String() string {
	return fmt.Sprintf("KEY %s %q", f.Op, f.Key)
}

type FilterKeyPrefix struct {
	Prefix key.Key
}

func (f FilterKeyPrefix) Filter(e Entry) bool {
	return e.Key.HasPrefix(f.Prefix) && !e.Key.Equal(f.Prefix)
}

func (f FilterKeyPrefix) String() string {
	return fmt.Sprintf("PREFIX(%q)", f.Prefix)
}

type FilterKeyRange struct {
	Range Range
}

func (f FilterKeyRange) Filter(e Entry) bool {
	if f.Range.Start != nil && e.Key.Less(f.Range.Start) ||
		f.Range.End != nil && (f.Range.End.Less(e.Key) || f.Range.End.Equal(e.Key)) {
		return false
	}
	return true
}

func (f FilterKeyRange) String() string {
	return fmt.Sprintf("RANGE[%q, %q)", f.Range.Start, f.Range.End)
}
