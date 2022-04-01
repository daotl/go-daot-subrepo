package test

import (
	"github.com/daotl/go-marsha"
	"github.com/daotl/go-marsha/cborgen"
)

type TestStruct struct {
	Data string
}

func (s TestStruct) Ptr() marsha.StructPtr { return &s }
func (s *TestStruct) Val() marsha.Struct   { return *s }

type TestStructs []TestStruct

func (s *TestStructs) Val() []marsha.StructPtr {
	models := make([]marsha.StructPtr, 0, len(*s))
	for i := range *s {
		models = append(models, &(*s)[i])
	}
	return models
}
func (*TestStructs) NewStructPtr() marsha.StructPtr { return &TestStruct{} }

func (s *TestStructs) Append(m cborgen.StructPtr) { *s = append(*s, *(m.(*TestStruct))) }

type TestStruct2 struct {
	Data2 int64
}

func (s TestStruct2) Ptr() marsha.StructPtr { return &s }
func (s *TestStruct2) Val() marsha.Struct   { return *s }
