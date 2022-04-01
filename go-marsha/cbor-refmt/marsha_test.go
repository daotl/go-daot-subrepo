package cbor_refmt_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/daotl/go-marsha"
	cbor_refmt "github.com/daotl/go-marsha/cbor-refmt"
	"github.com/daotl/go-marsha/test"
)

type TestStructNoGen struct {
	Data string
}

func (s TestStructNoGen) Ptr() marsha.StructPtr { return &s }
func (s *TestStructNoGen) Val() marsha.Struct   { return *s }

type TestStructsNoGen []TestStructNoGen

func (s *TestStructsNoGen) Val() []marsha.StructPtr {
	models := make([]marsha.StructPtr, 0, len(*s))
	for i := range *s {
		models = append(models, &(*s)[i])
	}
	return models
}

type TestStruct2NoGen struct {
	Data2 int64
}

func (s TestStruct2NoGen) Ptr() marsha.StructPtr { return &s }
func (s *TestStruct2NoGen) Val() marsha.Struct   { return *s }

func TestSuite(t *testing.T) {
	mrsh := cbor_refmt.New()
	mrsh.Register(test.TestStruct{})
	test.SubTestAll(t, mrsh)
}

func TestNoGenBasic(t *testing.T) {
	req := require.New(t)
	asrt := assert.New(t)
	mrsh := cbor_refmt.New()
	mrsh.Register(test.TestStruct{})
	s := &TestStructNoGen{"test"}
	ss := &TestStructsNoGen{TestStructNoGen{"test"}, TestStructNoGen{"test2"}}

	t.Run("Marhsal error: model type not registered", func(t *testing.T) {
		_, err := mrsh.MarshalStruct(s)
		asrt.Error(err)
	})

	mrsh.Register(TestStructNoGen{})

	t.Run("MarshalStruct/UnmarshalStruct", func(t *testing.T) {
		bin, err := mrsh.MarshalStruct(s)
		asrt.NoError(err)
		s2 := &TestStructNoGen{}
		read, err := mrsh.UnmarshalStruct(bin, s2)
		req.NoError(err)
		asrt.Equal(s.Data, s2.Data)
		asrt.Equal(-1, read)
	})

	t.Run("UnmarshalStruct error: model type does not match", func(t *testing.T) {
		mrsh.Register(TestStruct2NoGen{})
		bin, err := mrsh.MarshalStruct(s)
		asrt.NoError(err)
		s2 := &TestStruct2NoGen{}
		read, err := mrsh.UnmarshalStruct(bin, s2)
		req.Error(err)
		asrt.Equal(-1, read)
	})

	t.Run("MarshalStructSlice/UnmarshalStructSlice", func(t *testing.T) {
		bin, err := mrsh.MarshalStructSlice(ss)
		asrt.NoError(err)
		ss2 := &TestStructsNoGen{}
		read, err := mrsh.UnmarshalStructSlice(bin, ss2)
		req.NoError(err)
		asrt.Equal(ss, ss2)
		asrt.Equal(-1, read)
	})

	t.Run("Encoder_decoder NoGen", func(t *testing.T) {
		var network bytes.Buffer
		enc := mrsh.NewEncoder(&network)
		dec := mrsh.NewDecoder(&network)

		t.Run("MarshalStruct/UnmarshalStruct", func(t *testing.T) {
			_, err := enc.EncodeStruct(s)
			asrt.NoError(err)
			s2 := &TestStructNoGen{}
			_, err = dec.DecodeStruct(s2)
			asrt.NoError(err)
			asrt.Equal(s.Data, s2.Data)
		})

		t.Run("MarshalStructSlice/UnmarshalStructSlice", func(t *testing.T) {
			_, err := enc.EncodeStructSlice(ss)
			asrt.NoError(err)
			ss2 := &TestStructsNoGen{}
			_, err = dec.DecodeStructSlice(ss2)
			asrt.NoError(err)
			asrt.Equal(ss, ss2)
		})
	})
}
