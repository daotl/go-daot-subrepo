package cborgen_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/daotl/go-marsha/cborgen"
	"github.com/daotl/go-marsha/test"
)

func TestMarsha(t *testing.T) {
	mrsh := cborgen.New()
	test.SubTestAll(t, mrsh)
}

func TestSpecial(t *testing.T) {
	req := require.New(t)
	asrt := assert.New(t)
	mrsh := cborgen.New()
	s := &test.TestStruct{Data: "test"}

	t.Run("UnmarshalStruct error: model type does not match", func(t *testing.T) {
		bin, err := mrsh.MarshalStruct(s)
		req.NoError(err)
		s2 := &test.TestStruct2{}
		_, err = mrsh.UnmarshalStruct(bin, s2)
		asrt.Equal(cborgen.ErrTypeNotMatch, err)
	})
}
