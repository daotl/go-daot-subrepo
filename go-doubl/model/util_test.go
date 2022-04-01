package model_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/daotl/go-doubl/model"
	"github.com/daotl/go-doubl/test"
)

func TestUtil(t *testing.T) {
	req := require.New(t)
	assr := assert.New(t)
	var err error
	ut := test.Util

	t.Run("Signature verification", func(t *testing.T) {
		tx := &test.TestTransaction
		txx, err := ut.ExtendTransaction(tx)
		req.NoError(err)

		ok, err := ut.VerifyTransactionSignature(tx)
		assr.NoError(err)
		assr.True(ok)
		ok, err = ut.VerifyTransactionExtSignature(txx)
		assr.NoError(err)
		assr.True(ok)
	})

	t.Run("GenRootHashFromXxx", func(t *testing.T) {
		txs := test.TestTransactionSlice
		txhs := make([]TransactionHash, len(txs))
		for i, tx := range txs {
			txhs[i], err = ut.HashTransaction(&tx)
			req.NoError(err)
		}
		h := ut.GenRootHashFromTransactionHashes(txhs)

		h_, err := ut.GenRootHashFromTransactionSlice(txs)
		req.NoError(err)
		assr.Equal(h, h_)

		txxs, err := ut.ExtendTransactionSlice(txs)
		req.NoError(err)
		h_ = ut.GenRootHashFromTransactionExtSlice(txxs)
		assr.Equal(h, h_)
	})

	t.Run("Block bytes operations", func(t *testing.T) {
		b := &Block{
			Header: &test.TestBlockHeader,
			Txs:    test.TestTransactionSlice,
		}
		bx, err := ut.ExtendBlock(b)
		req.NoError(err)
		bhx, err := ut.ExtendBlockHeader(b.Header)
		req.NoError(err)
		txxs, err := ut.ExtendTransactionSlice(b.Txs)
		req.NoError(err)
		bin, err := ut.Mrsh.MarshalStruct(b)
		req.NoError(err)
		txsBytes, err := ut.Mrsh.MarshalStructSlice(&b.Txs)

		bhx_, read1, err := ut.ReadBlockHeaderExtFromBlockStream(bytes.NewReader(bin))
		//bhx_, err := ut.ExtractBlockHeaderExtFromBlockBytes(bin)
		req.NoError(err)
		assr.Equal(bhx, bhx_)

		txxs_, read2, err := ut.ReadTransactionExtSliceFrom(bytes.NewReader(txsBytes))
		//txxs_, err := ut.TransactionExtSliceFromTransactionsBytes(txsBytes)
		req.NoError(err)
		assr.Equal(len(txsBytes), int(read2))
		assr.Equal(len(bin), int(read1+read2))
		assr.Equal(txxs, txxs_)

		bx_, read, err := ut.ReadBlockExtFrom(bytes.NewReader(bin))
		//bx_, err := ut.BlockExtFromBytes(bin)
		req.NoError(err)
		assr.Equal(len(bin), int(read))
		assr.Equal(bhx, bx_.Header)
		assr.Equal(txxs, bx_.Txs)
		assr.NotEqual(bx, bx_) // Because bx_.block should be nil now (lazy initialized)
		assr.Equal(b, bx_.Raw())
		assr.Equal(bx, bx_) // bx_.block should equal to `b` now
	})
}
