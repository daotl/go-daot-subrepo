package model_test

import (
	"bytes"
	"fmt"
	"testing"

	ipldcbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/daotl/go-doubl/model"
	"github.com/daotl/go-doubl/test"
)

func TestMarshalUnmarshal(t *testing.T) {
	req := require.New(t)
	assr := assert.New(t)
	mrsh := test.Mrsh
	ut := test.Util

	t.Run("Transaction.MarshalCBOR/UnmarshalCBOR", func(t *testing.T) {
		tx := &test.TestTransaction
		var b bytes.Buffer
		n, err := tx.MarshalCBOR(&b)
		req.NoError(err)
		req.Equal(b.Len(), n)
		bin := b.Bytes()
		fmt.Println("Serialized Transaction size: ", len(bin))
		fmt.Printf("%02x\n", bin)
		tx2 := &Transaction{}
		read, err := tx2.UnmarshalCBOR(&b)
		req.NoError(err)
		assr.Equal(tx, tx2)
		assr.Equal(len(bin), read)
	})

	t.Run("Marshal & unmarshal Transaction", func(t *testing.T) {
		tx := &test.TestTransaction
		bin, err := mrsh.MarshalStruct(tx)
		req.NoError(err)
		fmt.Println("Serialized Transaction size: ", len(bin))
		fmt.Printf("%02x\n", bin)
		tx2 := &Transaction{}
		read, err := mrsh.UnmarshalStruct(bin, tx2)
		req.NoError(err)
		assr.Equal(tx, tx2)
		assr.Equal(len(bin), read)
	})

	t.Run("Marshal & unmarshal Transaction tuple", func(t *testing.T) {
		tx := &test.TestTransaction
		b1, err := ipldcbor.DumpObject(tx.Type)
		req.NoError(err)
		b2, err := ipldcbor.DumpObject(tx.From)
		req.NoError(err)
		b3, err := ipldcbor.DumpObject(tx.Nonce)
		req.NoError(err)
		b4, err := ipldcbor.DumpObject(tx.To)
		req.NoError(err)
		b5, err := ipldcbor.DumpObject(tx.Data)
		req.NoError(err)
		b6, err := ipldcbor.DumpObject(tx.Sig)
		req.NoError(err)
		l := len(b1) + len(b2) + len(b3) + len(b4) + len(b5) + len(b6)
		bin := make([]byte, 0, len(b1)+len(b2)+len(b3)+len(b4)+len(b5)+len(b6))
		bin = append(append(append(append(append(append(bin, b1...), b2...), b3...), b4...), b5...), b6...)
		req.Equal(l, len(bin))
		fmt.Println("Serialized Transaction size: ", l)
	})

	t.Run("Marshal & unmarshal TransactionSlice", func(t *testing.T) {
		txs := &test.TestTransactionSlice
		bin, err := mrsh.MarshalStructSlice(txs)
		req.NoError(err)
		fmt.Println("Serialized TransactionSlice size: ", len(bin))
		fmt.Printf("%02x\n", bin)
		txs2 := &TransactionSlice{}
		read, err := mrsh.UnmarshalStructSlice(bin, txs2)
		req.NoError(err)
		assr.Equal(txs, txs2)
		assr.Equal(len(bin), read)
	})

	// This is expected to not pass when using tuple encoding for the outmost struct.
	//t.Run("Test that Transaction with nil Sig field serialize correctly", func(t *testing.T) {
	//	tx := &test.TestTransaction
	//	tx.Sig = nil
	//	tx2 := &test.TransactionNoSig{
	//		Type:  tx.Type,
	//		From:  tx.From,
	//		Nonce: tx.Nonce,
	//		To:    tx.To,
	//		Data:  tx.Data,
	//		Extra: tx.Extra,
	//	}
	//	bin, err := mrsh.MarshalStruct(tx)
	//	req.NoError(err)
	//	bin2, err := mrsh.MarshalStruct(tx2)
	//	req.NoError(err)
	//	fmt.Printf("%0x\n", bin2)
	//	fmt.Printf("%0x\n", bin)
	//	req.Equal(bin2, bin)
	//})

	t.Run("BlockHeader.MarshalCBOR/UnmarshalCBOR", func(t *testing.T) {
		bh := &test.TestBlockHeader
		var b bytes.Buffer
		n, err := bh.MarshalCBOR(&b)
		req.NoError(err)
		req.Equal(b.Len(), n)
		bin := b.Bytes()
		fmt.Println("Serialized BlockHeader size: ", len(bin))
		bh2 := &BlockHeader{}
		read, err := bh2.UnmarshalCBOR(&b)
		req.NoError(err)
		assr.Equal(bh, bh2)
		assr.Equal(len(bin), read)
	})

	t.Run("Marshal & unmarshal BlockHeader", func(t *testing.T) {
		bh := &test.TestBlockHeader
		bin, err := mrsh.MarshalStruct(bh)
		fmt.Println("Serialized BlockHeader size: ", len(bin))
		fmt.Printf("%0x\n", bin)
		req.NoError(err)
		bh2 := &BlockHeader{}
		read, err := mrsh.UnmarshalStruct(bin, bh2)
		req.NoError(err)
		assr.Equal(bh, bh2)
		assr.Equal(len(bin), read)
	})

	t.Run("Marshal & unmarshal Block", func(t *testing.T) {
		b := &test.TestBlock
		bin, err := mrsh.MarshalStruct(b)
		fmt.Println("Serialized Block size: ", len(bin))
		fmt.Printf("%0x\n", bin)
		req.NoError(err)

		b2 := &Block{}
		read, err := mrsh.UnmarshalStruct(bin, b2)
		req.NoError(err)
		assr.Equal(b, b2)
		assr.Equal(len(bin), read)

		bx, err := ut.ExtendBlock(b)
		req.NoError(err)
		var buf bytes.Buffer
		n, err := bx.WriteTo(&buf)
		req.NoError(err)
		assr.Equal(buf.Len(), int(n))
		assr.Equal(bin, buf.Bytes())
	})
}

func BenchmarkMarshalUnmarshal(b *testing.B) {
	mrsh := test.Mrsh

	b.Run("Marshal Transaction map", func(b *testing.B) {
		tx := &test.TestTransaction
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mrsh.MarshalStruct(tx)
		}
	})

	b.Run("Unmarshal Transaction map", func(b *testing.B) {
		tx := &test.TestTransaction
		bin, _ := mrsh.MarshalStruct(tx)
		tx2 := &Transaction{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mrsh.UnmarshalStruct(bin, tx2)
		}
	})

	b.Run("Marshal Transaction tuple gen", func(b *testing.B) {
		tx := &test.TestTransaction
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var b bytes.Buffer
			tx.MarshalCBOR(&b)
			b.Bytes()
		}
	})

	b.Run("Marshal Transaction tuple append", func(b *testing.B) {
		tx := &test.TestTransaction
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b1, _ := ipldcbor.DumpObject(tx.Type)
			b2, _ := ipldcbor.DumpObject(tx.From)
			b3, _ := ipldcbor.DumpObject(tx.Nonce)
			b4, _ := ipldcbor.DumpObject(tx.To)
			b5, _ := ipldcbor.DumpObject(tx.Data)
			b6, _ := ipldcbor.DumpObject(tx.Sig)
			bin := make([]byte, 0, len(b1)+len(b2)+len(b3)+len(b4)+len(b5)+len(b6))
			bin = append(append(append(append(append(append(bin, b1...), b2...), b3...), b4...), b5...), b6...)
		}
	})

	b.Run("Marshal Transaction tuple copy", func(b *testing.B) {
		tx := &test.TestTransaction
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b1, _ := ipldcbor.DumpObject(tx.Type)
			b2, _ := ipldcbor.DumpObject(tx.From)
			b3, _ := ipldcbor.DumpObject(tx.Nonce)
			b4, _ := ipldcbor.DumpObject(tx.To)
			b5, _ := ipldcbor.DumpObject(tx.Data)
			b6, _ := ipldcbor.DumpObject(tx.Sig)
			bin := make([]byte, 0, len(b1)+len(b2)+len(b3)+len(b4)+len(b5)+len(b6))
			copy(bin, b1)
			copy(bin[len(bin):], b2)
			copy(bin[len(bin):], b3)
			copy(bin[len(bin):], b4)
			copy(bin[len(bin):], b5)
			copy(bin[len(bin):], b6)
		}
	})
}
