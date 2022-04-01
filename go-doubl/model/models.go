package model

import (
	"io"
	"unsafe"

	cbg "github.com/daotl/cbor-gen"
	"github.com/daotl/go-marsha"
	"github.com/daotl/go-marsha/cborgen"
)

const (
	// Reference: https://datatracker.ietf.org/doc/html/rfc7049#section-2

	CborNull       = byte(0xf6)
	CborNullLength = 1

	// Signature initial 2 bytes:
	// 0x58: 0b010_11000: major type 2 (010) + additional information 24 means 1-byte length (11000)
	// 0x41: 1-byte length: 64 bytes
	SignatureCborInitialLength = 2
	SignatureCborDataLength    = 64

	// Transaction is serialized as a CBOR array
	// Transaction initial byte: major type 4 (100) + array length 7 (00111)
	TransactionCborInitial       = byte(0b100_00111)
	TransactionCborInitialLength = 1

	// BlockHeader is serialized as a CBOR array
	// BlockHeader initial byte: major type 4 (100) + array length 9 (01001)
	BlockHeaderCborInitial       = byte(0b100_01001)
	BlockHeaderCborInitialLength = 1

	// Block is serialized as a CBOR array
	// Block initial byte: major type 4 (100) + array length 2 (00010)
	BlockCborInitial       = byte(0b100_00010)
	BlockCborInitialLength = 1
)

var BlockCborInitialBytes = []byte{BlockCborInitial}

// Transaction defines a transaction that can be encoded into CBOR or JSON representation.
type Transaction struct {

	// Transaction type
	Type TransactionType `json:"type"`

	// Sender address
	From Address `json:"from"`

	// Nonce
	Nonce uint64 `json:"nonce"`

	// Target address (optional)
	To Address `json:"to,omitempty"`

	// Payload data (optional)
	Data []byte `json:"data,omitempty"`

	// Extra metadata (optional)
	// This can be used for e.g., encryption with secret sharing scheme
	Extra []byte `json:"extra,omitempty"`

	// Signature (omitted when calculating the signature)
	// Put it last in the CBOR array, so it can be efficiently appended.
	Sig Signature `json:"signature,omitempty"`
}

// Ptr implements marsha.Struct
func (t Transaction) Ptr() marsha.StructPtr { return &t }

// Val implements marsha.StructPtr
func (t *Transaction) Val() marsha.Struct { return *t }

// Size calculates the estimated occupied memory of Transaction in bytes.
func (t *Transaction) Size() uint64 {
	return uint64(
		int(unsafe.Sizeof(t)) +
			len(t.From) +
			len(t.To) +
			len(t.Data) +
			len(t.Extra) +
			len(t.Sig),
	)
}

// TransactionSlice is a slice of Transaction
type TransactionSlice []Transaction

// Val implements marsha.StructSlicePtr
func (txs *TransactionSlice) Val() []marsha.StructPtr {
	models := make([]marsha.StructPtr, 0, len(*txs))
	for i := range *txs {
		models = append(models, &(*txs)[i])
	}
	return models
}

// NewStructPtr implements cborgen.StructSlicePtr
func (*TransactionSlice) NewStructPtr() marsha.StructPtr { return new(Transaction) }

// Append implements cborgen.StructSlicePtr
func (txs *TransactionSlice) Append(m cborgen.StructPtr) { *txs = append(*txs, *(m.(*Transaction))) }

// Size calculates the estimated occupied memory of TransactionSlice in bytes.
func (txs TransactionSlice) Size() uint64 {
	var size uint64 = 0
	for _, tx := range []Transaction(txs) {
		size += tx.Size()
	}
	return size
}

// TransactionExt extends Transaction to hold some useful data that can be computed once and used in multiple places.
type TransactionExt struct {
	*Transaction

	// CBOR encoded Transaction
	Bytes []byte

	// Transaction hash
	Hash TransactionHash

	// Pointer to the unmarshaled Transaction.Extra field
	ExtraUnmarshaled ExtraPtr
}

// Size calculates the estimated occupied memory of TransactionExt in bytes.
func (txx *TransactionExt) Size() uint64 {
	size := uint64(unsafe.Sizeof(txx)) +
		txx.Transaction.Size() +
		uint64(len(txx.Bytes)+len(txx.Hash))
	if txx.ExtraUnmarshaled != nil {
		size += txx.ExtraUnmarshaled.Size()
	}
	return size
}

// TransactionSlice is a slice of pointers to TransactionExtSlice
type TransactionExtSlice []*TransactionExt

// Raw returns the wrapped Transactions.
func (txxs TransactionExtSlice) Raw() TransactionSlice {
	return TransactionSliceFromExtSlice(txxs)
}

// Size calculates the estimated occupied memory of TransactionExtSlice in bytes.
func (txxs TransactionExtSlice) Size() uint64 {
	var size uint64 = 0
	for _, txx := range []*TransactionExt(txxs) {
		size += txx.Size()
	}
	return size
}

// RawSize calculates the estimated occupied memory of underlying Transactions in bytes.
func (txxs TransactionExtSlice) RawSize() uint64 {
	var size uint64 = 0
	for _, txx := range []*TransactionExt(txxs) {
		size += txx.Transaction.Size()
	}
	return size
}

// BlockHeader defines a block header that can be encoded into CBOR or JSON representation.
type BlockHeader struct {

	// Creator
	Creator Address `json:"creator"`

	// Timestamp in Unix time
	Time Timestamp `json:"timestamp"`

	// Previous block hashes
	PrevHashes []BlockHash `json:"prevHashes,omitempty"`

	// Block height
	Height BlockHeight `json:"height"`

	// Root hash of some kind of hash-based tree (e.g., Merkle tree) of transactions
	TxRoot TransactionsRootHash `json:"transactionsRoot"`

	// Number of transactions contained in the block
	TxCount uint64 `json:"transactionCount"`

	// Arbitrary byte array returned by the ACEI application after executing and commiting the previous block.
	// It serves as the basis for validating any merkle proofs that comes from the ACEI application
	// and represents the state of the actual application rather than the state of the ledger itself.
	// The first block's block.Header.AppHash is given by ResponseInitLedger.app_hash in ACEI.
	AppHash []byte `json:"apphash,omitempty"`

	// Extra metadata (optional)
	// This can be used for e.g., consensus-specific data
	Extra []byte `json:"extra,omitempty"`

	// Signature (omitted when calculating the signature)
	// Put it last in the CBOR array, so it can be efficiently appended.
	Sig Signature `json:"signature,omitempty"`
}

// Ptr implements marsha.Struct
func (bh BlockHeader) Ptr() marsha.StructPtr { return &bh }

// Val implements marsha.StructPtr
func (bh *BlockHeader) Val() marsha.Struct { return *bh }

// Size calculates the estimated occupied memory of BlockHeader in bytes.
func (bh *BlockHeader) Size() uint64 {
	return uint64(
		int(unsafe.Sizeof(bh)) +
			len(bh.Creator) +
			len(bh.PrevHashes)*HashSize +
			len(bh.TxRoot) +
			len(bh.Extra) +
			len(bh.Sig),
	)
}

// BlockHeaderExt extends BlockHeader to hold some useful data that can be computed once and used in multiple places.
type BlockHeaderExt struct {
	*BlockHeader

	// CBOR encoded BlockHeader
	Bytes []byte

	// Block hash (the same as BlockHeader hash)
	Hash BlockHash

	// Pointer to the unmarshaled Transaction.Extra field
	ExtraUnmarshaled ExtraPtr
}

// Size calculates the estimated occupied memory of BlockHeaderExt in bytes.
func (bhx *BlockHeaderExt) Size() uint64 {
	size := uint64(unsafe.Sizeof(bhx)) +
		bhx.BlockHeader.Size() +
		uint64(len(bhx.Bytes)+len(bhx.Hash))
	if bhx.ExtraUnmarshaled != nil {
		size += bhx.ExtraUnmarshaled.Size()
	}
	return size
}

// Block defines a block that can be encoded into CBOR or JSON representation.
type Block struct {

	// Block header
	Header *BlockHeader `json:"header"`

	// TransactionSlice contained in the block
	Txs TransactionSlice `json:"transactions,omitempty"`
}

// Ptr implements marsha.Struct
func (b Block) Ptr() marsha.StructPtr { return &b }

// Val implements marsha.StructPtr
func (b *Block) Val() marsha.Struct { return *b }

// Size calculates the estimated occupied memory of Block in bytes.
func (b *Block) Size() uint64 {
	return uint64(unsafe.Sizeof(b)) + b.Header.Size() + b.Txs.Size()
}

// BlockExt extends Block to hold some useful data that can be computed once and used in multiple places.
type BlockExt struct {
	// block will only be constructed when Raw() is called the first time to prevent unnecessary copying.
	block *Block

	util *Util

	// Extended block header
	Header *BlockHeaderExt

	// Extended transactions
	Txs TransactionExtSlice
}

func (bx *BlockExt) Raw() *Block {
	if bx.block == nil {
		bx.block = &Block{
			Header: bx.Header.BlockHeader,
			Txs:    bx.Txs.Raw(),
		}
	}
	return bx.block
}

// Size calculates the estimated occupied memory of Block in bytes.
func (bx *BlockExt) Size() uint64 {
	size := uint64(unsafe.Sizeof(bx)) +
		bx.Header.Size() +
		bx.Txs.Size()
	return size
}

// WriteTo writes CBOR encoded block to the io.Writer.
func (bx *BlockExt) WriteTo(w io.Writer) (n int, err error) {
	var n_ int

	n_, err = w.Write(BlockCborInitialBytes)
	n += n_
	if err != nil {
		return n, err
	}

	n_, err = w.Write(bx.Header.Bytes)
	n += n_
	if err != nil {
		return n, err
	}

	if bx.Txs == nil || bx.Txs.Size() == 0 {
		n_, err = w.Write(cbg.CborNull)
	} else {
		n_, err = bx.util.WriteMarshalTransactionExtSliceTo(bx.Txs, w)
	}
	n += n_
	if err != nil {
		return n, err
	}

	return n, nil
}

// Extra is the interface the struct pointers to be put in TransactionExt.UnmarshaledExtra and
// BlockHeaderExt.UnmarshaledExtra must implement.
type ExtraPtr interface {
	marsha.StructPtr

	// Size calculates the estimated occupied memory of the struct ExtraPtr points to in bytes.
	Size() uint64
}

// ExtraCtor is a function that creates a new instance of T, which is an ExtraPtr.
type ExtraCtor[T ExtraPtr] func() T
