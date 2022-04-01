package model

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"sync"
	"unsafe"

	"github.com/crpt/go-crpt"
	cbg "github.com/daotl/cbor-gen"
	"github.com/daotl/go-marsha"
)

var (
	ErrInvalidBytes = errors.New("failed to unmarshal because of invalid bytes")
)

const maxCBORHeaderSize = 9

// Util provides utility methods to work with DOUBL models.
type Util struct {
	Mrsh marsha.Marsha
	Crpt crpt.Crpt

	cborHeaderBufPool sync.Pool
}

// New creates a new Util with the specified Marsha and Crpt instances.
func New(mrsh marsha.Marsha, crpt crpt.Crpt) *Util {
	return &Util{
		Mrsh: mrsh,
		Crpt: crpt,
		cborHeaderBufPool: sync.Pool{
			New: func() interface{} {
				b := make([]byte, maxCBORHeaderSize)
				return &b
			},
		},
	}
}

// TransactionsFromExtPtrs returns the Transactions wrapped in the TransactionExtSlice.
func TransactionSliceFromExtSlice(txxs TransactionExtSlice) TransactionSlice {
	txs := make(TransactionSlice, len(txxs))
	for i, bhx := range txxs {
		txs[i] = *bhx.Transaction
	}
	return txs
}

// BlockHeadersFromExts returns the BlockHeaders wrapped in the BlockHeaderExts.
func BlockHeadersFromExts(bhxs []BlockHeaderExt) []BlockHeader {
	bhs := make([]BlockHeader, len(bhxs))
	for i, bhx := range bhxs {
		bhs[i] = *bhx.BlockHeader
	}
	return bhs
}

// BlocksFromExts returns the Blocks wrapped in the BlockExts.
func BlocksFromExts(bxs []BlockExt) []Block {
	bs := make([]Block, len(bxs))
	for i, bhx := range bxs {
		bs[i] = *bhx.Raw()
	}
	return bs
}

// ExtendTransaction extends a Transaction into a TransactionExt.
//
// NOTE: ExtraUnmarshaled is not set yet.
func (u *Util) ExtendTransaction(tx *Transaction) (*TransactionExt, error) {
	bin, err := u.Mrsh.MarshalStruct(tx)
	if err != nil {
		return nil, err
	}
	return &TransactionExt{
		Transaction: tx,
		Bytes:       bin,
		Hash:        u.Crpt.Hash(bin),
	}, nil
}

// ExtendTransaction extends TransactionSlice into TransactionExtSlice.
//
// NOTE: ExtraUnmarshaled is not set yet.
func (u *Util) ExtendTransactionSlice(txs TransactionSlice) (TransactionExtSlice, error) {
	txxs := make(TransactionExtSlice, len(txs))
	var err error
	for i := range txs {
		if txxs[i], err = u.ExtendTransaction(&txs[i]); err != nil {
			return nil, err
		}
	}
	return txxs, nil
}

// ReadTransactionExtFrom reads and unmarshals the encoded transaction from `r`
// and extends it into a TransactionExt, it also returns the number of bytes read.
//
// NOTE: ExtraUnmarshaled is not set yet.
func (u *Util) ReadTransactionExtFrom(r io.Reader) (txx *TransactionExt, n int64, err error) {
	txx = &TransactionExt{
		Transaction: new(Transaction),
	}
	var buf bytes.Buffer
	tr := io.TeeReader(r, &buf)
	n_, err := u.Mrsh.NewDecoder(tr).DecodeStruct(txx.Transaction)
	n = int64(n_)
	if err != nil {
		return nil, n, err
	}
	txx.Bytes = buf.Bytes()
	txx.Hash = u.Crpt.Hash(txx.Bytes)
	return txx, n, err
}

// TransactionExtFromBytes unmarshals Transaction and extends it into a TransactionExt.
// TransactionExt.Bytes points to the same underlying memory as `bin` for performance consideration,
// it's not safe to modify it anywhere.
//
// NOTE: ExtraUnmarshaled is not set yet.
//
// Deprecated: Use ReadTransactionExtFrom instead.
func (u *Util) TransactionExtFromBytes(bin []byte) (*TransactionExt, error) {
	txx := &TransactionExt{
		Transaction: new(Transaction),
	}
	read, err := u.Mrsh.UnmarshalStruct(bin, txx.Transaction)
	if err != nil {
		return nil, err
	}
	txx.Bytes = bin[:read]
	txx.Hash = u.Crpt.Hash(bin[:read])
	return txx, err
}

// TransactionExtSliceFromBytesSlice unmarshal Transaction byte slices and extends them into TransactionExtSlice.
// TransactionExt.Bytes points to the same underlying memory as `bins` for performance consideration,
// it's not safe to modify it anywhere.
//
// NOTE: ExtraUnmarshaled is not set yet.
func (u *Util) TransactionExtSliceFromBytesSlice(bins [][]byte) (TransactionExtSlice, error) {
	txxs := make(TransactionExtSlice, len(bins))
	var err error
	for i, bin := range bins {
		if txxs[i], _, err = u.ReadTransactionExtFrom(bytes.NewReader(bin)); err != nil {
			return nil, err
		}
	}
	return txxs, nil
}

// ReadTransactionExtSliceFrom reads and unmarshals the encoded transactions from `r` and extends
// them into TransactionExtSlice, it also returns the number of bytes read.
func (u *Util) ReadTransactionExtSliceFrom(r io.Reader) (
	txxs TransactionExtSlice, n int64, err error,
) {
	scratch := u.cborHeaderBufPool.Get().(*[]byte)
	defer u.cborHeaderBufPool.Put(scratch)
	majorType, extra, n_, err := cbg.CborReadHeaderBuf(r, *scratch)
	n = int64(n_)
	if err != nil {
		return nil, n, err
	} else if majorType == cbg.MajOther && extra == 22 { // CBOR Null, no transaction
		return nil, n, nil
	} else if majorType != cbg.MajArray {
		return nil, n, ErrInvalidBytes
	}

	txxs = make(TransactionExtSlice, 0, extra)
	for i := uint64(0); i < extra; i++ {
		txx, n_, err := u.ReadTransactionExtFrom(r)
		n += n_
		if err != nil {
			return txxs, n, err
		}
		txxs = append(txxs, txx)
	}
	return txxs, n, nil
}

// TransactionExtSliceFromTransactionsBytes unmarshals a single Transactions bytes
// into TransactionSlice and extends them into TransactionExtSlice.
//
// TransactionExt.Bytes points to the same underlying memory
// as `bins` for performance consideration, it's not safe to modify it anywhere.
//
// Deprecated: Use ReadTransactionExtSliceFrom instead.
func (u *Util) TransactionExtSliceFromTransactionsBytes(bin []byte) (TransactionExtSlice, error) {
	r := bytes.NewReader(bin)
	scratch := u.cborHeaderBufPool.Get().(*[]byte)
	defer u.cborHeaderBufPool.Put(scratch)
	majorType, count, offset, err := cbg.CborReadHeaderBuf(r, *scratch)
	if err != nil {
		return nil, err
	} else if majorType != cbg.MajArray {
		return nil, ErrInvalidBytes
	}

	txxs := make(TransactionExtSlice, 0, count)
	for i := uint64(0); i < count; i++ {
		txx, err := u.TransactionExtFromBytes(bin[offset:])
		if err != nil {
			return txxs, err
		}
		txxs = append(txxs, txx)
		offset += len(txx.Bytes)
	}
	return txxs, nil
}

// MarshalTransactionExtSlice encodes and writes the transactions in the TransactionExtSlice to io.Writer.
func (u *Util) WriteMarshalTransactionExtSliceTo(txxs TransactionExtSlice, w io.Writer,
) (n int, err error) {
	scratch := u.cborHeaderBufPool.Get().(*[]byte)
	defer u.cborHeaderBufPool.Put(scratch)
	if n, err = cbg.WriteMajorTypeHeaderBuf(*scratch, w, cbg.MajArray, uint64(len(txxs))); err != nil {
		return n, err
	}
	for _, txx := range txxs {
		n_, err := w.Write(txx.Bytes)
		n += n_
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// HashTransaction computes the hash of the transaction.
func (u *Util) HashTransaction(tx *Transaction) (TransactionHash, error) {
	bin, err := u.Mrsh.MarshalStruct(tx)
	if err != nil {
		return nil, err
	}
	return u.Crpt.Hash(bin), nil
}

// HashTransactionNoSig computes the hash of the transaction without signature.
func (u *Util) HashTransactionNoSig(tx *Transaction) (TransactionHash, error) {
	txNoSig := getTxNoSig(tx)
	bin, err := u.Mrsh.MarshalStruct(txNoSig)
	if err != nil {
		return nil, err
	}
	return u.Crpt.Hash(bin), nil
}

// HashTransactionSlice computes the hashes of the transactions.
func (u *Util) HashTransactionSlice(txs TransactionSlice) (hs []TransactionHash, err error) {
	hs = make([]TransactionHash, len(txs))
	for i, val := range txs {
		if hs[i], err = u.HashTransaction(&val); err != nil {
			return nil, err
		}
	}
	return hs, nil
}

// VerifyTransactionSignature verifies the transaction signature.
// Should prefer using VerifyTransactionExtSignature instead for better performance.
// TODO: prepend genesis block hash to bin
func (u *Util) VerifyTransactionSignature(tx *Transaction) (bool, error) {
	sig := tx.Sig
	//fmt.Printf("sig: %0x\n", sig)
	txNoSig := getTxNoSig(tx)
	bin, err := u.Mrsh.MarshalStruct(txNoSig)
	if err != nil {
		return false, err
	}
	//fmt.Printf("bin: %0x\n", bin)

	pub, err := u.Crpt.PublicKeyFromBytes(tx.From)
	if err != nil {
		return false, err
	}
	//fmt.Printf("pub: %0x\n", pub)
	return pub.VerifyMessage(bin, sig)
}

// 0x41=64
const signatureCborDataLengthByte = byte(SignatureCborDataLength)

// VerifyTransactionExtSignature verifies the transaction signature from TransactionExt.
// TODO: Prepend genesis block hash to bin
func (u *Util) VerifyTransactionExtSignature(txx *TransactionExt) (bool, error) {
	// In CBOR-encoded Transaction bytes:
	// if the signature is set, it's encoded as a byte string with txNoSigLen 64;
	// if not, it's a byte string with txNoSigLen 0, not `null`
	txNoSigLen := len(txx.Bytes) - SignatureCborDataLength - 1
	txNoSigBytes := make([]byte, txNoSigLen)
	copy(txNoSigBytes, txx.Bytes[:txNoSigLen-1])
	txNoSigBytes[txNoSigLen-1] = signatureCborDataLengthByte

	pub, err := u.Crpt.PublicKeyFromBytes(txx.From)
	if err != nil {
		return false, err
	}
	return pub.VerifyMessage(txNoSigBytes, txx.Sig)
}

// HashBlockHeader computes the hash of the BlockHeader.
func (u *Util) HashBlockHeader(bh *BlockHeader) (BlockHash, error) {
	bin, err := u.Mrsh.MarshalStruct(bh)
	if err != nil {
		return nil, err
	}
	return u.Crpt.Hash(bin), nil
}

// ExtendBlockHeader extends a BlockHeader into a BlockHeaderExt.
//
// NOTE: ExtraUnmarshaled is not set yet.
func (u *Util) ExtendBlockHeader(bh *BlockHeader) (*BlockHeaderExt, error) {
	bin, err := u.Mrsh.MarshalStruct(bh)
	if err != nil {
		return nil, err
	}
	return &BlockHeaderExt{
		BlockHeader: bh,
		Bytes:       bin,
		Hash:        u.Crpt.Hash(bin),
	}, nil
}

// ReadBlockHeaderExtFrom reads and unmarshals the encoded block header from `r`
// and extends it into a BlockHeaderExt, it also returns the number of bytes read.
//
// NOTE: ExtraUnmarshaled is not set yet.
func (u *Util) ReadBlockHeaderExtFrom(r io.Reader) (bhx *BlockHeaderExt, n int64, err error) {
	bhx = &BlockHeaderExt{
		BlockHeader: new(BlockHeader),
	}
	var buf bytes.Buffer
	tr := io.TeeReader(r, &buf)
	n_, err := u.Mrsh.NewDecoder(tr).DecodeStruct(bhx.BlockHeader)
	n = int64(n_)
	if err != nil {
		return nil, n, err
	}
	bhx.Bytes = buf.Bytes()
	bhx.Hash = u.Crpt.Hash(bhx.Bytes)
	return bhx, n, err
}

// BlockHeaderExtFromBytes unmarshals BlockHeader and extends it into a BlockHeaderExt.
// BlockHeaderExt.Bytes points to the same underlying memory as `bins` for performance consideration,
// it's not safe to modify it anywhere.
//
// NOTE: ExtraUnmarshaled is not set yet.
//
// Deprecated: Use ReadBlockHeaderExtFrom instead.
func (u *Util) BlockHeaderExtFromBytes(bin []byte) (*BlockHeaderExt, error) {
	bhx := &BlockHeaderExt{
		BlockHeader: new(BlockHeader),
	}
	read, err := u.Mrsh.UnmarshalStruct(bin, bhx.BlockHeader)
	if err != nil {
		return nil, err
	}
	bhx.Bytes = bin[:read]
	bhx.Hash = u.Crpt.Hash(bin[:read])
	return bhx, err
}

// ExtendBlock extends a Block into a BlockExt.
func (u *Util) ExtendBlock(b *Block) (*BlockExt, error) {
	bhx, err := u.ExtendBlockHeader(b.Header)
	if err != nil {
		return nil, err
	}

	txxs, err := u.ExtendTransactionSlice(b.Txs)
	if err != nil {
		return nil, err
	}

	return &BlockExt{
		block:  b,
		util:   u,
		Header: bhx,
		Txs:    txxs,
	}, nil
}

// ReadBlockHeaderExtFromBlockStream reads and unmarshals the encoded block header from byte stream
// of a Block and unmarshals it into a BlockHeaderExt.
//
// NOTE: ExtraUnmarshaled is not set yet.
func (u *Util) ReadBlockHeaderExtFromBlockStream(r io.Reader) (bhx *BlockHeaderExt, n int64, err error) {
	if n, err = io.CopyN(ioutil.Discard, r, BlockCborInitialLength); err != nil || n != BlockHeaderCborInitialLength {
		return nil, n, err
	}
	bhx, n_, err := u.ReadBlockHeaderExtFrom(r)
	return bhx, n + n_, err
}

// ExtractBlockHeaderExtFromBlockBytes extracts bytes corresponding to the
// BlockHeader from Block bytes and unmarshals it into a BlockHeaderExt.
//
// NOTE: ExtraUnmarshaled is not set yet.
//
// Deprecated: Use ReadBlockHeaderExtFromBlockStream instead.
func (u *Util) ExtractBlockHeaderExtFromBlockBytes(bin []byte) (*BlockHeaderExt, error) {
	return u.BlockHeaderExtFromBytes(bin[BlockCborInitialLength:])
}

// ReadBlockExtFrom reads and unmarshals the encoded block from `r` and extends it into a BlockExt,
// it also returns the number of bytes read.
//
// NOTE: ExtraUnmarshaled is not set yet.
func (u *Util) ReadBlockExtFrom(r io.Reader) (bx *BlockExt, n int64, err error) {
	bx = &BlockExt{
		util: u,
	}

	bx.Header, n, err = u.ReadBlockHeaderExtFromBlockStream(r)
	if err != nil {
		return nil, n, err
	}

	var n_ int64
	bx.Txs, n_, err = u.ReadTransactionExtSliceFrom(r)
	n += n_
	if err != nil {
		return nil, n, err
	}

	return bx, n, nil
}

// BlockExtFromBytes unmarshals Block and extends it into a BlockExt.
// BlockHeaderExt.Bytes and TransactionExt.Bytes point to the same underlying memory as `bins` for
// performance consideration, it's not safe to modify them anywhere.
//
// NOTE: ExtraUnmarshaled is not set yet.
//
// Deprecated: Use ReadBlockExtFrom instead.
func (u *Util) BlockExtFromBytes(bin []byte) (bx *BlockExt, err error) {
	bx = &BlockExt{
		util: u,
	}

	bx.Header, err = u.ExtractBlockHeaderExtFromBlockBytes(bin)
	if err != nil {
		return nil, err
	}

	i := BlockCborInitialLength + len(bx.Header.Bytes)
	if i >= len(bin) {
		return nil, ErrInvalidBytes
	}
	// Check that there are transactions
	if bin[i] != cbg.CborNull[0] {
		bx.Txs, err = u.TransactionExtSliceFromTransactionsBytes(bin[i:])
		if err != nil {
			return nil, err
		}
	}

	return bx, nil
}

// GenRootHashFromTransactionHashes computes the merkle tree root hash from TransactionHashes.
func (u *Util) GenRootHashFromTransactionHashes(transactionHashes []TransactionHash) TransactionsRootHash {
	return u.Crpt.MerkleHashFromByteSlices(*(*[][]byte)(unsafe.Pointer(&transactionHashes)))
}

// GenRootHashFromTransactions computes the merkle tree root hash from TransactionSlice.
func (u *Util) GenRootHashFromTransactionSlice(txs TransactionSlice) (TransactionsRootHash, error) {
	txhs := make([][]byte, len(txs))
	var err error
	for i := range txs {
		if txhs[i], err = u.HashTransaction(&txs[i]); err != nil {
			return nil, err
		}
	}
	return u.Crpt.MerkleHashFromByteSlices(txhs), nil
}

// GenRootHashFromTransactionExtSlice computes the merkle tree root hash from TransactionExtSlice.
func (u *Util) GenRootHashFromTransactionExtSlice(txxs TransactionExtSlice) TransactionsRootHash {
	txhs := make([][]byte, len(txxs))
	for i, txx := range txxs {
		txhs[i] = txx.Hash
	}
	return u.Crpt.MerkleHashFromByteSlices(txhs)
}

// If tx contains signature, return a copy without the signature.
func getTxNoSig(tx *Transaction) (txNoSig *Transaction) {
	// Don't need to copy Transaction
	if tx.Sig == nil {
		return tx
	}

	txNoSig = new(Transaction)
	*txNoSig = *tx
	txNoSig.Sig = nil
	return txNoSig
}
