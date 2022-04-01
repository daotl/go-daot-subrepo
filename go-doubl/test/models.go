//go:generate go run github.com/daotl/go-doubl/test/cborgen

package test

import (
	crand "crypto/rand"
	"math/rand"
	"strconv"
	"time"

	"github.com/crpt/go-crpt"
	"github.com/daotl/go-marsha"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	m "github.com/daotl/go-doubl/model"
)

func init() {
	SetUtil(Util)
	GenTestModels()
}

type TransactionNoSig struct {
	Type  m.TransactionType
	From  m.Address
	Nonce uint64
	To    m.Address
	Data  []byte
	Extra []byte
}

func (t TransactionNoSig) Ptr() marsha.StructPtr { return &t }
func (p *TransactionNoSig) Val() marsha.Struct   { return *p }

var (
	ut *m.Util

	TestPublicKey        crpt.PublicKey
	TestPrivateKey       crpt.PrivateKey
	TestAddress          crpt.Address
	TestSignature        m.Signature
	TestTransaction      m.Transaction
	TestTransactionHash  m.TransactionHash
	TestTransactionSlice m.TransactionSlice
	TestBlockHeader      m.BlockHeader
	TestBlock            m.Block

	TestHash = m.Hash32{
		0x1, 0x2, 0x3, 0x4, 0x1, 0x2, 0x3, 0x4, 0x1, 0x2, 0x3, 0x4, 0x1, 0x2, 0x3, 0x4,
		0x1, 0x2, 0x3, 0x4, 0x1, 0x2, 0x3, 0x4, 0x1, 0x2, 0x3, 0x4, 0x1, 0x2, 0x3, 0x4,
	}
	TestHash2 = m.Hash32{
		0x5, 0x6, 0x7, 0x8, 0x5, 0x6, 0x7, 0x8, 0x5, 0x6, 0x7, 0x8, 0x5, 0x6, 0x7, 0x8,
		0x5, 0x6, 0x7, 0x8, 0x5, 0x6, 0x7, 0x8, 0x5, 0x6, 0x7, 0x8, 0x5, 0x6, 0x7, 0x8,
	}
	TestBlockHash   = TestHash
	TestBlockHashes = make([]m.BlockHash, 1)
	TestAddress2    = m.Address(TestHash)
)

func SetUtil(u *m.Util) {
	ut = u
}

// Gen test models
func GenTestModels() {
	TestPublicKey, TestPrivateKey, _ = ut.Crpt.GenerateKey(nil)
	TestAddress = TestPublicKey.Address()
	TestTransaction = m.Transaction{
		Type:  4,
		Nonce: 52,
		From:  TestAddress,
		To:    TestAddress2,
		Data:  []byte{0x4, 0x13, 0x52},
		Extra: []byte{0x21, 0x9, 0x09},
		Sig:   nil,
	}

	bin, err := ut.Mrsh.MarshalStruct(&TestTransaction)
	if err != nil {
		panic(err)
	}

	TestTransaction.Sig, err = TestPrivateKey.SignMessage(bin, nil)
	if err != nil {
		panic(err)
	}

	TestTransactionHash, err = ut.HashTransaction(&TestTransaction)
	if err != nil {
		panic(err)
	}

	TestTransactionSlice = m.TransactionSlice{TestTransaction, TestTransaction}

	TestBlockHeader = *GenTestBlockHeaderWithExtra([]byte{0x4, 0x13, 0x52})

	TestBlock = m.Block{
		Header: &TestBlockHeader,
		Txs:    TestTransactionSlice,
	}
}

// GenRandomTransaction generates a random Transaction for test.
// NOTE: Sig is not a valid signature.
func GenRandomTransaction() *m.Transaction {
	return &m.Transaction{
		Type:  4,
		From:  TestAddress,
		Nonce: 52,
		To:    TestAddress2,
		Data:  []byte(strconv.FormatInt(rand.Int63(), 10)),
		Extra: []byte(strconv.FormatInt(rand.Int63(), 10)),
		Sig:   TestSignature,
	}
}

// GenRandomTransactionExt generates a random TransactionExt for test.
func GenRandomTransactionExt() *m.TransactionExt {
	tx := GenRandomTransaction()
	txx, err := ut.ExtendTransaction(tx)
	if err != nil {
		panic(err)
	}
	return txx
}

// GenRandomTransactionExt generates random TransactionExtSlice for test.
func GenRandomTransactionExtSlice(min, max int) m.TransactionExtSlice {
	min, max = FixCountRange(min, max)
	n := min + rand.Int()%(max-min+1)
	txxs := make(m.TransactionExtSlice, n)
	for i := 0; i < n; i++ {
		tx := GenRandomTransactionExt()
		txxs = append(txxs, tx)
	}
	return txxs
}

// GenTestBlockHeaderWithExtra creates a BlockHeader for test with given Extra data.
func GenTestBlockHeaderWithExtra(extraBytes []byte) *m.BlockHeader {
	return &m.BlockHeader{
		Creator:    TestAddress,
		Time:       1525392000,
		PrevHashes: []m.BlockHash{TestHash},
		Height:     52,
		TxRoot:     TestHash,
		TxCount:    52,
		Extra:      extraBytes,
		Sig:        TestSignature,
	}
}

// GenRandomBlockHeader creates a random BlockHeader for test with the given number of previous
// block hashes and Extra data.
func GenRandomBlockHeader(prevHashCount int, extraBytes []byte) *m.BlockHeader {
	bh := GenTestBlockHeaderWithExtra(extraBytes)
	bh.PrevHashes = make([]m.BlockHash, prevHashCount)
	for i := 0; i < prevHashCount; i++ {
		bh.PrevHashes[i] = GenRandomHash()
	}
	return bh
}

// GenRandomBlockHeaders creates random BlockHeaderExts for test with the given number of previous
// block hashes and Extra data.
func GenRandomBlockHeaderExts(min, max, prevHashCount int, extraBytes []byte) []*m.BlockHeaderExt {
	min, max = FixCountRange(min, max)
	n := min + rand.Int()%(max-min+1)
	bhxs := make([]*m.BlockHeaderExt, 0, n)
	for i := 0; i < n; i++ {
		bh := GenRandomBlockHeader(prevHashCount, extraBytes)
		bx, err := Util.ExtendBlockHeader(bh)
		if err != nil {
			panic(err)
		}
		bhxs = append(bhxs, bx)
	}
	return bhxs
}

// GenRandomBlock creates a random Block for test with the given number of previous block hashes,
// Extra data and Transactions.
func GenRandomBlock(prevHashCount int, extraBytes []byte, txCount int) *m.BlockExt {
	bh := GenRandomBlockHeader(prevHashCount, extraBytes)
	bh.TxCount = uint64(txCount)
	txs := make([]m.Transaction, txCount)
	for i := 0; i < txCount; i++ {
		txs[i] = *GenRandomTransaction()
	}
	var err error
	if bh.TxRoot, err = Util.GenRootHashFromTransactionSlice(txs); err != nil {
		panic(err)
	}

	bx, err := Util.ExtendBlock(&m.Block{Header: bh, Txs: txs})
	if err != nil {
		panic(err)
	}
	return bx
}

// GenRandomBlocks creates random Blocks for test with the given number of previous block hashes,
// Extra data and Transactions.
func GenRandomBlocks(min, max, prevHashCount int, extraBytes []byte, txCount int) []*m.BlockExt {
	min, max = FixCountRange(min, max)
	n := min + rand.Int()%(max-min+1)
	bxs := make([]*m.BlockExt, 0, n)
	for i := 0; i < n; i++ {
		bxs = append(bxs, GenRandomBlock(prevHashCount, extraBytes, txCount))
	}
	return bxs
}

// GenRandomHash generates a random hash for test.
func GenRandomHash() m.Hash32 {
	a := [m.HashSize]byte{}
	crand.Read(a[:])
	return a[:]
}

// GenRandomHash generates random hashes for test.
func GenRandomHashes() []m.Hash32 {
	rand.Seed(time.Now().UnixNano())
	cnt := rand.Intn(100)
	hs := make([]m.Hash32, cnt)
	for i := 0; i < cnt; i++ {
		hs[i] = GenRandomHash()
	}
	return hs
}

// GenRandomCid generates a random CID for test.
func GenRandomCid() cid.Cid {
	rh := GenRandomHash()
	mhbytes, _ := mh.Encode(rh, mh.SHA3_256)
	return cid.NewCidV1(cid.DagCBOR, mhbytes)
}

/* Helpers */

func FixCountRange(min, max int) (int, int) {
	switch {
	case max < 0:
		max = 0
		fallthrough
	case min < 0:
		min = 0
		fallthrough
	case max < min:
		min = max
	}
	return min, max
}
