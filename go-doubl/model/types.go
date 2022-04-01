package model

import (
	"encoding/binary"

	"github.com/crpt/go-crpt"
)

const (
	HashSize      = 32
	AddressSize   = 32
	SignatureSize = 64
)

// Unix time (the number of seconds that have elapsed since the Unix epoch, minus leap seconds)
type Timestamp uint64

// 32-byte account address
type Address = crpt.Address

//  64-byte signature
type Signature = crpt.Signature

// 32-byte hash
type Hash32 = []byte

// Transaction type (0~255)
type TransactionType uint8

// 32-byte transaction hash
type TransactionHash = Hash32

// 32-byte root hash of some kind of hash-based tree (e.g., Merkle tree) of transactions
type TransactionsRootHash = Hash32

type BlockHeight uint64

// 32-byte block hash
type BlockHash = Hash32

// Encodes a block height as big endian uint64, so that it can be used as storage key and retain the order.
// From: https://github.com/ethereum/go-ethereum/blob/72c2c0ae7e2332b08d3e1ebfe5f850a92e26e8a1/core/rawdb/schema.go#L142
func (n BlockHeight) Encode() []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, uint64(n))
	return enc
}

// DecodeBlockHeight decodes block number from big endian encoded bytes.
func DecodeBlockHeight(bin []byte) BlockHeight {
	return BlockHeight(binary.BigEndian.Uint64(bin))
}

// LedgerID is the hash of the transaction that created the ledger.
type LedgerID = TransactionHash
