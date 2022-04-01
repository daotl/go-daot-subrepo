package local

import (
	t "github.com/daotl/go-acei/types"
)

// Application interface is a process-local version of the types.Application interface whose
// methods take a pointer argument and return a pointer to avoid copying, some methods also
// take a RequestNativeXxx argument and/or return a ResponseNativeXxx argument, which directly
// use Go-native types form go-doubl instead of corresponding Protocol Buffers types.
type Application interface {
	// Info/Query Connection
	Info(*t.RequestInfo) *t.ResponseInfo    // Return application info
	Query(*t.RequestQuery) *t.ResponseQuery // Query for state

	// Mempool Connection
	CheckTx(*RequestNativeCheckTx) *ResponseNativeCheckTx // Validate a tx for the mempool

	// Consensus Connection
	InitLedger(*RequestNativeInitLedger) *ResponseNativeInitLedger // Initialize distributed ledger w/ validators/other info from the consensus engine
	BeginBlock(*RequestNativeBeginBlock) *t.ResponseBeginBlock     // Signals the beginning of a block
	DeliverTx(*RequestNativeDeliverTx) *t.ResponseDeliverTx        // Deliver a tx for full processing
	EndBlock(*t.RequestEndBlock) *ResponseNativeEndBlock           // Signals the end of a block, returns changes to the validator set
	Commit() *t.ResponseCommit                                     // Commit the state and return the application Merkle root hash

	// State Sync Connection
	ListSnapshots(*t.RequestListSnapshots) *t.ResponseListSnapshots                // List available snapshots
	OfferSnapshot(*t.RequestOfferSnapshot) *t.ResponseOfferSnapshot                // Offer a snapshot to the application
	LoadSnapshotChunk(*t.RequestLoadSnapshotChunk) *t.ResponseLoadSnapshotChunk    // Load a snapshot chunk
	ApplySnapshotChunk(*t.RequestApplySnapshotChunk) *t.ResponseApplySnapshotChunk // Apply a shapshot chunk
}

//-------------------------------------------------------
// BaseApplication is a base form of Application

var _ Application = (*BaseApplication)(nil)

type BaseApplication struct {
}

func NewBaseApplication() *BaseApplication {
	return &BaseApplication{}
}

func (BaseApplication) Info(req *t.RequestInfo) *t.ResponseInfo {
	return &t.ResponseInfo{}
}

func (BaseApplication) DeliverTx(req *RequestNativeDeliverTx) *t.ResponseDeliverTx {
	return &t.ResponseDeliverTx{Code: t.CodeTypeOK}
}

func (BaseApplication) CheckTx(req *RequestNativeCheckTx) *ResponseNativeCheckTx {
	return &ResponseNativeCheckTx{Code: t.CodeTypeOK}
}

func (BaseApplication) Commit() *t.ResponseCommit {
	return &t.ResponseCommit{}
}

func (BaseApplication) Query(req *t.RequestQuery) *t.ResponseQuery {
	return &t.ResponseQuery{Code: t.CodeTypeOK}
}

func (BaseApplication) InitLedger(req *RequestNativeInitLedger) *ResponseNativeInitLedger {
	return &ResponseNativeInitLedger{}
}

func (BaseApplication) BeginBlock(req *RequestNativeBeginBlock) *t.ResponseBeginBlock {
	return &t.ResponseBeginBlock{}
}

func (BaseApplication) EndBlock(req *t.RequestEndBlock) *ResponseNativeEndBlock {
	return &ResponseNativeEndBlock{}
}

func (BaseApplication) ListSnapshots(req *t.RequestListSnapshots) *t.ResponseListSnapshots {
	return &t.ResponseListSnapshots{}
}

func (BaseApplication) OfferSnapshot(req *t.RequestOfferSnapshot) *t.ResponseOfferSnapshot {
	return &t.ResponseOfferSnapshot{}
}

func (BaseApplication) LoadSnapshotChunk(req *t.RequestLoadSnapshotChunk) *t.ResponseLoadSnapshotChunk {
	return &t.ResponseLoadSnapshotChunk{}
}

func (BaseApplication) ApplySnapshotChunk(
	req *t.RequestApplySnapshotChunk) *t.ResponseApplySnapshotChunk {
	return &t.ResponseApplySnapshotChunk{}
}
