package aceiclient

import (
	"context"
	"fmt"
	"sync"

	"github.com/daotl/go-log/v2"
	ssrv "github.com/daotl/guts/service/suture"
	gsync "github.com/daotl/guts/sync"

	"github.com/daotl/go-acei/types"
	"github.com/daotl/go-acei/types/local"
)

const (
	dialRetryIntervalSeconds = 3
	echoRetryIntervalSeconds = 1
)

//go:generate ../scripts/mockery_generate.sh Client

// Client defines an interface for an ACEI client.
//
// All `Async` methods return a `ReqRes` object and an error.
// All `Sync` methods return the appropriate protobuf ResponseXxx struct and an error.
//
// NOTE these are client errors, eg. ACEI socket connectivity issues.
// Application-related errors are reflected in response via ACEI error codes
// and logs.
type Client interface {
	ssrv.Service

	SetResponseCallback(Callback)
	Error() error

	// Asynchronous requests
	FlushAsync(context.Context) (*ReqRes, error)
	EchoAsync(ctx context.Context, msg string) (*ReqRes, error)
	InfoAsync(context.Context, types.RequestInfo) (*ReqRes, error)
	DeliverTxAsync(context.Context, types.RequestDeliverTx) (*ReqRes, error)
	CheckTxAsync(context.Context, types.RequestCheckTx) (*ReqRes, error)
	QueryAsync(context.Context, types.RequestQuery) (*ReqRes, error)
	CommitAsync(context.Context) (*ReqRes, error)
	InitLedgerAsync(context.Context, types.RequestInitLedger) (*ReqRes, error)
	BeginBlockAsync(context.Context, types.RequestBeginBlock) (*ReqRes, error)
	EndBlockAsync(context.Context, types.RequestEndBlock) (*ReqRes, error)
	ListSnapshotsAsync(context.Context, types.RequestListSnapshots) (*ReqRes, error)
	OfferSnapshotAsync(context.Context, types.RequestOfferSnapshot) (*ReqRes, error)
	LoadSnapshotChunkAsync(context.Context, types.RequestLoadSnapshotChunk) (*ReqRes, error)
	ApplySnapshotChunkAsync(context.Context, types.RequestApplySnapshotChunk) (*ReqRes, error)

	// Synchronous requests
	FlushSync(context.Context) error
	EchoSync(ctx context.Context, msg string) (*types.ResponseEcho, error)
	InfoSync(context.Context, types.RequestInfo) (*types.ResponseInfo, error)
	DeliverTxSync(context.Context, types.RequestDeliverTx) (*types.ResponseDeliverTx, error)
	CheckTxSync(context.Context, types.RequestCheckTx) (*types.ResponseCheckTx, error)
	QuerySync(context.Context, types.RequestQuery) (*types.ResponseQuery, error)
	CommitSync(context.Context) (*types.ResponseCommit, error)
	InitLedgerSync(context.Context, types.RequestInitLedger) (*types.ResponseInitLedger, error)
	BeginBlockSync(context.Context, types.RequestBeginBlock) (*types.ResponseBeginBlock, error)
	EndBlockSync(context.Context, types.RequestEndBlock) (*types.ResponseEndBlock, error)
	ListSnapshotsSync(context.Context, types.RequestListSnapshots) (*types.ResponseListSnapshots, error)
	OfferSnapshotSync(context.Context, types.RequestOfferSnapshot) (*types.ResponseOfferSnapshot, error)
	LoadSnapshotChunkSync(context.Context, types.RequestLoadSnapshotChunk) (*types.ResponseLoadSnapshotChunk, error)
	ApplySnapshotChunkSync(context.Context, types.RequestApplySnapshotChunk) (*types.ResponseApplySnapshotChunk, error)
}

// LocalClient interface is a process-local version of the Client interface.
//
// All methods take a pointer argument and return a pointer to avoid copying, some
// methods also take a RequestNativeXxx argument and/or return a ResponseNativeXxx
// argument, which directly use Go-native types form go-doubl instead of corresponding
// Protocol Buffers types.
//
// Application-related errors are reflected in response via ACEI error codes
// and logs.
type LocalClient interface {
	Error() error

	Flush(context.Context) error
	Echo(ctx context.Context, msg string) (*types.ResponseEcho, error)
	Info(context.Context, *types.RequestInfo) (*types.ResponseInfo, error)
	DeliverTx(context.Context, *local.RequestNativeDeliverTx) (*types.ResponseDeliverTx, error)
	CheckTx(context.Context, *local.RequestNativeCheckTx) (*local.ResponseNativeCheckTx, error)
	Query(context.Context, *types.RequestQuery) (*types.ResponseQuery, error)
	Commit(context.Context) (*types.ResponseCommit, error)
	InitLedger(context.Context, *local.RequestNativeInitLedger) (*local.ResponseNativeInitLedger, error)
	BeginBlock(context.Context, *local.RequestNativeBeginBlock) (*types.ResponseBeginBlock, error)
	EndBlock(context.Context, *types.RequestEndBlock) (*local.ResponseNativeEndBlock, error)
	ListSnapshots(context.Context, *types.RequestListSnapshots) (*types.ResponseListSnapshots, error)
	OfferSnapshot(context.Context, *types.RequestOfferSnapshot) (*types.ResponseOfferSnapshot, error)
	LoadSnapshotChunk(context.Context, *types.RequestLoadSnapshotChunk) (*types.ResponseLoadSnapshotChunk, error)
	ApplySnapshotChunk(context.Context, *types.RequestApplySnapshotChunk) (*types.ResponseApplySnapshotChunk, error)
}

//----------------------------------------

// NewClient returns a new ACEI client of the specified transport type.
// It returns an error if the transport is not "socket" or "grpc"
func NewClient(logger log.StandardLogger, addr, transport string, mustConnect bool,
) (client Client, err error) {
	switch transport {
	case "socket":
		client, err = NewSocketClient(logger, addr, mustConnect)
	case "grpc":
		client, err = NewGRPCClient(logger, addr, mustConnect)
	default:
		err = fmt.Errorf("unknown abci transport %s", transport)
	}
	return
}

type Callback func(*types.Request, *types.Response)

type ReqRes struct {
	*types.Request
	*sync.WaitGroup
	*types.Response // Not set atomically, so be sure to use WaitGroup.

	mtx  gsync.Mutex
	done bool                  // Gets set to true once *after* WaitGroup.Done().
	cb   func(*types.Response) // A single callback that may be set.
}

func NewReqRes(req *types.Request) *ReqRes {
	return &ReqRes{
		Request:   req,
		WaitGroup: waitGroup1(),
		Response:  nil,

		done: false,
		cb:   nil,
	}
}

// Sets sets the callback. If reqRes is already done, it will call the cb
// immediately. Note, reqRes.cb should not change if reqRes.done and only one
// callback is supported.
func (r *ReqRes) SetCallback(cb func(res *types.Response)) {
	r.mtx.Lock()

	if r.done {
		r.mtx.Unlock()
		cb(r.Response)
		return
	}

	r.cb = cb
	r.mtx.Unlock()
}

// InvokeCallback invokes a thread-safe execution of the configured callback
// if non-nil.
func (r *ReqRes) InvokeCallback() {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.cb != nil {
		r.cb(r.Response)
	}
}

// GetCallback returns the configured callback of the ReqRes object which may be
// nil. Note, it is not safe to concurrently call this in cases where it is
// marked done and SetCallback is called before calling GetCallback as that
// will invoke the callback twice and create a potential race condition.
//
// ref: https://github.com/tendermint/tendermint/issues/5439
func (r *ReqRes) GetCallback() func(*types.Response) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.cb
}

// SetDone marks the ReqRes object as done.
func (r *ReqRes) SetDone() {
	r.mtx.Lock()
	r.done = true
	r.mtx.Unlock()
}

func waitGroup1() (wg *sync.WaitGroup) {
	wg = &sync.WaitGroup{}
	wg.Add(1)
	return
}
