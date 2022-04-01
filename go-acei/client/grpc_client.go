package aceiclient

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/daotl/go-log/v2"
	gnet "github.com/daotl/guts/net"
	ssrv "github.com/daotl/guts/service/suture"
	gsync "github.com/daotl/guts/sync"
	"google.golang.org/grpc"

	"github.com/daotl/go-acei/types"
)

// A gRPC client.
type grpcClient struct {
	*ssrv.BaseService
	mustConnect bool

	client   types.ACEIApplicationClient
	conn     *grpc.ClientConn
	chReqRes chan *ReqRes // dispatches "async" responses to callbacks *in order*, needed by mempool

	mtx   gsync.Mutex
	addr  string
	err   error
	resCb Callback // listens to all callbacks
}

var _ Client = (*grpcClient)(nil)

// NewGRPCClient creates a gRPC client, which will connect to addr upon the
// start. Note Client#Start returns an error if connection is unsuccessful and
// mustConnect is true.
//
// GRPC calls are synchronous, but some callbacks expect to be called
// asynchronously (eg. the mempool expects to be able to lock to remove bad txs
// from cache). To accommodate, we finish each call in its own go-routine,
// which is expensive, but easy - if you want something better, use the socket
// protocol! maybe one day, if people really want it, we use grpc streams, but
// hopefully not :D
func NewGRPCClient(logger log.StandardLogger, addr string, mustConnect bool,
) (*grpcClient, error) {
	cli := &grpcClient{
		addr:        addr,
		mustConnect: mustConnect,
	}
	var err error
	cli.BaseService, err = ssrv.NewBaseService(cli.run, logger)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func dialerFunc(ctx context.Context, addr string) (net.Conn, error) {
	return gnet.Connect(addr)
}

// func (cli *grpcClient) OnStart() error {
func (cli *grpcClient) run(ctx context.Context, ready func(error)) error {
	// Reset state
	cli.client = nil
	cli.conn = nil
	// Buffering the channel is needed to make calls appear asynchronous,
	// which is required when the caller makes multiple async calls before
	// processing callbacks (e.g. due to holding locks). 64 means that a
	// caller can make up to 64 async calls before a callback must be
	// processed (otherwise it deadlocks). It also means that we can make 64
	// gRPC calls while processing a slow callback at the channel head.
	cli.chReqRes = make(chan *ReqRes, 64)
	cli.err = nil
	//cli.resCb = nil // Keep response callback

	// This processes asynchronous request/response messages and dispatches
	// them to callbacks.
	go func() {
		// Use a separate function to use defer for mutex unlocks (this handles panics)
		callCb := func(reqres *ReqRes) {
			cli.mtx.Lock()
			defer cli.mtx.Unlock()

			reqres.SetDone()
			reqres.Done()

			// Notify client listener if set
			if cli.resCb != nil {
				cli.resCb(reqres.Request, reqres.Response)
			}

			// Notify reqRes listener if set
			if cb := reqres.GetCallback(); cb != nil {
				cb(reqres.Response)
			}
		}

		for {
			select {
			case reqres := <-cli.chReqRes:
				if reqres != nil {
					callCb(reqres)
				} else {
					cli.Logger.Error("Received nil reqres")
				}
			case <-ctx.Done():
				return
			}

		}
	}()

	var (
		err  error // error to return
		conn *grpc.ClientConn
	)

RETRY_CONNECT_LOOP:
	for {
		select {
		case <-ctx.Done():
			goto CLEANUP

		default:
			conn, err = grpc.Dial(cli.addr, grpc.WithInsecure(), grpc.WithContextDialer(dialerFunc))
			// Success, break
			if err == nil {
				break RETRY_CONNECT_LOOP
			}
			// Fail, stop or retry
			if cli.mustConnect {
				ready(err)
				goto CLEANUP
			}
			cli.Logger.Error(fmt.Sprintf("abci.grpcClient failed to connect to %v.  Retrying...\n", cli.addr), "err", err)
			time.Sleep(time.Second * dialRetryIntervalSeconds)
		}
	}

	cli.Logger.Info("Dialed server. Waiting for echo.", "addr", cli.addr)
	cli.client = types.NewACEIApplicationClient(conn)
	cli.conn = conn

ENSURE_CONNECTED:
	for {
		select {
		case <-ctx.Done():
			goto CLEANUP

		default:
			_, err := cli.client.Echo(context.Background(), &types.RequestEcho{Message: "hello"}, grpc.WaitForReady(true))
			// Success, break
			if err == nil {
				break ENSURE_CONNECTED
			}
			// Fail, retry
			cli.Logger.Error("Echo failed", "err", err)
			time.Sleep(time.Second * echoRetryIntervalSeconds)
		}
	}

	ready(nil)
	// Block until stopped
	<-ctx.Done()

CLEANUP:
	// func (cli *grpcClient) OnStop() {
	if cli.conn != nil {
		cli.conn.Close()
	}
	close(cli.chReqRes)
	return err
}

func (cli *grpcClient) StopForError(cerr error) {
	stopped, err := cli.Stop()
	if err != nil {
		return
	}

	cli.mtx.Lock()
	if cli.err == nil {
		cli.err = cerr
	}
	cli.mtx.Unlock()

	cli.Logger.Error(fmt.Sprintf("Stopping abci.grpcClient for error: %v", err.Error()))
	<-stopped
}

func (cli *grpcClient) Error() error {
	cli.mtx.Lock()
	defer cli.mtx.Unlock()
	return cli.err
}

// Set listener for all responses
// NOTE: callback may get internally generated flush responses.
func (cli *grpcClient) SetResponseCallback(resCb Callback) {
	cli.mtx.Lock()
	cli.resCb = resCb
	cli.mtx.Unlock()
}

//----------------------------------------

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) EchoAsync(ctx context.Context, msg string) (*ReqRes, error) {
	req := types.ToRequestEcho(msg)
	res, err := cli.client.Echo(ctx, req.GetEcho(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_Echo{Echo: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) FlushAsync(ctx context.Context) (*ReqRes, error) {
	req := types.ToRequestFlush()
	res, err := cli.client.Flush(ctx, req.GetFlush(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_Flush{Flush: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) InfoAsync(ctx context.Context, params types.RequestInfo) (*ReqRes, error) {
	req := types.ToRequestInfo(params)
	res, err := cli.client.Info(ctx, req.GetInfo(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_Info{Info: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) DeliverTxAsync(ctx context.Context, params types.RequestDeliverTx) (*ReqRes, error) {
	req := types.ToRequestDeliverTx(params)
	res, err := cli.client.DeliverTx(ctx, req.GetDeliverTx(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_DeliverTx{DeliverTx: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) CheckTxAsync(ctx context.Context, params types.RequestCheckTx) (*ReqRes, error) {
	req := types.ToRequestCheckTx(params)
	res, err := cli.client.CheckTx(ctx, req.GetCheckTx(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_CheckTx{CheckTx: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) QueryAsync(ctx context.Context, params types.RequestQuery) (*ReqRes, error) {
	req := types.ToRequestQuery(params)
	res, err := cli.client.Query(ctx, req.GetQuery(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_Query{Query: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) CommitAsync(ctx context.Context) (*ReqRes, error) {
	req := types.ToRequestCommit()
	res, err := cli.client.Commit(ctx, req.GetCommit(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_Commit{Commit: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) InitLedgerAsync(ctx context.Context, params types.RequestInitLedger) (*ReqRes, error) {
	req := types.ToRequestInitLedger(params)
	res, err := cli.client.InitLedger(ctx, req.GetInitLedger(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_InitLedger{InitLedger: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) BeginBlockAsync(ctx context.Context, params types.RequestBeginBlock) (*ReqRes, error) {
	req := types.ToRequestBeginBlock(params)
	res, err := cli.client.BeginBlock(ctx, req.GetBeginBlock(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_BeginBlock{BeginBlock: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) EndBlockAsync(ctx context.Context, params types.RequestEndBlock) (*ReqRes, error) {
	req := types.ToRequestEndBlock(params)
	res, err := cli.client.EndBlock(ctx, req.GetEndBlock(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_EndBlock{EndBlock: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) ListSnapshotsAsync(ctx context.Context, params types.RequestListSnapshots) (*ReqRes, error) {
	req := types.ToRequestListSnapshots(params)
	res, err := cli.client.ListSnapshots(ctx, req.GetListSnapshots(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_ListSnapshots{ListSnapshots: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) OfferSnapshotAsync(ctx context.Context, params types.RequestOfferSnapshot) (*ReqRes, error) {
	req := types.ToRequestOfferSnapshot(params)
	res, err := cli.client.OfferSnapshot(ctx, req.GetOfferSnapshot(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_OfferSnapshot{OfferSnapshot: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) LoadSnapshotChunkAsync(
	ctx context.Context,
	params types.RequestLoadSnapshotChunk,
) (*ReqRes, error) {
	req := types.ToRequestLoadSnapshotChunk(params)
	res, err := cli.client.LoadSnapshotChunk(ctx, req.GetLoadSnapshotChunk(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(ctx, req, &types.Response{Value: &types.Response_LoadSnapshotChunk{LoadSnapshotChunk: res}})
}

// NOTE: call is synchronous, use ctx to break early if needed
func (cli *grpcClient) ApplySnapshotChunkAsync(
	ctx context.Context,
	params types.RequestApplySnapshotChunk,
) (*ReqRes, error) {
	req := types.ToRequestApplySnapshotChunk(params)
	res, err := cli.client.ApplySnapshotChunk(ctx, req.GetApplySnapshotChunk(), grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	return cli.finishAsyncCall(
		ctx,
		req,
		&types.Response{Value: &types.Response_ApplySnapshotChunk{ApplySnapshotChunk: res}},
	)
}

// finishAsyncCall creates a ReqRes for an async call, and immediately populates it
// with the response. We don't complete it until it's been ordered via the channel.
func (cli *grpcClient) finishAsyncCall(ctx context.Context, req *types.Request, res *types.Response) (*ReqRes, error) {
	reqres := NewReqRes(req)
	reqres.Response = res
	select {
	case cli.chReqRes <- reqres: // use channel for async responses, since they must be ordered
		return reqres, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// finishSyncCall waits for an async call to complete. It is necessary to call all
// sync calls asynchronously as well, to maintain call and response ordering via
// the channel, and this method will wait until the async call completes.
func (cli *grpcClient) finishSyncCall(reqres *ReqRes) *types.Response {
	// It's possible that the callback is called twice, since the callback can
	// be called immediately on SetCallback() in addition to after it has been
	// set. This is because completing the ReqRes happens in a separate critical
	// section from the one where the callback is called: there is a race where
	// SetCallback() is called between completing the ReqRes and dispatching the
	// callback.
	//
	// We also buffer the channel with 1 response, since SetCallback() will be
	// called synchronously if the reqres is already completed, in which case
	// it will block on sending to the channel since it hasn't gotten around to
	// receiving from it yet.
	//
	// ReqRes should really handle callback dispatch internally, to guarantee
	// that it's only called once and avoid the above race conditions.
	var once sync.Once
	ch := make(chan *types.Response, 1)
	reqres.SetCallback(func(res *types.Response) {
		once.Do(func() {
			ch <- res
		})
	})
	return <-ch
}

//----------------------------------------

func (cli *grpcClient) FlushSync(ctx context.Context) error {
	return nil
}

func (cli *grpcClient) EchoSync(ctx context.Context, msg string) (*types.ResponseEcho, error) {
	reqres, err := cli.EchoAsync(ctx, msg)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetEcho(), cli.Error()
}

func (cli *grpcClient) InfoSync(
	ctx context.Context,
	req types.RequestInfo,
) (*types.ResponseInfo, error) {
	reqres, err := cli.InfoAsync(ctx, req)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetInfo(), cli.Error()
}

func (cli *grpcClient) DeliverTxSync(
	ctx context.Context,
	params types.RequestDeliverTx,
) (*types.ResponseDeliverTx, error) {

	reqres, err := cli.DeliverTxAsync(ctx, params)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetDeliverTx(), cli.Error()
}

func (cli *grpcClient) CheckTxSync(
	ctx context.Context,
	params types.RequestCheckTx,
) (*types.ResponseCheckTx, error) {

	reqres, err := cli.CheckTxAsync(ctx, params)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetCheckTx(), cli.Error()
}

func (cli *grpcClient) QuerySync(
	ctx context.Context,
	req types.RequestQuery,
) (*types.ResponseQuery, error) {
	reqres, err := cli.QueryAsync(ctx, req)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetQuery(), cli.Error()
}

func (cli *grpcClient) CommitSync(ctx context.Context) (*types.ResponseCommit, error) {
	reqres, err := cli.CommitAsync(ctx)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetCommit(), cli.Error()
}

func (cli *grpcClient) InitLedgerSync(
	ctx context.Context,
	params types.RequestInitLedger,
) (*types.ResponseInitLedger, error) {

	reqres, err := cli.InitLedgerAsync(ctx, params)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetInitLedger(), cli.Error()
}

func (cli *grpcClient) BeginBlockSync(
	ctx context.Context,
	params types.RequestBeginBlock,
) (*types.ResponseBeginBlock, error) {

	reqres, err := cli.BeginBlockAsync(ctx, params)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetBeginBlock(), cli.Error()
}

func (cli *grpcClient) EndBlockSync(
	ctx context.Context,
	params types.RequestEndBlock,
) (*types.ResponseEndBlock, error) {

	reqres, err := cli.EndBlockAsync(ctx, params)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetEndBlock(), cli.Error()
}

func (cli *grpcClient) ListSnapshotsSync(
	ctx context.Context,
	params types.RequestListSnapshots,
) (*types.ResponseListSnapshots, error) {

	reqres, err := cli.ListSnapshotsAsync(ctx, params)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetListSnapshots(), cli.Error()
}

func (cli *grpcClient) OfferSnapshotSync(
	ctx context.Context,
	params types.RequestOfferSnapshot,
) (*types.ResponseOfferSnapshot, error) {

	reqres, err := cli.OfferSnapshotAsync(ctx, params)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetOfferSnapshot(), cli.Error()
}

func (cli *grpcClient) LoadSnapshotChunkSync(
	ctx context.Context,
	params types.RequestLoadSnapshotChunk) (*types.ResponseLoadSnapshotChunk, error) {

	reqres, err := cli.LoadSnapshotChunkAsync(ctx, params)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetLoadSnapshotChunk(), cli.Error()
}

func (cli *grpcClient) ApplySnapshotChunkSync(
	ctx context.Context,
	params types.RequestApplySnapshotChunk) (*types.ResponseApplySnapshotChunk, error) {

	reqres, err := cli.ApplySnapshotChunkAsync(ctx, params)
	if err != nil {
		return nil, err
	}
	return cli.finishSyncCall(reqres).GetApplySnapshotChunk(), cli.Error()
}
