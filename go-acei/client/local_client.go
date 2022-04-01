package aceiclient

import (
	"context"

	"github.com/daotl/go-log/v2"
	ssrv "github.com/daotl/guts/service/suture"
	gsync "github.com/daotl/guts/sync"

	"github.com/daotl/go-acei/types"
	"github.com/daotl/go-acei/types/local"
)

// NOTE: use defer to unlock mutex because Application might panic (e.g., in
// case of malicious tx or query). It only makes sense for publicly exposed
// methods like CheckTx (/broadcast_tx_* RPC endpoint) or Query (/abci_query
// RPC endpoint), but defers are used everywhere for the sake of consistency.
type localClient struct {
	*ssrv.BaseService

	mtx *gsync.Mutex
	local.Application
}

var _ LocalClient = (*localClient)(nil)

// NewLocalClient creates a local client, which will be directly calling the
// methods of the given app.
//
// Both Async and Sync methods ignore the given context.Context parameter.
func NewLocalClient(logger log.StandardLogger, mtx *gsync.Mutex, app local.Application,
) (*localClient, error) {
	if mtx == nil {
		mtx = new(gsync.Mutex)
	}
	cli := &localClient{
		mtx:         mtx,
		Application: app,
	}
	var err error
	cli.BaseService, err = ssrv.NewBaseService(cli.run, logger)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

// Just a placeholder: localClient won't actually be used as a Suture service
func (cli *localClient) run(ctx context.Context, ready func(error)) error {
	ready(nil)
	<-ctx.Done()
	return nil
}

// TODO: change types.Application to include Error()?
func (app *localClient) Error() error {
	return nil
}

func (app *localClient) Flush(ctx context.Context) error {
	return nil
}

func (app *localClient) Echo(ctx context.Context, msg string) (*types.ResponseEcho, error) {
	return &types.ResponseEcho{Message: msg}, nil
}

func (app *localClient) Info(ctx context.Context, req *types.RequestInfo) (*types.ResponseInfo, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.Info(req), nil
}

func (app *localClient) DeliverTx(ctx context.Context, req *local.RequestNativeDeliverTx,
) (*types.ResponseDeliverTx, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.DeliverTx(req), nil
}

func (app *localClient) CheckTx(ctx context.Context, req *local.RequestNativeCheckTx,
) (*local.ResponseNativeCheckTx, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.CheckTx(req), nil
}

func (app *localClient) Query(ctx context.Context, req *types.RequestQuery,
) (*types.ResponseQuery, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.Query(req), nil
}

func (app *localClient) Commit(ctx context.Context) (*types.ResponseCommit, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.Commit(), nil
}

func (app *localClient) InitLedger(ctx context.Context, req *local.RequestNativeInitLedger,
) (*local.ResponseNativeInitLedger, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.InitLedger(req), nil
}

func (app *localClient) BeginBlock(ctx context.Context, req *local.RequestNativeBeginBlock,
) (*types.ResponseBeginBlock, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.BeginBlock(req), nil
}

func (app *localClient) EndBlock(ctx context.Context, req *types.RequestEndBlock,
) (*local.ResponseNativeEndBlock, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.EndBlock(req), nil
}

func (app *localClient) ListSnapshots(ctx context.Context, req *types.RequestListSnapshots,
) (*types.ResponseListSnapshots, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.ListSnapshots(req), nil
}

func (app *localClient) OfferSnapshot(ctx context.Context, req *types.RequestOfferSnapshot,
) (*types.ResponseOfferSnapshot, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.OfferSnapshot(req), nil
}

func (app *localClient) LoadSnapshotChunk(ctx context.Context, req *types.RequestLoadSnapshotChunk,
) (*types.ResponseLoadSnapshotChunk, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.LoadSnapshotChunk(req), nil
}

func (app *localClient) ApplySnapshotChunk(ctx context.Context, req *types.RequestApplySnapshotChunk,
) (*types.ResponseApplySnapshotChunk, error) {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	return app.Application.ApplySnapshotChunk(req), nil
}
