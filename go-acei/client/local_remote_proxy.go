package aceiclient

import (
	"context"
	"encoding/hex"

	"github.com/daotl/go-doubl/model"
	"github.com/daotl/go-log/v2"
	"github.com/daotl/go-marsha"
	ssrv "github.com/daotl/guts/service/suture"

	"github.com/daotl/go-acei/types"
	"github.com/daotl/go-acei/types/local"
)

type ExtraCtor = model.ExtraCtor[model.ExtraPtr]

// localToRemoteProxy is a LocalClient that proxies all requests to a remote Client.
type localToRemoteProxy struct {
	*ssrv.BaseService
	*localClient

	mrsh                        marsha.Marsha
	client                      Client
	blockExtraCtor, txExtraCtor ExtraCtor
}

var _ LocalClient = (*localToRemoteProxy)(nil)

// NewLocalToRemoteProxy creates a localToRemoteProxy, which will proxies all requests to `remote`.
func NewLocalToRemoteProxy(logger log.StandardLogger, mrsh marsha.Marsha, remote Client, blockExtraCtor, txExtraCtor ExtraCtor,
) (*localToRemoteProxy, error) {
	cli := &localToRemoteProxy{
		mrsh:           mrsh,
		client:         remote,
		blockExtraCtor: blockExtraCtor,
		txExtraCtor:    txExtraCtor,
	}
	var err error
	cli.BaseService, err = ssrv.NewBaseService(cli.run, logger)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func (app *localToRemoteProxy) Info(ctx context.Context, req *types.RequestInfo) (*types.ResponseInfo, error) {
	return app.client.InfoSync(ctx, *req)
}

func (app *localToRemoteProxy) Query(ctx context.Context, req *types.RequestQuery,
) (*types.ResponseQuery, error) {
	return app.client.QuerySync(ctx, *req)
}

func (app *localToRemoteProxy) InitLedger(ctx context.Context, req *local.RequestNativeInitLedger,
) (*local.ResponseNativeInitLedger, error) {
	bin, err := app.mrsh.MarshalStruct(req.Extra)
	if err != nil {
		return nil, err
	}
	res, err := app.client.InitLedgerSync(ctx, types.RequestInitLedger{
		Time:          req.Time,
		LedgerId:      hex.EncodeToString(req.LedgerId),
		AppStateBytes: req.AppStateBytes,
		InitialHeight: req.InitialHeight,
		Extra:         bin,
	})
	te := app.txExtraCtor()
	if _, err := app.mrsh.UnmarshalStruct(res.Extra, te); err != nil {
		return nil, err
	}
	return &local.ResponseNativeInitLedger{
		AppHash: res.AppHash,
		Extra:   te,
	}, nil
}

func (app *localToRemoteProxy) CheckTx(ctx context.Context, req *local.RequestNativeCheckTx,
) (*local.ResponseNativeCheckTx, error) {
	res, err := app.client.CheckTxSync(ctx, types.RequestCheckTx{Tx: req.Tx.Bytes, Type: req.Type})
	if err != nil {
		return nil, err
	}
	sender, err := hex.DecodeString(res.Sender)
	if err != nil {
		return nil, err
	}
	return &local.ResponseNativeCheckTx{
		Code:         res.Code,
		Data:         res.Data,
		Log:          res.Log,
		Info:         res.Info,
		GasWanted:    res.GasWanted,
		GasUsed:      res.GasUsed,
		Events:       res.Events,
		Codespace:    res.Codespace,
		Sender:       sender,
		Priority:     res.Priority,
		MempoolError: nil,
	}, nil
}

func (app *localToRemoteProxy) BeginBlock(ctx context.Context, req *local.RequestNativeBeginBlock,
) (*types.ResponseBeginBlock, error) {
	bhx := req.GetHeader()
	return app.client.BeginBlockSync(ctx, types.RequestBeginBlock{
		Hash: req.GetHash(),
		Header: types.Header{
			Creator:          bhx.Creator,
			Timestamp:        uint64(bhx.Time),
			PreviousHashes:   bhx.PrevHashes,
			Height:           uint64(bhx.Height),
			TransactionsRoot: bhx.TxRoot,
			TransactionCount: bhx.TxCount,
			Extra:            bhx.Extra,
			Signature:        bhx.Sig,
		},
		Extra: bhx.Extra,
	})
}

func (app *localToRemoteProxy) DeliverTx(ctx context.Context, req *local.RequestNativeDeliverTx,
) (*types.ResponseDeliverTx, error) {
	return app.client.DeliverTxSync(ctx, types.RequestDeliverTx{Tx: req.Tx.Bytes})
}

func (app *localToRemoteProxy) EndBlock(ctx context.Context, req *types.RequestEndBlock,
) (*local.ResponseNativeEndBlock, error) {
	res, err := app.client.EndBlockSync(ctx, *req)
	if err != nil {
		return nil, err
	}
	be := app.blockExtraCtor()
	if _, err := app.mrsh.UnmarshalStruct(res.Extra, be); err != nil {
		return nil, err
	}
	return &local.ResponseNativeEndBlock{
		Events: res.Events,
		Extra:  be,
	}, nil
}

func (app *localToRemoteProxy) Commit(ctx context.Context) (*types.ResponseCommit, error) {
	return app.client.CommitSync(ctx)
}

func (app *localToRemoteProxy) ListSnapshots(ctx context.Context, req *types.RequestListSnapshots,
) (*types.ResponseListSnapshots, error) {
	return app.client.ListSnapshotsSync(ctx, *req)
}

func (app *localToRemoteProxy) OfferSnapshot(ctx context.Context, req *types.RequestOfferSnapshot,
) (*types.ResponseOfferSnapshot, error) {
	return app.client.OfferSnapshotSync(ctx, *req)
}

func (app *localToRemoteProxy) LoadSnapshotChunk(ctx context.Context, req *types.RequestLoadSnapshotChunk,
) (*types.ResponseLoadSnapshotChunk, error) {
	return app.client.LoadSnapshotChunkSync(ctx, *req)
}

func (app *localToRemoteProxy) ApplySnapshotChunk(ctx context.Context, req *types.RequestApplySnapshotChunk,
) (*types.ResponseApplySnapshotChunk, error) {
	return app.client.ApplySnapshotChunkSync(ctx, *req)
}
