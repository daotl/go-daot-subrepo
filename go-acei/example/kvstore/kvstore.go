package kvstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"

	"github.com/daotl/go-acei/example/code"
	"github.com/daotl/go-acei/types"
	"github.com/daotl/go-acei/version"
)

var (
	stateKey        = key.NewBytesKeyFromString("stateKey")
	kvPairPrefixKey = key.NewBytesKeyFromString("kvPairKey:")

	ProtocolVersion uint64 = 0x1
)

type State struct {
	ds      datastore.Datastore
	Size    int64  `json:"size"`
	Height  uint64 `json:"height"`
	AppHash []byte `json:"app_hash"`
}

func loadState(ctx context.Context, ds datastore.Datastore) State {
	var state State
	state.ds = ds
	stateBytes, err := ds.Get(ctx, stateKey)
	if err != nil && err != datastore.ErrNotFound {
		panic(err)
	}
	if len(stateBytes) == 0 {
		return state
	}
	err = json.Unmarshal(stateBytes, &state)
	if err != nil {
		panic(err)
	}
	return state
}

func saveState(ctx context.Context, state State) {
	stateBytes, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	err = state.ds.Put(ctx, stateKey, stateBytes)
	if err != nil {
		panic(err)
	}
}

func prefixKey(key []byte) key.Key {
	return kvPairPrefixKey.ChildBytes(key)
}

//---------------------------------------------------

var _ types.Application = (*Application)(nil)

type Application struct {
	types.BaseApplication

	ctx          context.Context
	state        State
	RetainBlocks uint64 // blocks to retain after commit (via ResponseCommit.RetainHeight)
}

func NewApplication(ctx context.Context) *Application {
	d, err := datastore.NewMapDatastore(key.KeyTypeBytes)
	if err != nil {
		panic(err)
	}
	state := loadState(ctx, d)
	return &Application{ctx: ctx, state: state}
}

func (app *Application) Info(req types.RequestInfo) (resInfo types.ResponseInfo) {
	return types.ResponseInfo{
		Data:             fmt.Sprintf("{\"size\":%v}", app.state.Size),
		Version:          version.Version,
		AppVersion:       ProtocolVersion,
		LastBlockHeight:  app.state.Height,
		LastBlockAppHash: app.state.AppHash,
	}
}

// tx is either "key=value" or just arbitrary bytes
func (app *Application) DeliverTx(req types.RequestDeliverTx) types.ResponseDeliverTx {
	var key, value string

	parts := bytes.Split(req.Tx, []byte("="))
	if len(parts) == 2 {
		key, value = string(parts[0]), string(parts[1])
	} else {
		key, value = string(req.Tx), string(req.Tx)
	}

	err := app.state.ds.Put(app.ctx, prefixKey([]byte(key)), []byte(value))
	if err != nil {
		panic(err)
	}
	app.state.Size++

	events := []types.Event{
		{
			Type: "app",
			Attributes: []types.EventAttribute{
				{Key: "creator", Value: "Cosmoshi Netowoko", Index: true},
				{Key: "key", Value: key, Index: true},
				{Key: "index_key", Value: "index is working", Index: true},
				{Key: "noindex_key", Value: "index is working", Index: false},
			},
		},
	}

	return types.ResponseDeliverTx{Code: code.CodeTypeOK, Events: events}
}

func (app *Application) CheckTx(req types.RequestCheckTx) types.ResponseCheckTx {
	return types.ResponseCheckTx{Code: code.CodeTypeOK, GasWanted: 1}
}

func (app *Application) Commit() types.ResponseCommit {
	// Using a memdb - just return the big endian size of the ds
	appHash := make([]byte, 8)
	binary.PutVarint(appHash, app.state.Size)
	app.state.AppHash = appHash
	app.state.Height++
	saveState(app.ctx, app.state)

	resp := types.ResponseCommit{Data: appHash}
	if app.RetainBlocks > 0 && app.state.Height >= app.RetainBlocks {
		resp.RetainHeight = app.state.Height - app.RetainBlocks + 1
	}
	return resp
}

// Returns an associated value or nil if missing.
func (app *Application) Query(reqQuery types.RequestQuery) (resQuery types.ResponseQuery) {
	if reqQuery.Prove {
		value, err := app.state.ds.Get(app.ctx, prefixKey(reqQuery.Data))
		if err != nil {
			panic(err)
		}
		if value == nil {
			resQuery.Log = "does not exist"
		} else {
			resQuery.Log = "exists"
		}
		resQuery.Index = -1 // TODO make Proof return index
		resQuery.Key = reqQuery.Data
		resQuery.Value = value
		resQuery.Height = app.state.Height

		return
	}

	resQuery.Key = reqQuery.Data
	value, err := app.state.ds.Get(app.ctx, prefixKey(reqQuery.Data))
	if err != nil {
		panic(err)
	}
	if value == nil {
		resQuery.Log = "does not exist"
	} else {
		resQuery.Log = "exists"
	}
	resQuery.Value = value
	resQuery.Height = app.state.Height

	return resQuery
}
