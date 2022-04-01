package kvstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	dskey "github.com/daotl/go-datastore/key"
	dsq "github.com/daotl/go-datastore/query"
	leveldb "github.com/daotl/go-ds-leveldb"
	"github.com/daotl/go-log/v2"
	"github.com/gogo/protobuf/proto"

	"github.com/daotl/go-acei/crypto/encoding"
	"github.com/daotl/go-acei/example/code"
	"github.com/daotl/go-acei/types"
	"github.com/daotl/go-acei/types/consensus/tendermint"
)

const (
	ValidatorSetChangePrefix string = "val:"
)

//-----------------------------------------

var _ types.Application = (*PersistentKVStoreApplication)(nil)

type PersistentKVStoreApplication struct {
	app *Application

	// validator set
	ValUpdates []tendermint.ValidatorUpdate

	valAddrToPubKeyMap map[string]types.PublicKey

	logger log.StandardLogger
}

func NewPersistentKVStoreApplication(ctx context.Context, dbDir string) *PersistentKVStoreApplication {
	name := "kvstore"
	db, err := leveldb.NewDatastore(dbDir+"/"+name, dskey.KeyTypeBytes, nil)
	if err != nil {
		panic(err)
	}

	state := loadState(ctx, db)

	return &PersistentKVStoreApplication{
		app:                &Application{ctx: ctx, state: state},
		valAddrToPubKeyMap: make(map[string]types.PublicKey),
		logger:             log.NopLogger(),
	}
}

func (app *PersistentKVStoreApplication) Close() error {
	return app.app.state.ds.Close()
}

func (app *PersistentKVStoreApplication) SetLogger(l log.StandardLogger) {
	app.logger = l
}

func (app *PersistentKVStoreApplication) Info(req types.RequestInfo) types.ResponseInfo {
	res := app.app.Info(req)
	res.LastBlockHeight = app.app.state.Height
	res.LastBlockAppHash = app.app.state.AppHash
	return res
}

// tx is either "val:pubkey!power" or "key=value" or just arbitrary bytes
func (app *PersistentKVStoreApplication) DeliverTx(req types.RequestDeliverTx) types.ResponseDeliverTx {
	// if it starts with "val:", update the validator set
	// format is "val:pubkey!power"
	if isValidatorTx(req.Tx) {
		// update validators in the merkle tree
		// and in app.ValUpdates
		return app.execValidatorTx(req.Tx)
	}

	// otherwise, update the key-value store
	return app.app.DeliverTx(req)
}

func (app *PersistentKVStoreApplication) CheckTx(req types.RequestCheckTx) types.ResponseCheckTx {
	return app.app.CheckTx(req)
}

// Commit will panic if InitLedger was not called
func (app *PersistentKVStoreApplication) Commit() types.ResponseCommit {
	return app.app.Commit()
}

// When path=/val and data={validator address}, returns the validator update (tendermint.
//ValidatorUpdate) varint encoded.
// For any other path, returns an associated value or nil if missing.
func (app *PersistentKVStoreApplication) Query(reqQuery types.RequestQuery) (resQuery types.ResponseQuery) {
	switch reqQuery.Path {
	case "/val":
		key := dskey.NewBytesKeyFromString("val:" + string(reqQuery.Data))
		value, err := app.app.state.ds.Get(app.app.ctx, key)
		if err != nil {
			panic(err)
		}

		resQuery.Key = reqQuery.Data
		resQuery.Value = value
		return
	default:
		return app.app.Query(reqQuery)
	}
}

// Save the validators in the merkle tree
func (app *PersistentKVStoreApplication) InitLedger(req types.RequestInitLedger) types.ResponseInitLedger {
	extra := &tendermint.RequestInitLedgerExtra{}
	if proto.Unmarshal(req.Extra, extra) != nil {
		app.logger.Error("Error decoding extra data for Tendermint")
	} else {
		for _, v := range extra.Validators {
			r := app.updateValidator(v)
			if r.IsErr() {
				app.logger.Error("Error updating validators", "r", r)
			}
		}
	}
	return types.ResponseInitLedger{}
}

// Track the block hash and header information
func (app *PersistentKVStoreApplication) BeginBlock(req types.RequestBeginBlock) types.ResponseBeginBlock {
	// reset valset changes
	app.ValUpdates = make([]tendermint.ValidatorUpdate, 0)

	// Punish validators who committed equivocation.
	extra := &tendermint.RequestBeginBlockExtra{}
	if err := proto.Unmarshal(req.Extra, extra); err != nil {
		app.logger.Error("Error decoding extra data for Tendermint")
	} else {
		for _, ev := range extra.ByzantineValidators {
			if ev.Type == tendermint.EvidenceType_DUPLICATE_VOTE {
				addr := string(ev.Validator.Address)
				if pubKey, ok := app.valAddrToPubKeyMap[addr]; ok {
					app.updateValidator(tendermint.ValidatorUpdate{
						PubKey: pubKey,
						Power:  ev.Validator.Power - 1,
					})
					app.logger.Info("Decreased val power by 1 because of the equivocation",
						"val", addr)
				} else {
					app.logger.Error("Wanted to punish val, but can't find it",
						"val", addr)
				}
			}
		}
	}

	return types.ResponseBeginBlock{}
}

// Update the validator set
func (app *PersistentKVStoreApplication) EndBlock(req types.RequestEndBlock) types.ResponseEndBlock {
	extra := &tendermint.ResponseEndBlockExtra{ValidatorUpdates: app.ValUpdates}
	ebin, err := proto.Marshal(extra)
	if err != nil {
		panic(err)
	}
	return types.ResponseEndBlock{Extra: ebin}
}

func (app *PersistentKVStoreApplication) ListSnapshots(
	req types.RequestListSnapshots) types.ResponseListSnapshots {
	return types.ResponseListSnapshots{}
}

func (app *PersistentKVStoreApplication) LoadSnapshotChunk(
	req types.RequestLoadSnapshotChunk) types.ResponseLoadSnapshotChunk {
	return types.ResponseLoadSnapshotChunk{}
}

func (app *PersistentKVStoreApplication) OfferSnapshot(
	req types.RequestOfferSnapshot) types.ResponseOfferSnapshot {
	return types.ResponseOfferSnapshot{Result: types.ResponseOfferSnapshot_ABORT}
}

func (app *PersistentKVStoreApplication) ApplySnapshotChunk(
	req types.RequestApplySnapshotChunk) types.ResponseApplySnapshotChunk {
	return types.ResponseApplySnapshotChunk{Result: types.ResponseApplySnapshotChunk_ABORT}
}

//---------------------------------------------
// update validators

func (app *PersistentKVStoreApplication) Validators() (validators []tendermint.ValidatorUpdate) {
	results, err := app.app.state.ds.Query(app.app.ctx, dsq.Query{})
	if err != nil {
		panic(err)
	}
	for r := range results.Next() {
		if isValidatorTx(r.Key.Bytes()) {
			validator := new(tendermint.ValidatorUpdate)
			err := types.ReadMessage(bytes.NewBuffer(r.Value), validator)
			if err != nil {
				panic(err)
			}
			validators = append(validators, *validator)
		}
	}
	if err = results.Close(); err != nil {
		panic(err)
	}
	return
}

func MakeValSetChangeTx(pubkey types.PublicKey, power int64) []byte {
	pk, err := encoding.PubKeyFromProto(pubkey)
	if err != nil {
		panic(err)
	}
	pubStr := base64.StdEncoding.EncodeToString(pk.Bytes())
	return []byte(fmt.Sprintf("val:%s!%d", pubStr, power))
}

func isValidatorTx(tx []byte) bool {
	return strings.HasPrefix(string(tx), ValidatorSetChangePrefix)
}

// format is "val:pubkey!power"
// pubkey is a base64-encoded 32-byte ed25519 key
func (app *PersistentKVStoreApplication) execValidatorTx(tx []byte) types.ResponseDeliverTx {
	tx = tx[len(ValidatorSetChangePrefix):]

	//  get the pubkey and power
	pubKeyAndPower := strings.Split(string(tx), "!")
	if len(pubKeyAndPower) != 2 {
		return types.ResponseDeliverTx{
			Code: code.CodeTypeEncodingError,
			Log:  fmt.Sprintf("Expected 'pubkey!power'. Got %v", pubKeyAndPower)}
	}
	pubkeyS, powerS := pubKeyAndPower[0], pubKeyAndPower[1]

	// decode the pubkey
	pubkey, err := base64.StdEncoding.DecodeString(pubkeyS)
	if err != nil {
		return types.ResponseDeliverTx{
			Code: code.CodeTypeEncodingError,
			Log:  fmt.Sprintf("Pubkey (%s) is invalid base64", pubkeyS)}
	}

	// decode the power
	power, err := strconv.ParseInt(powerS, 10, 64)
	if err != nil {
		return types.ResponseDeliverTx{
			Code: code.CodeTypeEncodingError,
			Log:  fmt.Sprintf("Power (%s) is not an int", powerS)}
	}

	// update
	return app.updateValidator(tendermint.UpdateValidator(pubkey, power, ""))
}

// add, update, or remove a validator
func (app *PersistentKVStoreApplication) updateValidator(v tendermint.ValidatorUpdate,
) types.ResponseDeliverTx {
	pubkey, err := encoding.PubKeyFromProto(v.PubKey)
	if err != nil {
		panic(fmt.Errorf("can't decode public key: %w", err))
	}
	key := dskey.NewBytesKeyFromString("val:" + string(pubkey.Bytes()))

	if v.Power == 0 {
		// remove validator
		hasKey, err := app.app.state.ds.Has(app.app.ctx, key)
		if err != nil {
			panic(err)
		}
		if !hasKey {
			pubStr := base64.StdEncoding.EncodeToString(pubkey.Bytes())
			return types.ResponseDeliverTx{
				Code: code.CodeTypeUnauthorized,
				Log:  fmt.Sprintf("Cannot remove non-existent validator %s", pubStr)}
		}
		if err = app.app.state.ds.Delete(app.app.ctx, key); err != nil {
			panic(err)
		}
		delete(app.valAddrToPubKeyMap, string(pubkey.Address()))
	} else {
		// add or update validator
		value := bytes.NewBuffer(make([]byte, 0))
		if err := types.WriteMessage(&v, value); err != nil {
			return types.ResponseDeliverTx{
				Code: code.CodeTypeEncodingError,
				Log:  fmt.Sprintf("Error encoding validator: %v", err)}
		}
		if err = app.app.state.ds.Put(app.app.ctx, key, value.Bytes()); err != nil {
			panic(err)
		}
		app.valAddrToPubKeyMap[string(pubkey.Address())] = v.PubKey
	}

	// we only update the changes array if we successfully updated the tree
	app.ValUpdates = append(app.ValUpdates, v)

	return types.ResponseDeliverTx{Code: code.CodeTypeOK}
}
