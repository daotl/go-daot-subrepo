package kvstore

import (
	mrand "math/rand"

	grand "github.com/daotl/guts/rand"
	"github.com/gogo/protobuf/proto"

	"github.com/daotl/go-acei/types"
	"github.com/daotl/go-acei/types/consensus/tendermint"
)

// RandVal creates one random validator, with a key derived
// from the input value
func RandVal(i int) tendermint.ValidatorUpdate {
	pubkey := grand.Bytes(32)
	// Random value between [0, 2^16 - 1]
	power := mrand.Uint32() & (1<<16 - 1) // nolint:gosec // G404: Use of weak random number generator
	v := tendermint.UpdateValidator(pubkey, int64(power), "")
	return v
}

// RandVals returns a list of cnt validators for initializing
// the application. Note that the keys are deterministically
// derived from the index in the array, while the power is
// random (Change this if not desired)
func RandVals(cnt int) []tendermint.ValidatorUpdate {
	res := make([]tendermint.ValidatorUpdate, cnt)
	for i := 0; i < cnt; i++ {
		res[i] = RandVal(i)
	}
	return res
}

// InitKVStore initializes the kvstore app with some data,
// which allows tests to pass and is fine as long as you
// don't make any tx that modify the validator state
func InitKVStore(app *PersistentKVStoreApplication) {
	extra := &tendermint.RequestInitLedgerExtra{Validators: RandVals(1)}
	ebin, err := proto.Marshal(extra)
	if err != nil {
		panic(err)
	}
	app.InitLedger(types.RequestInitLedger{
		Extra: ebin,
	})
}
