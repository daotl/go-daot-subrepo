package kvstore

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/daotl/go-log/v2"
	ssrv "github.com/daotl/guts/service/suture"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	aceiclient "github.com/daotl/go-acei/client"
	"github.com/daotl/go-acei/example/code"
	aceiserver "github.com/daotl/go-acei/server"
	"github.com/daotl/go-acei/types"
	"github.com/daotl/go-acei/types/consensus/tendermint"
)

const (
	testKey   = "abc"
	testValue = "def"
)

func testKVStore(t *testing.T, app types.Application, tx []byte, key, value string) {
	req := types.RequestDeliverTx{Tx: tx}
	ar := app.DeliverTx(req)
	require.False(t, ar.IsErr(), ar)
	// repeating tx doesn't raise error
	ar = app.DeliverTx(req)
	require.False(t, ar.IsErr(), ar)
	// commit
	app.Commit()

	info := app.Info(types.RequestInfo{})
	require.NotZero(t, info.LastBlockHeight)

	// make sure query is fine
	resQuery := app.Query(types.RequestQuery{
		Path: "/store",
		Data: []byte(key),
	})
	require.Equal(t, code.CodeTypeOK, resQuery.Code)
	require.Equal(t, key, string(resQuery.Key))
	require.Equal(t, value, string(resQuery.Value))
	require.EqualValues(t, info.LastBlockHeight, resQuery.Height)

	// make sure proof is fine
	resQuery = app.Query(types.RequestQuery{
		Path:  "/store",
		Data:  []byte(key),
		Prove: true,
	})
	require.EqualValues(t, code.CodeTypeOK, resQuery.Code)
	require.Equal(t, key, string(resQuery.Key))
	require.Equal(t, value, string(resQuery.Value))
	require.EqualValues(t, info.LastBlockHeight, resQuery.Height)
}

func TestKVStoreKV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvstore := NewApplication(ctx)
	key := testKey
	value := key
	tx := []byte(key)
	testKVStore(t, kvstore, tx, key, value)

	value = testValue
	tx = []byte(key + "=" + value)
	testKVStore(t, kvstore, tx, key, value)
}

func TestPersistentKVStoreKV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := os.MkdirTemp("/tmp", "acei-kvstore-test") // TODO
	if err != nil {
		t.Fatal(err)
	}
	kvstore := NewPersistentKVStoreApplication(ctx, dir)
	key := testKey
	value := key
	tx := []byte(key)
	testKVStore(t, kvstore, tx, key, value)

	value = testValue
	tx = []byte(key + "=" + value)
	testKVStore(t, kvstore, tx, key, value)
}

func TestPersistentKVStoreInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := os.MkdirTemp("/tmp", "acei-kvstore-test") // TODO
	if err != nil {
		t.Fatal(err)
	}
	kvstore := NewPersistentKVStoreApplication(ctx, dir)
	InitKVStore(kvstore)
	height := uint64(0)

	resInfo := kvstore.Info(types.RequestInfo{})
	if resInfo.LastBlockHeight != height {
		t.Fatalf("expected height of %d, got %d", height, resInfo.LastBlockHeight)
	}

	// make and apply block
	height = uint64(1)
	hash := []byte("foo")
	header := types.Header{
		Height: height,
	}
	kvstore.BeginBlock(types.RequestBeginBlock{Hash: hash, Header: header})
	kvstore.EndBlock(types.RequestEndBlock{Height: header.Height})
	kvstore.Commit()

	resInfo = kvstore.Info(types.RequestInfo{})
	if resInfo.LastBlockHeight != height {
		t.Fatalf("expected height of %d, got %d", height, resInfo.LastBlockHeight)
	}

}

// add a validator, remove a validator, update a validator
func TestValUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := os.MkdirTemp("/tmp", "acei-kvstore-test") // TODO
	if err != nil {
		t.Fatal(err)
	}
	kvstore := NewPersistentKVStoreApplication(ctx, dir)

	// init with some validators
	total := 10
	nInit := 5
	vals := RandVals(total)
	// initialize with the first nInit
	extra := &tendermint.RequestInitLedgerExtra{Validators: vals[:nInit]}
	ebin, err := proto.Marshal(extra)
	if err != nil {
		panic(err)
	}
	kvstore.InitLedger(types.RequestInitLedger{
		Extra: ebin,
	})

	vals1, vals2 := vals[:nInit], kvstore.Validators()
	valsEqual(t, vals1, vals2)

	var v1, v2, v3 tendermint.ValidatorUpdate

	// add some validators
	v1, v2 = vals[nInit], vals[nInit+1]
	diff := []tendermint.ValidatorUpdate{v1, v2}
	tx1 := MakeValSetChangeTx(v1.PubKey, v1.Power)
	tx2 := MakeValSetChangeTx(v2.PubKey, v2.Power)

	makeApplyBlock(t, kvstore, 1, diff, tx1, tx2)

	vals1, vals2 = vals[:nInit+2], kvstore.Validators()
	valsEqual(t, vals1, vals2)

	// remove some validators
	v1, v2, v3 = vals[nInit-2], vals[nInit-1], vals[nInit]
	v1.Power = 0
	v2.Power = 0
	v3.Power = 0
	diff = []tendermint.ValidatorUpdate{v1, v2, v3}
	tx1 = MakeValSetChangeTx(v1.PubKey, v1.Power)
	tx2 = MakeValSetChangeTx(v2.PubKey, v2.Power)
	tx3 := MakeValSetChangeTx(v3.PubKey, v3.Power)

	makeApplyBlock(t, kvstore, 2, diff, tx1, tx2, tx3)

	vals1 = append(vals[:nInit-2], vals[nInit+1]) // nolint: gocritic
	vals2 = kvstore.Validators()
	valsEqual(t, vals1, vals2)

	// update some validators
	v1 = vals[0]
	if v1.Power == 5 {
		v1.Power = 6
	} else {
		v1.Power = 5
	}
	diff = []tendermint.ValidatorUpdate{v1}
	tx1 = MakeValSetChangeTx(v1.PubKey, v1.Power)

	makeApplyBlock(t, kvstore, 3, diff, tx1)

	vals1 = append([]tendermint.ValidatorUpdate{v1}, vals1[1:]...)
	vals2 = kvstore.Validators()
	valsEqual(t, vals1, vals2)

}

func makeApplyBlock(
	t *testing.T,
	kvstore types.Application,
	heightInt int,
	diff []tendermint.ValidatorUpdate,
	txs ...[]byte) {
	// make and apply block
	height := uint64(heightInt)
	hash := []byte("foo")
	header := types.Header{
		Height: height,
	}

	kvstore.BeginBlock(types.RequestBeginBlock{Hash: hash, Header: header})
	for _, tx := range txs {
		if r := kvstore.DeliverTx(types.RequestDeliverTx{Tx: tx}); r.IsErr() {
			t.Fatal(r)
		}
	}
	resEndBlock := kvstore.EndBlock(types.RequestEndBlock{Height: header.Height})
	kvstore.Commit()

	extra := &tendermint.ResponseEndBlockExtra{}
	if err := proto.Unmarshal(resEndBlock.Extra, extra); err != nil {
		panic(err)
	}

	valsEqual(t, diff, extra.ValidatorUpdates)

}

// order doesn't matter
func valsEqual(t *testing.T, vals1, vals2 []tendermint.ValidatorUpdate) {
	if len(vals1) != len(vals2) {
		t.Fatalf("vals dont match in len. got %d, expected %d", len(vals2), len(vals1))
	}
	sort.Sort(tendermint.ValidatorUpdates(vals1))
	sort.Sort(tendermint.ValidatorUpdates(vals2))
	for i, v1 := range vals1 {
		v2 := vals2[i]
		if !v1.PubKey.Equal(v2.PubKey) ||
			v1.Power != v2.Power {
			t.Fatalf("vals dont match at index %d. got %X/%d , expected %X/%d", i, v2.PubKey, v2.Power, v1.PubKey, v1.Power)
		}
	}
}

func makeSocketClientServer(
	ctx context.Context,
	t *testing.T,
	logger *zap.SugaredLogger,
	app types.Application,
	name string,
) (aceiclient.Client, ssrv.Service, error) {
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	// Start the listener
	socket := fmt.Sprintf("unix://%s.sock", name)

	server, err := aceiserver.NewSocketServer(logger.With("module", "acei-server"), socket, app)
	if err != nil {
		return nil, nil, err
	}
	readyCh, sResCh := server.Start(ctx)
	if err := <-readyCh; err != nil {
		cancel()
		return nil, nil, err
	}
	go func() {
		if err := <-sResCh; err != nil {
			panic(err)
		}
	}()

	// Connect to the socket
	client, err := aceiclient.NewSocketClient(logger.With("module", "acei-client"), socket, false)
	if err != nil {
		return nil, nil, err
	}
	readyCh, cResCh := client.Start(ctx)
	if <-readyCh != nil {
		cancel()
		if stopped, err := server.Stop(); err == nil {
			<-stopped
		}
		return nil, nil, err
	}
	go func() {
		if err := <-cResCh; err != nil {
			panic(err)
		}
	}()

	return client, server, nil
}

func makeGRPCClientServer(
	ctx context.Context,
	t *testing.T,
	logger *zap.SugaredLogger,
	app types.Application,
	name string,
) (aceiclient.Client, ssrv.Service, error) {
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	// Start the listener
	socket := fmt.Sprintf("unix://%s.sock", name)

	gapp := types.NewGRPCApplication(app)
	server, err := aceiserver.NewGRPCServer(logger.Named("acei-server"), socket, gapp)
	if err != nil {
		return nil, nil, err
	}
	readyCh, sResCh := server.Start(ctx)
	if err := <-readyCh; err != nil {
		cancel()
		return nil, nil, err
	}
	go func() {
		if err := <-sResCh; err != nil {
			panic(err)
		}
	}()

	client, err := aceiclient.NewGRPCClient(logger.Named("acei-client"), socket, true)
	if err != nil {
		return nil, nil, err
	}
	readyCh, cResCh := client.Start(ctx)
	if <-readyCh != nil {
		cancel()
		return nil, nil, err
	}
	go func() {
		if err := <-cResCh; err != nil {
			panic(err)
		}
	}()
	return client, server, nil
}

func TestClientServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.TestingLogger()

	// set up socket app
	kvstore := NewApplication(ctx)
	client, server, err := makeSocketClientServer(ctx, t, logger, kvstore, "kvstore-socket")
	require.NoError(t, err)
	t.Cleanup(func() {
		if stopped, err := server.Stop(); err == nil {
			<-stopped
		}
	})
	t.Cleanup(func() {
		if stopped, err := client.Stop(); err == nil {
			<-stopped
		}
	})

	runClientTests(ctx, t, client)

	// set up grpc app
	kvstore = NewApplication(ctx)
	gclient, gserver, err := makeGRPCClientServer(ctx, t, logger, kvstore, "/tmp/kvstore-grpc")
	require.NoError(t, err)

	t.Cleanup(func() {
		if stopped, err := gserver.Stop(); err == nil {
			<-stopped
		}
	})
	t.Cleanup(func() {
		if stopped, err := gclient.Stop(); err == nil {
			<-stopped
		}
	})

	runClientTests(ctx, t, gclient)
}

func runClientTests(ctx context.Context, t *testing.T, client aceiclient.Client) {
	// run some tests....
	key := testKey
	value := key
	tx := []byte(key)
	testClient(ctx, t, client, tx, key, value)

	value = testValue
	tx = []byte(key + "=" + value)
	testClient(ctx, t, client, tx, key, value)
}

func testClient(ctx context.Context, t *testing.T, app aceiclient.Client, tx []byte, key, value string) {
	ar, err := app.DeliverTxSync(ctx, types.RequestDeliverTx{Tx: tx})
	require.NoError(t, err)
	require.False(t, ar.IsErr(), ar)
	// repeating tx doesn't raise error
	ar, err = app.DeliverTxSync(ctx, types.RequestDeliverTx{Tx: tx})
	require.NoError(t, err)
	require.False(t, ar.IsErr(), ar)
	// commit
	_, err = app.CommitSync(ctx)
	require.NoError(t, err)

	info, err := app.InfoSync(ctx, types.RequestInfo{})
	require.NoError(t, err)
	require.NotZero(t, info.LastBlockHeight)

	// make sure query is fine
	resQuery, err := app.QuerySync(ctx, types.RequestQuery{
		Path: "/store",
		Data: []byte(key),
	})
	require.Nil(t, err)
	require.Equal(t, code.CodeTypeOK, resQuery.Code)
	require.Equal(t, key, string(resQuery.Key))
	require.Equal(t, value, string(resQuery.Value))
	require.EqualValues(t, info.LastBlockHeight, resQuery.Height)

	// make sure proof is fine
	resQuery, err = app.QuerySync(ctx, types.RequestQuery{
		Path:  "/store",
		Data:  []byte(key),
		Prove: true,
	})
	require.Nil(t, err)
	require.Equal(t, code.CodeTypeOK, resQuery.Code)
	require.Equal(t, key, string(resQuery.Key))
	require.Equal(t, value, string(resQuery.Value))
	require.EqualValues(t, info.LastBlockHeight, resQuery.Height)
}
