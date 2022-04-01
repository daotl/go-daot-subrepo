package testsuite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	mrand "math/rand"

	grand "github.com/daotl/guts/rand"
	"github.com/gogo/protobuf/proto"

	aceiclient "github.com/daotl/go-acei/client"
	"github.com/daotl/go-acei/types"
	"github.com/daotl/go-acei/types/consensus/tendermint"
)

func InitLedger(ctx context.Context, client aceiclient.Client) error {
	total := 10
	vals := make([]tendermint.ValidatorUpdate, total)
	for i := 0; i < total; i++ {
		pubkey := grand.Bytes(33)
		// nolint:gosec // G404: Use of weak random number generator
		power := mrand.Int()
		vals[i] = tendermint.UpdateValidator(pubkey, int64(power), "")
	}
	extra := &tendermint.RequestInitLedgerExtra{Validators: vals}
	ebin, err := proto.Marshal(extra)
	if err != nil {
		return err
	}
	_, err = client.InitLedgerSync(ctx, types.RequestInitLedger{
		Extra: ebin,
	})
	if err != nil {
		fmt.Printf("Failed test: InitLedger - %v\n", err)
		return err
	}
	fmt.Println("Passed test: InitLedger")
	return nil
}

func Commit(ctx context.Context, client aceiclient.Client, hashExp []byte) error {
	res, err := client.CommitSync(ctx)
	data := res.Data
	if err != nil {
		fmt.Println("Failed test: Commit")
		fmt.Printf("error while committing: %v\n", err)
		return err
	}
	if !bytes.Equal(data, hashExp) {
		fmt.Println("Failed test: Commit")
		fmt.Printf("Commit hash was unexpected. Got %X expected %X\n", data, hashExp)
		return errors.New("commitTx failed")
	}
	fmt.Println("Passed test: Commit")
	return nil
}

func DeliverTx(ctx context.Context, client aceiclient.Client, txBytes []byte, codeExp uint32, dataExp []byte) error {
	res, _ := client.DeliverTxSync(ctx, types.RequestDeliverTx{Tx: txBytes})
	code, data, log := res.Code, res.Data, res.Log
	if code != codeExp {
		fmt.Println("Failed test: DeliverTx")
		fmt.Printf("DeliverTx response code was unexpected. Got %v expected %v. Log: %v\n",
			code, codeExp, log)
		return errors.New("deliverTx error")
	}
	if !bytes.Equal(data, dataExp) {
		fmt.Println("Failed test: DeliverTx")
		fmt.Printf("DeliverTx response data was unexpected. Got %X expected %X\n",
			data, dataExp)
		return errors.New("deliverTx error")
	}
	fmt.Println("Passed test: DeliverTx")
	return nil
}

func CheckTx(ctx context.Context, client aceiclient.Client, txBytes []byte, codeExp uint32, dataExp []byte) error {
	res, _ := client.CheckTxSync(ctx, types.RequestCheckTx{Tx: txBytes})
	code, data, log := res.Code, res.Data, res.Log
	if code != codeExp {
		fmt.Println("Failed test: CheckTx")
		fmt.Printf("CheckTx response code was unexpected. Got %v expected %v. Log: %v\n",
			code, codeExp, log)
		return errors.New("checkTx")
	}
	if !bytes.Equal(data, dataExp) {
		fmt.Println("Failed test: CheckTx")
		fmt.Printf("CheckTx response data was unexpected. Got %X expected %X\n",
			data, dataExp)
		return errors.New("checkTx")
	}
	fmt.Println("Passed test: CheckTx")
	return nil
}
