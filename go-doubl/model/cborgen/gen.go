package main

import (
	cbg "github.com/daotl/cbor-gen"

	"github.com/daotl/go-doubl/model"
)

func main() {
	if err := cbg.WriteTupleEncodersToFile(
		"model/models_cbor.go",
		"model",
		true,
		nil,
		model.Transaction{},
		model.BlockHeader{},
		model.Block{},
	); err != nil {
		panic(err)
	}
}
