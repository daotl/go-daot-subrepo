package main

import (
	cbg "github.com/daotl/cbor-gen"

	"github.com/daotl/go-doubl/test"
)

func main() {
	if err := cbg.WriteTupleEncodersToFile(
		"test/models_cbor.go",
		"test",
		true,
		nil,
		test.TransactionNoSig{},
	); err != nil {
		panic(err)
	}
}
