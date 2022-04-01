package main

import (
	cbg "github.com/daotl/cbor-gen"

	"github.com/daotl/go-marsha/test"
)

func main() {
	if err := cbg.WriteTupleEncodersToFile("test/models_cbor.go",
		"test", true, nil, test.TestStruct{}, test.TestStruct2{}); err != nil {
		panic(err)
	}
}
