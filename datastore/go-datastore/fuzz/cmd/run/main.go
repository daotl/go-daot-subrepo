// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/pflag"

	fuzzer "github.com/daotl/go-datastore/fuzz"
)

var input *string = pflag.StringP("input", "i", "", "file to read input from (stdin used if not specified)")
var db *string = pflag.StringP("database", "d", "go-ds-badger", "database to fuzz")
var dbFile *string = pflag.StringP("file", "f", "tmp", "where the db instace should live on disk")
var threads *int = pflag.IntP("threads", "t", 1, "concurrent threads")

func main() {
	pflag.Parse()

	fuzzer.Threads = *threads

	if *input != "" {
		dat, err := ioutil.ReadFile(*input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not read %s: %v\n", *input, err)
			os.Exit(1)
		}
		ret := fuzzer.FuzzDB(*db, *dbFile, false, dat)
		os.Exit(ret)
	} else {
		reader := bufio.NewReader(os.Stdin)
		err := fuzzer.FuzzStream(*db, *dbFile, false, reader)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fuzzing: %v\n", err)
			os.Exit(1)
		}
		return
	}
}
