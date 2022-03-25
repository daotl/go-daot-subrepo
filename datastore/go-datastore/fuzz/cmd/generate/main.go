// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

// This file is invoked by `go generate`
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"text/template"
)

// This program generates bindings to fuzz a concrete datastore implementation.
// It can be invoked by running `go generate <implemenation>`

func main() {
	providers := os.Args[1:]

	if len(providers) == 0 {
		providers = strings.Split(os.Getenv("DS_PROVIDERS"), ",")
	}
	if len(providers) == 0 {
		fmt.Fprintf(os.Stderr, "No providers specified to generate. Nothing to do.")
		return
	}

	for _, provider := range providers {
		provider = strings.TrimSpace(provider)
		if len(provider) == 0 {
			continue
		}
		cmd := exec.Command("go", "get", provider)
		err := cmd.Run()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to add dependency for %s: %v\n", provider, err)
			os.Exit(1)
		}

		nameComponents := strings.Split(provider, "/")
		name := nameComponents[len(nameComponents)-1]
		f, err := os.Create(fmt.Sprintf("provider_%s.go", name))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create provider file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		err = provideTemplate.Execute(f, struct {
			Package     string
			PackageName string
		}{
			Package:     provider,
			PackageName: name,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to write provider: %v\n", err)
			os.Exit(1)
		}
	}
}

var provideTemplate = template.Must(template.New("").Parse(`// Code generated by go generate; DO NOT EDIT.

package fuzzer
import prov "{{ .Package }}"
import ds "github.com/daotl/go-datastore"

func init() {
	AddOpener("{{ .PackageName }}", func(loc string) ds.Datastore {
		d, err := prov.NewDatastore(loc, nil)
		if err != nil {
			panic("could not create db instance")
		}
		return d
	})
}
`))
