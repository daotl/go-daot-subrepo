// Copyright (c) Tendermint. All rights reserved.
// Use of this source code is governed by Apache License 2.0 that can be
// found in the LICENSE_APACHE_2.0 file.

package net

// From: https://github.com/tendermint/tendermint/blob/5cc980698a3402afce76b26693ab54b8f67f038b/libs/net/net_test.go

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtocolAndAddress(t *testing.T) {

	cases := []struct {
		fullAddr string
		proto    string
		addr     string
	}{
		{
			"tcp://mydomain:80",
			"tcp",
			"mydomain:80",
		},
		{
			"grpc://mydomain:80",
			"grpc",
			"mydomain:80",
		},
		{
			"mydomain:80",
			"tcp",
			"mydomain:80",
		},
		{
			"unix://mydomain:80",
			"unix",
			"mydomain:80",
		},
	}

	for _, c := range cases {
		proto, addr := ProtocolAndAddress(c.fullAddr)
		assert.Equal(t, proto, c.proto)
		assert.Equal(t, addr, c.addr)
	}
}
