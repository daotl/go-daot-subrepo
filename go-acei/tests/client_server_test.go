package tests

import (
	"context"
	"testing"

	"github.com/daotl/go-log/v2"
	"github.com/stretchr/testify/assert"

	abciclientent "github.com/daotl/go-acei/client"
	"github.com/daotl/go-acei/example/kvstore"
	abciserver "github.com/daotl/go-acei/server"
)

func TestClientServerNoAddrPrefix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		addr      = "localhost:26658"
		transport = "socket"
	)
	app := kvstore.NewApplication(ctx)
	logger := log.TestingLogger()

	server, err := abciserver.NewServer(logger, addr, transport, app)
	assert.NoError(t, err, "expected no error on NewServer")
	readyCh, sResCh := server.Start(ctx)
	assert.NoError(t, <-readyCh, "expected no error on server.Start")
	go func() { assert.NoError(t, <-sResCh, "expected no error on server stopping") }()

	client, err := abciclientent.NewClient(logger, addr, transport, true)
	assert.NoError(t, err, "expected no error on NewClient")
	readyCh, cResCh := client.Start(ctx)
	assert.NoError(t, <-readyCh, "expected no error on client.Start")
	go func() { assert.NoError(t, <-cResCh, "expected no error on client stopping") }()
}
