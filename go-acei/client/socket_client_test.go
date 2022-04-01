package aceiclient_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/daotl/go-log/v2"
	ssrv "github.com/daotl/guts/service/suture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	aceiclient "github.com/daotl/go-acei/client"
	"github.com/daotl/go-acei/server"
	"github.com/daotl/go-acei/types"
)

func TestProperSyncCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app := slowApp{}
	logger := log.TestingLogger()

	_, c := setupClientServer(ctx, t, logger, app)

	resp := make(chan error, 1)
	go func() {
		// This is BeginBlockSync unrolled....
		reqres, err := c.BeginBlockAsync(ctx, types.RequestBeginBlock{})
		assert.NoError(t, err)
		err = c.FlushSync(ctx)
		assert.NoError(t, err)
		res := reqres.Response.GetBeginBlock()
		assert.NotNil(t, res)
		resp <- c.Error()
	}()

	select {
	case <-time.After(time.Second):
		require.Fail(t, "No response arrived")
	case err, ok := <-resp:
		require.True(t, ok, "Must not close channel")
		assert.NoError(t, err, "This should return success")
	}
}

func setupClientServer(
	ctx context.Context,
	t *testing.T,
	logger log.StandardLogger,
	app types.Application,
) (ssrv.Service, aceiclient.Client) {
	t.Helper()

	// some port between 20k and 30k
	port := 20000 + rand.Int31()%10000
	addr := fmt.Sprintf("localhost:%d", port)

	s, err := server.NewServer(logger, addr, "socket", app)
	require.NoError(t, err)
	readyCh, sResCh := s.Start(ctx)
	require.NoError(t, <-readyCh)
	go func() { require.NoError(t, <-sResCh) }()
	t.Cleanup(func() {
		if stopped, err := s.Stop(); err == nil {
			<-stopped
		}
	})

	c, err := aceiclient.NewSocketClient(logger, addr, true)
	require.NoError(t, err)
	readyCh, cResCh := c.Start(ctx)
	require.NoError(t, <-readyCh)
	go func() { require.NoError(t, <-cResCh) }()
	t.Cleanup(func() {
		if stopped, err := c.Stop(); err == nil {
			<-stopped
		}
	})

	return s, c
}

type slowApp struct {
	types.BaseApplication
}

func (slowApp) BeginBlock(req types.RequestBeginBlock) types.ResponseBeginBlock {
	time.Sleep(200 * time.Millisecond)
	return types.ResponseBeginBlock{}
}
