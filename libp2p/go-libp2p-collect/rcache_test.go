package collect

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	rid1 = RequestID("1")
	rid2 = RequestID("2")
)

func TestRemove(t *testing.T) {
	rcache, err := newRequestWorkerPool(1)
	assert.NoError(t, err)
	rcache.AddReqItem(context.Background(), rid1, &reqItem{})
	w, ok := rcache.GetReqWorker(rid1)
	assert.True(t, ok)
	ori := w.onClose
	close := make(chan struct{})
	w.onClose = func() {
		ori()
		close <- struct{}{}
	}
	rcache.RemoveReqItem(rid1)
	assert.True(t, !rcache.cache.Contains(rid1), "unexpected item")
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "haven't called onClose")
	case <-close:
	}
}

func TestCancel(t *testing.T) {
	rcache, err := newRequestWorkerPool(1)
	assert.NoError(t, err)
	cctx, cc := context.WithCancel(context.Background())
	rcache.AddReqItem(cctx, rid1, &reqItem{})
	w, ok := rcache.GetReqWorker(rid1)
	assert.True(t, ok)
	ori := w.onClose
	close := make(chan struct{})
	w.onClose = func() {
		ori()
		close <- struct{}{}
	}
	cc()
	// wait for close
	time.Sleep(1 * time.Millisecond)
	assert.True(t, !rcache.cache.Contains(rid1), "unexpected item")
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "haven't called onClose")
	case <-close:
	}
}

func TestEvict(t *testing.T) {
	rcache, err := newRequestWorkerPool(1)
	assert.NoError(t, err)
	rcache.AddReqItem(context.Background(), rid1, &reqItem{})
	w, ok := rcache.GetReqWorker(rid1)
	assert.True(t, ok)
	ori := w.onClose
	close := make(chan struct{})
	w.onClose = func() {
		ori()
		close <- struct{}{}
	}
	rcache.AddReqItem(context.Background(), rid2, &reqItem{})
	assert.True(t, !rcache.cache.Contains(rid1), "unexpected item")
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "haven't called onClose")
	case <-close:
	}
}

func TestSameEvict(t *testing.T) {

	rcache, err := newRequestWorkerPool(2)
	assert.NoError(t, err)
	rcache.AddReqItem(context.Background(), rid1, &reqItem{})
	w, ok := rcache.GetReqWorker(rid1)
	assert.True(t, ok)
	ori := w.onClose
	close := make(chan struct{})
	w.onClose = func() {
		ori()
		close <- struct{}{}
	}
	rcache.AddReqItem(context.Background(), rid1, &reqItem{})
	assert.True(t, rcache.cache.Contains(rid1), "unexpected nil item")
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "haven't called onClose")
	case <-close:
	}
}
