package collect

import (
	"context"
	"hash/fnv"

	lru "github.com/hashicorp/golang-lru"
)

// requestCache .
// requestCache is used to store the request control message,
// which is for response routing.
// TODO: rename it to requestCache.
type requestWorkerPool struct {
	cache *lru.Cache
}

// reqstItem .
type reqItem struct {
	finalHandler FinalRespHandler
	topic        string
	req          *Request
}

type reqWorker struct {
	item    *reqItem
	cc      func()
	onClose func()
}

func newReqWorker(ctx context.Context, item *reqItem, onClose func()) *reqWorker {
	cctx, cc := context.WithCancel(ctx)
	r := &reqWorker{
		item:    item,
		cc:      cc,
		onClose: onClose,
	}
	go func(ctx context.Context, r *reqWorker) {
		select {
		case <-ctx.Done():
			if r.onClose != nil {
				r.onClose()
			}
			return
		}
	}(cctx, r)
	return r
}

// callback when a request is evicted.
func onReqCacheEvict(key interface{}, value interface{}) {
	// cancel context
	value.(*reqWorker).cc()
}

// newRequestWorkerPool .
func newRequestWorkerPool(size int) (*requestWorkerPool, error) {
	l, err := lru.NewWithEvict(size, onReqCacheEvict)
	return &requestWorkerPool{
		cache: l,
	}, err
}

// AddReqItem .
// Add with the same reqid will make the previous one cancelled.
// When context is done, item will be removed from the cache;
// When the item is evicted, the context will be cancelled.
func (rc *requestWorkerPool) AddReqItem(ctx context.Context, reqid RequestID, item *reqItem) {
	rc.RemoveReqItem(reqid)
	w := newReqWorker(ctx, item, func() {
		rc.RemoveReqItem(reqid)
	})
	rc.cache.Add(reqid, w)
}

// RemoveReqItem .
func (rc *requestWorkerPool) RemoveReqItem(reqid RequestID) {
	rc.cache.Remove(reqid)
}

// GetReqItem .
func (rc *requestWorkerPool) GetReqItem(reqid RequestID) (out *reqItem, ok bool, cancel func()) {
	var w *reqWorker
	w, ok = rc.GetReqWorker(reqid)
	if w != nil {
		out, cancel = w.item, w.cc
	}
	return
}

// GetReqItem .
func (rc *requestWorkerPool) GetReqWorker(reqid RequestID) (w *reqWorker, ok bool) {
	var v interface{}
	v, ok = rc.cache.Get(reqid)
	if ok {
		w = v.(*reqWorker)
	}
	return
}

// RemoveTopic .
func (rc *requestWorkerPool) RemoveTopic(topic string) {
	for _, k := range rc.cache.Keys() {
		if v, ok := rc.cache.Peek(k); ok {
			w := v.(*reqWorker)
			if w.item.topic == topic {
				rc.cache.Remove(k)
			}
		}
	}
}

// RemoveAll .
func (rc *requestWorkerPool) RemoveAll() {
	rc.cache.Purge()
}

// ResponseCache .
// ResponseCache is used to deduplicate the response
type responseCache struct {
	cache *lru.Cache
}

// respItem .
type respItem struct{}

// newResponseCache .
func newResponseCache(size int) (*responseCache, error) {
	l, err := lru.New(size)
	return &responseCache{
		cache: l,
	}, err
}

func (r *responseCache) markSeen(resp *Response) bool {
	var (
		err   error
		hash  uint64
		found bool
	)
	if err == nil {
		s := fnv.New64()
		_, err = s.Write(resp.Payload)
		_, err = s.Write([]byte(resp.Control.RequestId))
		hash = s.Sum64()
	}
	if err == nil {
		_, found = r.cache.Get(hash)
	}
	if err == nil && !found {
		r.cache.Add(hash, respItem{})
		return true
	}
	return false
}
