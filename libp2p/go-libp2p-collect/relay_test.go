package collect

import (
	"context"
	"testing"
	"time"

	"sync/atomic"

	"github.com/daotl/go-libp2p-collect/mock"
	"github.com/stretchr/testify/assert"
)

func TestRelayFinalDeduplication(t *testing.T) {
	// A -- B
	// |
	// C
	// B,C return the same response to A, A drop the second one.
	mnet := mock.NewMockNet()
	ahost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	bhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	chost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	a, err := NewRelayPubSubCollector(ahost)
	assert.NoError(t, err)
	b, err := NewRelayPubSubCollector(bhost)
	assert.NoError(t, err)
	c, err := NewRelayPubSubCollector(bhost)
	assert.NoError(t, err)

	mnet.ConnectPeers(ahost.ID(), bhost.ID())
	mnet.ConnectPeers(ahost.ID(), chost.ID())

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	handle := func(ctx context.Context, req *Request) *Intermediate {
		out := &Intermediate{
			Hit:     true,
			Payload: payload,
		}
		return out
	}

	err = a.Join(topic)
	assert.NoError(t, err)
	err = b.Join(topic, WithRequestHandler(handle))
	assert.NoError(t, err)
	err = c.Join(topic, WithRequestHandler(handle))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	// add count by 1 when final handler is called
	count := int32(0)
	finalHandler := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payload, rp.Payload)
		atomic.AddInt32(&count, 1)
	}
	err = a.Publish(topic, payload, WithFinalRespHandler(finalHandler))
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, atomic.LoadInt32(&count), int32(1))
}

// tuple nodes' tests

func TestRelayDeduplication(t *testing.T) {
	// A -- B -- C
	// A broadcasts, then B and C response to it with the same content;
	// B should be able to intercept the response from C, because it knows
	// it is a duplicated response by checking its response cache.
	mnet := mock.NewMockNet()
	hostA, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	hostB, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	hostC, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pscA, err := NewRelayPubSubCollector(hostA)
	assert.NoError(t, err)
	pscB, err := NewRelayPubSubCollector(hostB)
	assert.NoError(t, err)
	pscC, err := NewRelayPubSubCollector(hostC)
	assert.NoError(t, err)
	_, err = mnet.ConnectPeers(hostA.ID(), hostB.ID())
	assert.NoError(t, err)
	_, err = mnet.ConnectPeers(hostB.ID(), hostC.ID())
	assert.NoError(t, err)

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	handle := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, hostA.ID(), r.Control.Requester)
		out := &Intermediate{
			Hit:     true,
			Payload: payload,
		}
		return out
	}

	err = pscA.Join(topic)
	assert.NoError(t, err)
	err = pscB.Join(topic, WithRequestHandler(handle))
	assert.NoError(t, err)
	err = pscC.Join(topic, WithRequestHandler(handle))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	// add count by 1 when final handler is called
	count := int32(0)
	finalHandler := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payload, rp.Payload)
		atomic.AddInt32(&count, 1)
	}
	err = pscA.Publish(topic, payload, WithFinalRespHandler(finalHandler))
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, atomic.LoadInt32(&count), int32(1))
}

func TestNoRequestIDForResponse(t *testing.T) {
	// A -- B -- C
	// A broadcasts a request R, and B should know R's route;
	// what will happen if R expired in B,
	// and at the same time C send R's response to B?
	// We have some strategy to skip the forgetful node.
	// for example, we can forward this response to a random node.
	// But now, we just drop it.
	mnet := mock.NewMockNet()
	hostA, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	hostC, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	hostB, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pscA, err := NewRelayPubSubCollector(hostA)
	assert.NoError(t, err)
	pscB, err := NewRelayPubSubCollector(hostB)
	assert.NoError(t, err)
	pscC, err := NewRelayPubSubCollector(hostC)
	assert.NoError(t, err)
	_, err = mnet.ConnectPeers(hostA.ID(), hostB.ID())
	assert.NoError(t, err)
	_, err = mnet.ConnectPeers(hostB.ID(), hostC.ID())
	assert.NoError(t, err)

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}
	payloadB := []byte{1, 2, 3}
	payloadC := []byte{3, 2, 1}

	recvB := int32(0)
	handlerForB := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, hostA.ID(), r.Control.Requester)
		assert.Equal(t, hostA.ID(), r.Control.Sender, "B is expected to receive req from A")
		atomic.StoreInt32(&recvB, 1)
		out := &Intermediate{
			Hit:     true,
			Payload: payloadB,
		}
		return out
	}
	okch := make(chan struct{}, 1)

	recvC := int32(0)
	handlerForC := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, hostA.ID(), r.Control.Requester)
		assert.Equal(t, hostB.ID(), r.Control.Sender, "C is expected to receive req from B")
		atomic.StoreInt32(&recvC, 1)
		out := &Intermediate{
			Hit:     true,
			Payload: payloadC,
		}
		// wait until A has received response from B, now it's safe to clear B's request cache
		// B will not forward C's response to A
		<-okch
		pscB.reqWorkerPool.RemoveAll()
		return out
	}

	err = pscA.Join(topic)
	assert.NoError(t, err)
	err = pscB.Join(topic, WithRequestHandler(handlerForB))
	assert.NoError(t, err)
	err = pscC.Join(topic, WithRequestHandler(handlerForC))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	// add count by 1 when final handler is called
	count := int32(0)
	finalHandler := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payloadB, rp.Payload)
		atomic.AddInt32(&count, 1)
		okch <- struct{}{}
	}
	err = pscA.Publish(topic, payload, WithFinalRespHandler(finalHandler))
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, atomic.LoadInt32(&count), int32(1), "A should receive only 1 response")
	assert.Equal(t, atomic.LoadInt32(&recvB), int32(1))
	assert.Equal(t, atomic.LoadInt32(&recvC), int32(1))
}
