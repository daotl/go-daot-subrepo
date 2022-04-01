package collect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daotl/go-libp2p-collect/mock"
	"github.com/stretchr/testify/assert"
)

/*===========================================================================*/

type testLogger testing.T

func (l *testLogger) Logf(level, format string, args ...interface{}) {
	(*testing.T)(l).Logf("%s:", level)
	(*testing.T)(l).Logf(format, args...)
}

/*===========================================================================*/

func makeRouterConf(name string) Conf {
	conf := MakeDefaultConf()
	conf.Router = name
	return conf
}

func inSkips(cand string, skips []string) bool {
	for _, skip := range skips {
		if cand == skip {
			return true
		}
	}
	return false
}

/*===========================================================================*/

var tconf Conf = makeRouterConf("intbfs")

// cases

func testSendRecvData(t *testing.T, payload []byte) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewCollector(pubhost, WithConf(tconf))
	assert.NoError(t, err)
	sub, err := NewCollector(subhost, WithConf(tconf))
	assert.NoError(t, err)
	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"

	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), r.Control.Requester)
		out := &Intermediate{
			Hit:     false,
			Payload: payload,
		}
		return out
	}
	handleSub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), r.Control.Requester)
		out := &Intermediate{
			Hit:     true,
			Payload: payload,
		}
		return out
	}

	err = pub.Join(topic, WithRequestHandler(handlePub))
	assert.NoError(t, err)
	err = sub.Join(topic, WithRequestHandler(handleSub))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	okch := make(chan struct{})
	notif := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payload, rp.Payload)
		okch <- struct{}{}
	}
	err = pub.Publish(topic, payload, WithFinalRespHandler(notif))
	assert.NoError(t, err)

	// after 2 seconds, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}
}

func testSelfPub(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pub, err := NewCollector(pubhost, WithConf(tconf), WithLogger((*testLogger)(t)))
	assert.NoError(t, err)

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), r.Control.Requester)
		out := &Intermediate{
			Hit:     true,
			Payload: payload,
		}
		return out
	}

	err = pub.Join(topic, WithRequestHandler(handlePub))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	okch := make(chan struct{})
	notif := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payload, rp.Payload)
		okch <- struct{}{}
	}
	err = pub.Publish(topic, payload, WithFinalRespHandler(notif))
	assert.NoError(t, err)

	// after 2 seconds, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}
}

func testRejoin(t *testing.T) {
	mnet := mock.NewMockNet()
	h, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pub, err := NewCollector(h, WithConf(tconf))
	assert.NoError(t, err)

	topic := "test-topic"
	payload := []byte{1, 2, 3}
	another := []byte{4, 5, 6}

	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, h.ID(), r.Control.Requester)
		out := &Intermediate{
			Hit:     true,
			Payload: payload,
		}
		return out
	}
	anotherHandle := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, h.ID(), r.Control.Requester)
		out := &Intermediate{
			Hit:     true,
			Payload: another,
		}
		return out
	}

	err = pub.Join(topic, WithRequestHandler(handlePub))
	assert.NoError(t, err)

	// We join the same topic for a second time.
	err = pub.Join(topic, WithRequestHandler(anotherHandle))
	assert.NoError(t, err)

	okch := make(chan struct{})
	notif := func(ctx context.Context, rp *Response) {
		assert.Equal(t, another, rp.Payload)
		okch <- struct{}{}
	}
	err = pub.Publish(topic, payload, WithFinalRespHandler(notif))
	assert.NoError(t, err)

	// after 2 seconds, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}
}

func testLeaveAndJoin(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewCollector(pubhost, WithConf(tconf), WithLogger((*testLogger)(t)))
	assert.NoError(t, err)
	sub, err := NewCollector(subhost, WithConf(tconf), WithLogger((*testLogger)(t)))
	assert.NoError(t, err)
	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), r.Control.Requester)
		out := &Intermediate{
			Hit:     false,
			Payload: payload,
		}
		return out
	}
	handleSub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), r.Control.Requester)
		out := &Intermediate{
			Hit:     true,
			Payload: payload,
		}
		return out
	}

	err = pub.Join(topic, WithRequestHandler(handlePub))
	assert.NoError(t, err)
	err = sub.Join(topic, WithRequestHandler(handleSub))
	assert.NoError(t, err)
	// time to join
	time.Sleep(50 * time.Millisecond)

	okch := make(chan struct{})

	notifOK := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payload, rp.Payload)
		okch <- struct{}{}
	}
	err = pub.Publish(topic, payload, WithFinalRespHandler(notifOK))
	assert.NoError(t, err)

	// after 2 seconds, if okch receive nothing, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}

	// let sub leave the topic
	err = sub.Leave(topic)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	notifFail := func(ctx context.Context, rp *Response) {
		assert.FailNow(t, "should not recv message")
	}
	err = pub.Publish(topic, payload, WithFinalRespHandler(notifFail))
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	// let sub join again
	err = sub.Join(topic, WithRequestHandler(handleSub))
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	err = pub.Publish(topic, payload, WithFinalRespHandler(notifOK))
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	// after 2 seconds, if okch receive nothing, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}
}

func testRequestContextExpired(t *testing.T) {
	// FinalResponseHandler shouldn't be called after the request context expires.
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pub, err := NewCollector(pubhost, WithConf(tconf))
	assert.NoError(t, err)
	sub, err := NewCollector(subhost, WithConf(tconf))
	assert.NoError(t, err)
	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	cctx, cc := context.WithCancel(context.Background())
	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *Request) *Intermediate {
		out := &Intermediate{
			Hit:     false,
			Payload: payload,
		}
		return out
	}
	handleSub := func(ctx context.Context, r *Request) *Intermediate {
		// cancel the request context
		cc()
		// wait for cancellation
		time.Sleep(50 * time.Millisecond)

		out := &Intermediate{
			Hit:     true,
			Payload: payload,
		}
		return out
	}

	err = pub.Join(topic, WithRequestHandler(handlePub))
	assert.NoError(t, err)
	err = sub.Join(topic, WithRequestHandler(handleSub))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	notif := func(ctx context.Context, rp *Response) {
		assert.Fail(t, "unexpected callation of finalRespHandler")
	}

	err = pub.Publish(
		topic,
		payload,
		WithFinalRespHandler(notif),
		WithRequestContext(cctx))
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
}

func testFinalDeduplication(t *testing.T) {
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
	a, err := NewCollector(ahost, WithConf(tconf))
	assert.NoError(t, err)
	b, err := NewCollector(bhost, WithConf(tconf))
	assert.NoError(t, err)
	c, err := NewCollector(bhost, WithConf(tconf))
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

func testDeduplication(t *testing.T) {
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

	pscA, err := NewCollector(hostA, WithConf(tconf))
	assert.NoError(t, err)
	pscB, err := NewCollector(hostB, WithConf(tconf))
	assert.NoError(t, err)
	pscC, err := NewCollector(hostC, WithConf(tconf))
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

func testSendRecv(t *testing.T) {
	testSendRecvData(t, []byte{1, 2, 3})
}

func testSendRecvBig(t *testing.T) {
	big := make([]byte, 1000000) // 1M
	testSendRecvData(t, big)
}

/*========================================================================*/
// settings
func TestAll(t *testing.T) {

	routerConfs := []struct {
		name string
		conf Conf
	}{
		{
			name: "basic",
			conf: makeRouterConf("basic"),
		},
		{
			name: "relay",
			conf: makeRouterConf("relay"),
		},
		//{
		//	name: "intbfs",
		//	conf: makeRouterConf("intbfs"),
		//},
	}

	tcases := []struct {
		name  string
		tcase func(t *testing.T)
		skips []string
	}{
		{
			name:  "TestLeaveAndJoin",
			tcase: testLeaveAndJoin,
			skips: []string{"relay"},
		},
		//{
		//	name:  "TestRejoin",
		//	tcase: testRejoin,
		//	skips: []string{"intbfs"},
		//},
		{
			name:  "TestSelfPub",
			tcase: testSelfPub,
		},
		{
			name:  "TestSendRecv",
			tcase: testSendRecv,
		},
		{
			name:  "TestRequestContextExpired",
			tcase: testRequestContextExpired,
		},

		{
			name:  "TestFinalDeduplication",
			tcase: testFinalDeduplication,
		},
		//{
		//	name:  "TestDeduplication",
		//	tcase: testDeduplication,
		//	skips: []string{"basic", "intbfs"},
		//},
		{
			name:  "TestSendRecvBig",
			tcase: testSendRecvBig,
		},
	}

	/*========================================================================*/
	// run it!

	for _, rc := range routerConfs {
		tconf = makeRouterConf(rc.name)
		for _, tc := range tcases {
			if inSkips(rc.name, tc.skips) {
				continue
			}
			t.Run(rc.name+"/"+tc.name, func(t *testing.T) {
				tc.tcase(t)
			})
		}
	}

}
