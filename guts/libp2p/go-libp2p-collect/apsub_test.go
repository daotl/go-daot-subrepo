package collect

import (
	"context"
	"testing"
	"time"

	"github.com/daotl/go-libp2p-collect/mock"
	"github.com/stretchr/testify/assert"
)

func TestPubSub(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewAsyncPubSub(pubhost)
	assert.NoError(t, err)
	sub, err := NewAsyncPubSub(subhost)
	assert.NoError(t, err)
	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

	// wait for discovery
	time.Sleep(50 * time.Millisecond)

	expecttopic := "test-topic"
	expectdata := []byte{1, 2, 3}
	okch := make(chan struct{})
	subhandle := func(topic string, msg *Message) {
		assert.Equal(t, expecttopic, topic)
		assert.Equal(t, expectdata, msg.Data)
		// to make sure handle is called
		okch <- struct{}{}
	}
	err = sub.Subscribe(expecttopic, subhandle)
	assert.NoError(t, err)
	// wait for subscription
	time.Sleep(50 * time.Millisecond)
	err = pub.Publish(context.TODO(), expecttopic, expectdata)
	assert.NoError(t, err)
	select {
	case <-time.After(1 * time.Second):
		assert.Fail(t, "subhandle is not called in 1s")
	case <-okch:
	}
}

func TestPublishAfterSubscription(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewAsyncPubSub(pubhost)
	assert.NoError(t, err)
	sub, err := NewAsyncPubSub(subhost)
	assert.NoError(t, err)
	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

	// wait for discovery
	time.Sleep(50 * time.Millisecond)

	expecttopic := "test-topic"
	expectdata := []byte{1, 2, 3}
	okch := make(chan struct{})
	pubhandle := func(topic string, msg *Message) {}
	subhandle := func(topic string, msg *Message) {
		assert.Equal(t, expecttopic, topic)
		assert.Equal(t, expectdata, msg.Data)
		// to make sure handle is called
		okch <- struct{}{}
	}
	err = pub.Subscribe(expecttopic, pubhandle)
	assert.NoError(t, err)
	err = sub.Subscribe(expecttopic, subhandle)
	assert.NoError(t, err)
	// wait for subscription
	time.Sleep(50 * time.Millisecond)
	err = pub.Publish(context.TODO(), expecttopic, expectdata)
	assert.NoError(t, err)
	select {
	case <-time.After(1 * time.Second):
		assert.Fail(t, "subhandle is not called in 1s")
	case <-okch:
	}
}

func TestReSubcription(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewAsyncPubSub(pubhost)
	assert.NoError(t, err)
	sub, err := NewAsyncPubSub(subhost)
	assert.NoError(t, err)
	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

	// wait for discovery
	time.Sleep(50 * time.Millisecond)

	expecttopic := "test-topic"
	expectdata := []byte{1, 2, 3}
	okch := make(chan struct{})
	emptyhandle := func(topic string, msg *Message) {
		assert.Fail(t, "unexpected call of empty handle")
	}
	subhandle := func(topic string, msg *Message) {
		assert.Equal(t, expecttopic, topic)
		assert.Equal(t, expectdata, msg.Data)
		// to make sure handle is called
		okch <- struct{}{}
	}
	err = sub.Subscribe(expecttopic, emptyhandle)
	assert.NoError(t, err)
	// resubscribe
	err = sub.Subscribe(expecttopic, subhandle)
	assert.NoError(t, err)
	// wait for subscription
	time.Sleep(50 * time.Millisecond)
	err = pub.Publish(context.TODO(), expecttopic, expectdata)
	assert.NoError(t, err)
	select {
	case <-time.After(1 * time.Second):
		assert.Fail(t, "subhandle is not called in 1s")
	case <-okch:
	}
}

func TestDoubleSlowPublish(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pub, err := NewAsyncPubSub(pubhost)
	assert.NoError(t, err)

	// wait for discovery
	time.Sleep(50 * time.Millisecond)

	expecttopic := "test-topic"
	expectdata := []byte{1, 2, 3}

	err = pub.Publish(context.TODO(), expecttopic, expectdata)
	assert.NoError(t, err)

	err = pub.Publish(context.TODO(), expecttopic, expectdata)
	assert.NoError(t, err)

}

func TestGetAndSetItem(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewAsyncPubSub(pubhost)
	assert.NoError(t, err)

	topic := "test-topic"

	emptyhandle := func(topic string, msg *Message) {
		assert.Fail(t, "unexpected call of empty handle")
	}

	err = pub.Subscribe(topic, emptyhandle)
	assert.NoError(t, err)

	expect := "value"
	pub.SetTopicItem(topic, "key", expect)

	actual, err := pub.LoadTopicItem(topic, "key")
	assert.NoError(t, err)
	assert.Equal(t, expect, actual)
}

func TestUnsubscribe(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewAsyncPubSub(pubhost)
	assert.NoError(t, err)
	sub, err := NewAsyncPubSub(subhost)
	assert.NoError(t, err)
	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

	// wait for discovery
	time.Sleep(50 * time.Millisecond)

	expecttopic := "test-topic"
	expectdata := []byte{1, 2, 3}
	failhandle := func(topic string, msg *Message) {
		assert.FailNow(t, "unexpected call of handle")
	}
	err = sub.Subscribe(expecttopic, failhandle)
	assert.NoError(t, err)
	// wait for subscription
	time.Sleep(10 * time.Millisecond)
	// Leave
	err = sub.Unsubscribe(expecttopic)
	assert.NoError(t, err)
	// wait for subscription
	time.Sleep(10 * time.Millisecond)
	err = pub.Publish(context.TODO(), expecttopic, expectdata)
	assert.NoError(t, err)
	select {
	case <-time.After(300 * time.Millisecond):
	}
}
