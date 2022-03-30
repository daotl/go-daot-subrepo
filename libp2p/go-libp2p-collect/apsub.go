package collect

import (
	"context"
	"fmt"
	"sync"

	pubsub "github.com/daotl/go-libp2p-collect/pubsub"
	host "github.com/libp2p/go-libp2p-core/host"
)

const (
	requestHandlerKey    = 'q'
	responseHandlerKey   = 'p'
	defaultRandomSubSize = 10
)

// TopicHandle is the handle function of subscription.
// WARNING: DO NOT change msg, if a msg includes multiple topics, they share a message.
type TopicHandle func(topic string, msg *Message)

// Message is type alias
type Message = pubsub.Message

// AsyncPubSub encapsulates pubsub, provides async methods to get subscribe messages.
// AsyncPubSub also manages the joined topics
type AsyncPubSub struct {
	lock      sync.RWMutex
	items     map[string]*topicitem
	pubs      *pubsub.PubSub
	host      host.Host
	selfNotif bool
}

type topicitem struct {
	name  string
	topic *pubsub.Topic
	// storage is useful to storage user-defined topic-related item
	storage map[interface{}]interface{}
	// ctxcancel is called to cancel context
	ctxcancel func()
	// subcancel is called to cancel subscription
	subcancel func()
}

// TopicOpt is the option in NewTopics
type TopicOpt func(*AsyncPubSub) error

// PubSubFactory initializes a pubsub in Topics
type PubSubFactory func(h host.Host) (*pubsub.PubSub, error)

// WithCustomPubSubFactory initials the pubsub based on given factory
func WithCustomPubSubFactory(psubFact PubSubFactory) TopicOpt {
	return func(t *AsyncPubSub) (err error) {
		psub, err := psubFact(t.host)
		if err == nil {
			t.pubs = psub
		}
		return
	}
}

// WithSelfNotif decides whether a node receives self-published messages.
// If WithSelfNotif is set to true, the node will receive messages published by itself.
// Default is set to false.
func WithSelfNotif(enable bool) TopicOpt {
	return func(t *AsyncPubSub) (err error) {
		t.selfNotif = enable
		return
	}
}

// NewAsyncPubSub returns a new Topics instance.
// If WithCustomPubSubFactory is not set, a default randomsub will be used.
func NewAsyncPubSub(h host.Host, opts ...TopicOpt) (apsub *AsyncPubSub, err error) {
	t := &AsyncPubSub{
		lock:      sync.RWMutex{},
		items:     make(map[string]*topicitem),
		host:      h,
		selfNotif: false,
	}
	for _, opt := range opts {
		if err == nil {
			err = opt(t)
		}
	}

	// if pubs is not set, use the default initialization
	if err == nil && t.pubs == nil {
		t.pubs, err = pubsub.NewRandomSub(context.Background(), h, defaultRandomSubSize)
	}

	if err == nil {
		apsub = t
	}
	return
}

// Close the topics
func (ap *AsyncPubSub) Close() error {
	defer ap.lock.Unlock()
	/*_*/ ap.lock.Lock()

	for k := range ap.items {
		ap.unsubscribe(k)
	}
	return nil
}

// Publish a message with given topic
func (ap *AsyncPubSub) Publish(ctx context.Context, topic string, data []byte) (err error) {
	defer ap.lock.RUnlock()
	/*_*/ ap.lock.RLock()
	// lock here to avoid unexpected join in slow publish
	var ok bool
	if err == nil {
		ok, err = ap.fastPublish(ctx, topic, data)
	}
	if err == nil && !ok {
		err = ap.slowPublish(ctx, topic, data)
	}
	return
}

func (ap *AsyncPubSub) fastPublish(ctx context.Context, topic string, data []byte) (ok bool, err error) {
	var it *topicitem
	it, ok = ap.items[topic]
	if ok {
		err = it.topic.Publish(ctx, data)
	}
	return
}

// we assume the given topic is not joint yet.
// after the message is published, quit the topic.
func (ap *AsyncPubSub) slowPublish(ctx context.Context, topic string, data []byte) (err error) {
	var t *pubsub.Topic
	t, err = ap.pubs.Join(topic)
	if err == nil {
		defer t.Close()
		err = t.Publish(ctx, data)
	}
	return
}

// Subscribe a topic
// Subscribe a same topic is ok, but the previous handle will be replaced.
func (ap *AsyncPubSub) Subscribe(topic string, handle TopicHandle) (err error) {
	defer ap.lock.Unlock()
	/*_*/ ap.lock.Lock()

	it, ok := ap.items[topic]
	if !ok {
		it = &topicitem{
			name:      topic,
			storage:   make(map[interface{}]interface{}),
			subcancel: func() {},
			ctxcancel: func() {},
		}
		// Join should be called only once
		it.topic, err = ap.pubs.Join(topic)
	}

	var sub *pubsub.Subscription
	if err == nil {
		sub, err = it.topic.Subscribe()
	}

	var ctx context.Context
	if err == nil {
		it.subcancel()
		it.ctxcancel()
		it.subcancel = sub.Cancel
		ctx, it.ctxcancel = context.WithCancel(context.TODO())

		ap.items[topic] = it
		go ap.forwardTopic(ctx, sub, topic, handle)
	}

	return
}

// Unsubscribe the given topic
func (ap *AsyncPubSub) Unsubscribe(topic string) (err error) {
	defer ap.lock.Unlock()
	/*_*/ ap.lock.Lock()
	return ap.unsubscribe(topic)
}

func (ap *AsyncPubSub) unsubscribe(topic string) (err error) {
	it, ok := ap.items[topic]
	if ok {
		if it.subcancel != nil {
			it.subcancel()
		}
		if it.ctxcancel != nil {
			it.ctxcancel()
		}
		// be careful here.
		// topic.Close() will return an error if there are active subscription.
		if it.topic != nil {
			err = it.topic.Close()
		}

		delete(ap.items, topic)
	}
	return
}

func (ap *AsyncPubSub) forwardTopic(ctx context.Context, sub *pubsub.Subscription, topic string, f TopicHandle) {
	for {
		msg, err := sub.Next(ctx)
		if err == nil {
			err = ctx.Err()
		}

		if err == nil {
			if msg.ReceivedFrom != ap.host.ID() || ap.selfNotif {
				go f(topic, msg)
			}
		} else {
			break
		}
	}
}

// SetTopicItem .
func (ap *AsyncPubSub) SetTopicItem(topic string, key interface{}, value interface{}) error {
	defer ap.lock.Unlock()
	ap.lock.Lock()
	it, ok := ap.items[topic]
	if !ok {
		return fmt.Errorf("can not find topic %s", topic)
	}
	it.storage[key] = value
	return nil
}

// LoadTopicItem .
func (ap *AsyncPubSub) LoadTopicItem(topic string, key interface{}) (value interface{}, err error) {
	defer ap.lock.RUnlock()
	ap.lock.RLock()
	it, ok := ap.items[topic]
	if !ok {
		return nil, fmt.Errorf("can not find topic %s", topic)
	}
	value, ok = it.storage[key]
	if !ok {
		return nil, fmt.Errorf("can not find value in topic %s", topic)
	}
	return
}

// Topics returns the subscribed topics
func (ap *AsyncPubSub) Topics() (out []string) {
	defer ap.lock.RUnlock()
	ap.lock.RLock()
	for k := range ap.items {
		out = append(out, k)
	}
	return out
}
