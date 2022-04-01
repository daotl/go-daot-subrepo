package pubsub

import (
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

//Message is type alias.
type Message = pubsub.Message

//PbMessage is type alias.
type PbMessage = pubsub_pb.Message

//PubSub is type alias.
type PubSub = pubsub.PubSub

//Topic is type alias.
type Topic = pubsub.Topic

//Subscription is type alias.
type Subscription = pubsub.Subscription

//TraceEvent is type alias.
type TraceEvent = pubsub_pb.TraceEvent

// Notif is type alias.
type Notif = pubsub.PubSubNotif

// MsgIDFn is type alias.
type MsgIDFn = pubsub.MsgIdFunction

// function aliases
var (
	NewRandomSub              = pubsub.NewRandomSub
	NewRandomSubWithProtocols = pubsub.NewRandomSubWithProtocols
	NewGossipSub              = pubsub.NewGossipSub
	WithEventTracer           = pubsub.WithEventTracer
	WithMessageIDFn           = pubsub.WithMessageIdFn
)

// variable aliases
var (
	FloodSubID = pubsub.FloodSubID
)

// comment out
// NewTopicWires       = pubsub.NewPubSubTopicWires
// WithCustomProtocols = pubsub.WithCustomProtocols
// type TopicWireListener = pubsub.TopicWireListener
// type TopicMsgHandler = pubsub.TopicMsgHandler

type TopicWireListener = interface{}
type TopicMsgHandler = interface{}
