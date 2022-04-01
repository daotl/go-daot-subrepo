package collect

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"fmt"

	"github.com/daotl/go-libp2p-collect/pubsub"
)

// InitOpt is options used in NewBasicPubSubCollector
type InitOpt func(*InitOpts) error

// InitOpts is options used in NewBasicPubSubCollector
type InitOpts struct {
	Conf    Conf
	ReqIDFn ReqIDFn
	MsgIDFn pubsub.MsgIDFn
	Logger  Logger
	Wires   TopicWires
}

// NewInitOpts returns initopts
func NewInitOpts(opts []InitOpt) (out *InitOpts, err error) {
	out = &InitOpts{
		Conf:   MakeDefaultConf(),
		Logger: MakeDefaultLogger(),
	}
	for _, opt := range opts {
		err = opt(out)
		if err != nil {
			return nil, err
		}
	}
	// initialize reqIDFn and msgIDFn simultaneously
	if out.ReqIDFn == nil {
		out.ReqIDFn = DefaultReqIDFn
		out.MsgIDFn = DefaultMsgIDFn
	}
	return
}

// WithConf specifies configuration of pubsubcollector
func WithConf(conf Conf) InitOpt {
	return func(opts *InitOpts) error {
		opts.Conf = conf
		return nil
	}
}

// WithRequestIDGenerator .
func WithRequestIDGenerator(ridFn ReqIDFn) InitOpt {
	return func(opts *InitOpts) error {
		if ridFn == nil {
			return fmt.Errorf("unexpected nil ReqIDGenerator")
		}
		opts.ReqIDFn = ridFn
		opts.MsgIDFn = reqIDFnToMsgIDFn(ridFn)
		return nil
	}
}

// ReqIDFn is used to generate id for each request
type ReqIDFn func(*Request) RequestID

// DefaultReqIDFn returns default ReqIDGenerator.
// SHA-512 hash function is called in it.
func DefaultReqIDFn(rq *Request) RequestID {
	bin, err := rq.Marshal()
	if err != nil {
		return RequestID("")
	}
	h := sha512.New()
	out := RequestID(hex.EncodeToString(h.Sum(bin)))
	return out
}

// DefaultMsgIDFn should be used with DefaultMsgIDFn.
func DefaultMsgIDFn(pmsg *pubsub.PbMessage) string {
	h := sha512.New()
	out := string(hex.EncodeToString(h.Sum(pmsg.Data)))
	return out
}

func reqIDFnToMsgIDFn(fn ReqIDFn) pubsub.MsgIDFn {
	return func(pmsg *pubsub.PbMessage) string {
		var (
			err error
			req *Request
		)
		err = req.Unmarshal(pmsg.Data)
		if err != nil {
			return ""
		}
		return string(fn(req))
	}
}

func reqIDToMsgID(rid RequestID) string {
	return string(rid)
}

func msgIDToReqID(mid string) RequestID {
	return RequestID(mid)
}

// Logger .
// level is {"debug", "info", "warn", "error", "fatal"};
// format and args are compatable with fmt.Printf.
type Logger interface {
	Logf(level, format string, args ...interface{})
}

type emptyLogger struct{}

func (e emptyLogger) Logf(level, format string, args ...interface{}) {}

// MakeDefaultLogger .
func MakeDefaultLogger() Logger {
	return emptyLogger{}
}

// WithLogger .
func WithLogger(l Logger) InitOpt {
	return func(opts *InitOpts) error {
		opts.Logger = l
		return nil
	}
}

// JoinOpt is optional options in PubSubCollector.Join
type JoinOpt func(*JoinOpts) error

// JoinOpts is the aggregated options
type JoinOpts struct {
	RequestHandler
	ProfileFactory
}

// NewJoinOptions returns an option collection
func NewJoinOptions(opts []JoinOpt) (out *JoinOpts, err error) {
	out = &JoinOpts{
		RequestHandler: defaultRequestHandler,
		ProfileFactory: defaultProfileFactory,
	}

	for _, opt := range opts {
		if err == nil {
			err = opt(out)
		}
	}
	return
}

// RequestHandler is the callback function when receiving a request.
// It will be called in every node joined the network.
// The return value will be sent to the root (directly or relayedly).
type RequestHandler func(ctx context.Context, req *Request) *Intermediate

// WithRequestHandler registers request handler
func WithRequestHandler(rqhandle RequestHandler) JoinOpt {
	return func(opts *JoinOpts) error {
		opts.RequestHandler = rqhandle
		return nil
	}
}

func defaultRequestHandler(context.Context, *Request) *Intermediate {
	return &Intermediate{
		Hit:     false,
		Payload: []byte{},
	}
}

func WithProfileFactory(pf ProfileFactory) JoinOpt {
	return func(jo *JoinOpts) error {
		jo.ProfileFactory = pf
		return nil
	}
}

// PubOpt is optional options in PubSubCollector.Publish
type PubOpt func(*PubOpts) error

// PubOpts is the aggregated options
type PubOpts struct {
	RequestContext  context.Context
	FinalRespHandle FinalRespHandler
}

// NewPublishOptions returns an option collection
func NewPublishOptions(opts []PubOpt) (out *PubOpts, err error) {
	out = &PubOpts{
		RequestContext:  context.TODO(),
		FinalRespHandle: func(context.Context, *Response) {},
	}
	for _, opt := range opts {
		if err == nil {
			err = opt(out)
		}
	}
	return
}

// FinalRespHandler is the callback function when the root node receiving a response.
// It will be called only in the root node.
// It will be called more than one time when the number of responses is larger than one.
type FinalRespHandler func(context.Context, *Response)

// WithFinalRespHandler registers notifHandler
func WithFinalRespHandler(handler FinalRespHandler) PubOpt {
	return func(pubopts *PubOpts) error {
		pubopts.FinalRespHandle = handler
		return nil
	}
}

// WithRequestContext adds cancellation or timeout for a request
// default is withCancel. (ctx will be cancelled when request is closed)
func WithRequestContext(ctx context.Context) PubOpt {
	return func(pubopts *PubOpts) error {
		pubopts.RequestContext = ctx
		return nil
	}
}
