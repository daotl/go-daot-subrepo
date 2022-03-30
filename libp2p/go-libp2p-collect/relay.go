package collect

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync/atomic"

	"github.com/daotl/go-libp2p-collect/pb"
	"github.com/daotl/go-libp2p-collect/pubsub"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

type deduplicator interface {
	// markSeen returns false if resp has been seen;
	// return true if resp hasn't been seen before.
	// The response will be marked seen after markSeen callation.
	markSeen(resp *Response) bool
}

// RelayPubSubCollector .
type RelayPubSubCollector struct {
	conf          *conf
	host          host.Host
	seqno         uint64
	apubsub       *AsyncPubSub
	reqWorkerPool *requestWorkerPool
	dedup         deduplicator
	ridFn         ReqIDFn
	logger        *standardLogger
}

// NewRelayPubSubCollector .
func NewRelayPubSubCollector(h host.Host, options ...InitOpt) (r *RelayPubSubCollector, err error) {
	// TODO: add lifetime control for randomSub
	var (
		opts          *InitOpts
		conf          *conf
		reqWorkerPool *requestWorkerPool
		respCache     *responseCache
		ap            *AsyncPubSub
	)
	{
		opts, err = NewInitOpts(options)
	}
	if err == nil {
		conf, err = checkOptConfAndGetInnerConf(&opts.Conf)
	}
	if err == nil {
		reqWorkerPool, err = newRequestWorkerPool(conf.requestCacheSize)
	}
	if err == nil {
		respCache, err = newResponseCache(conf.requestCacheSize)
	}
	if err == nil {
		r = &RelayPubSubCollector{
			conf:          conf,
			host:          h,
			seqno:         rand.Uint64(),
			reqWorkerPool: reqWorkerPool,
			dedup:         respCache,
			ridFn:         opts.ReqIDFn,
			logger:        &standardLogger{opts.Logger},
		}
		ap, err = NewAsyncPubSub(
			h,
			WithSelfNotif(true),
			WithCustomPubSubFactory(
				func(h host.Host) (*pubsub.PubSub, error) {
					// psub, err := pubsub.NewRandomSub(
					// 	context.Background(),
					// 	h,
					// 	defaultRandomSubSize,
					// 	pubsub.WithCustomProtocols([]protocol.ID{conf.requestProtocol}),
					// 	pubsub.WithEventTracer((*tracer)(r)),
					// 	pubsub.WithMessageIDFn(opts.MsgIDFn),
					// )
					psub, err := pubsub.NewGossipSub(
						context.Background(),
						h,
						pubsub.WithGossipSubProtocol(conf.requestProtocol),
						pubsub.WithEventTracer((*tracer)(r)),
						pubsub.WithMessageIDFn(opts.MsgIDFn),
					)
					return psub, err
				},
			),
		)
	}
	if err == nil {
		r.apubsub = ap
		r.host.SetStreamHandler(r.conf.responseProtocol, r.responseStreamHandler)

	} else { // err != nil
		r = nil
	}

	return
}

// Join the overlay network defined by topic.
// Register RequestHandle and ResponseHandle in opts.
func (r *RelayPubSubCollector) Join(topic string, options ...JoinOpt) (err error) {

	r.logger.funcCall("debug", "Join",
		map[string]interface{}{
			"topic":  topic,
			"myself": r.host.ID().ShortString(),
		})

	var opts *JoinOpts
	{
		opts, err = NewJoinOptions(options)
	}
	// subscribe the topic
	if err == nil {
		err = r.apubsub.Subscribe(topic, r.topicHandle)
	}

	// register request handler
	if err == nil {
		err = r.apubsub.SetTopicItem(topic, requestHandlerKey, opts.RequestHandler)
	}

	if err != nil {
		r.logger.error(err)
	}

	return
}

// Publish a serialized request. Request should be encasulated in data argument.
func (r *RelayPubSubCollector) Publish(topic string, data []byte, opts ...PubOpt) (err error) {

	r.logger.funcCall("debug", "publish", map[string]interface{}{
		"topic":  topic,
		"myself": r.host.ID().ShortString(),
	})

	var (
		rqID    RequestID
		options *PubOpts
		tosend  []byte
		req     *Request
	)
	{
		options, err = NewPublishOptions(opts)
	}
	if err == nil {
		myself := r.host.ID()
		req = &Request{
			Control: pb.RequestControl{
				Requester: myself,
				Sender:    myself,
				Seqno:     atomic.AddUint64(&(r.seqno), 1),
			},
			Payload: data,
		}

		rqID = r.ridFn(req)

		// Root and From will not be transmitted on network.
		// req.Control.Requester = ""
		// req.Control.Sender = ""

		tosend, err = req.Marshal()
	}
	if err == nil {
		// register notif handler
		r.reqWorkerPool.AddReqItem(options.RequestContext, rqID, &reqItem{
			finalHandler: options.FinalRespHandle,
			topic:        topic,
			req:          req,
		})

		//  publish marshaled request
		err = r.apubsub.Publish(options.RequestContext, topic, tosend)

	}

	if err != nil {
		r.logger.error(err)
	}

	return
}

// Leave the overlay
func (r *RelayPubSubCollector) Leave(topic string) (err error) {

	r.logger.funcCall("debug", "leave", map[string]interface{}{
		"topic":  topic,
		"myself": r.host.ID().ShortString(),
	})

	err = r.apubsub.Unsubscribe(topic)
	r.reqWorkerPool.RemoveTopic(topic)

	if err != nil {
		r.logger.error(err)
	}

	return
}

// Close the BasicPubSubCollector.
func (r *RelayPubSubCollector) Close() (err error) {

	r.logger.funcCall("debug", "close", nil)

	r.reqWorkerPool.RemoveAll()
	err = r.apubsub.Close()

	if err != nil {
		r.logger.error(err)
	}

	return
}

func (r *RelayPubSubCollector) topicHandle(topic string, msg *Message) {

	r.logger.funcCall("debug", "topicHandle", map[string]interface{}{
		"topic":  topic,
		"myself": r.host.ID().ShortString(),
		"receive-from": func() string {
			if msg == nil {
				return ""
			}
			return msg.ReceivedFrom.Pretty()
		}(),
		"from": func() string {
			if msg == nil {
				return ""
			}
			pid, err := peer.IDFromBytes(msg.From)
			if err != nil {
				return ""
			}
			return pid.ShortString()
		}(),
	})

	var err error
	if msg == nil {
		err = fmt.Errorf("unexpected nil msg")
	}

	var req *Request
	{
		// unmarshal the received data into request struct
		req = &Request{}
		err = req.Unmarshal(msg.Data)
	}
	if err == nil {
		// req.Control.From and Control.Root is not transmitted on wire actually.
		// we can get it from message.From and ReceivedFrom, and then
		// we pass req to requestHandler.
		req.Control.Requester = peer.ID(msg.From)
		req.Control.Sender = msg.ReceivedFrom
	}
	var (
		ok          bool
		rqhandleRaw interface{}
		rqhandle    RequestHandler
		rqID        RequestID
	)
	if err == nil {
		rqID = r.ridFn(req)

		r.logger.message("debug", fmt.Sprintf("reqID: %v", rqID))
		// Dispatch request to relative topic request handler,
		// which should be initialized in join function
		rqhandleRaw, err = r.apubsub.LoadTopicItem(topic, requestHandlerKey)

		if err != nil {
			err = fmt.Errorf("cannot find request handler:%w", err)
		} else {
			rqhandle, ok = rqhandleRaw.(RequestHandler)
			if !ok {
				err = fmt.Errorf("unexpected request handler type")
			}
		}
	}

	var (
		item *reqItem
		resp *Response
		ctx  context.Context
	)
	if err == nil {
		// send payload
		ctx = context.Background()
		item, ok, _ = r.reqWorkerPool.GetReqItem(rqID)
		if !ok {
			item = &reqItem{
				finalHandler: func(context.Context, *Response) {},
				topic:        topic,
				req:          req,
			}
			r.reqWorkerPool.AddReqItem(ctx, rqID, item)
		} else {
			item.req = req
		}

		// handle request
		// TODO: add timeout
		rqresult := rqhandle(ctx, req)

		// After request is processed, we will have a Intermediate.
		// We send the response to the root node directly if sendback is set to true.
		// Another protocol will be used to inform the root node.
		if rqresult == nil || !rqresult.Hit {
			// drop any response if sendback is false or sendback == nil
			r.logger.message("info", "not sendback")

			return
		}

		// assemble the response
		myself := r.host.ID()
		resp = &Response{
			Control: pb.ResponseControl{
				RequestId: rqID,
				Requester: req.Control.Requester,
				Responser: myself,
				Sender:    myself,
			},
			Payload: rqresult.Payload,
		}

		// receive self-published message
		if req.Control.Requester == r.host.ID() {

			r.logger.message("info", "self publish")

			err = r.handleFinalResponse(ctx, resp)

		} else {
			r.logger.message("info", "answering response")
			err = r.handleAndForwardResponse(ctx, resp)
		}

	}

	if err != nil {
		r.logger.error(err)
	}

	return
}

func (r *RelayPubSubCollector) responseStreamHandler(s network.Stream) {

	r.logger.Logf("debug", "relay streamHandler: from: %s", s.Conn().RemotePeer().ShortString())

	var (
		respBytes []byte
		err       error
		resp      *Response
	)
	respBytes, err = ioutil.ReadAll(s)
	s.Close()
	if err == nil {
		resp = &Response{}
		err = resp.Unmarshal(respBytes)
	}
	if err == nil {
		if resp.Control.Requester == r.host.ID() {
			err = r.handleFinalResponse(context.Background(), resp)
		} else {
			err = r.handleAndForwardResponse(context.Background(), resp)
		}
	}

	if err != nil {
		r.logger.error(err)
	}
}

func (r *RelayPubSubCollector) handleAndForwardResponse(ctx context.Context, recv *Response) (err error) {

	r.logger.funcCall("debug", "handleAndForwardResponse", map[string]interface{}{
		"receive-from": func() string {
			if recv == nil {
				return ""
			}
			return recv.Control.Sender.Pretty()
		}(),
	})

	if recv == nil {
		err = fmt.Errorf("unexpect nil response")
	}

	// check response cache, deduplicate and forward
	var (
		reqID RequestID
		item  *reqItem
		ok    bool
	)
	reqID = recv.Control.RequestId
	item, ok, _ = r.reqWorkerPool.GetReqItem(reqID)
	if !ok {
		err = fmt.Errorf("cannot find reqItem for response ID: %s", reqID)
	}

	var (
		s         network.Stream
		respBytes []byte
		from      peer.ID
	)
	if err == nil && r.dedup.markSeen(recv) {
		// send back the first seen response
		{
			recv.Control.Sender = r.host.ID()
			respBytes, err = recv.Marshal()
		}
		if err == nil {
			from = peer.ID(item.req.Control.Sender)

			s, err = r.host.NewStream(context.Background(), from, r.conf.responseProtocol)
		}
		if err == nil {
			defer s.Close()
			_, err = s.Write(respBytes)
		}
	}

	if err != nil {
		r.logger.error(err)
	}

	return
}

// only called in root node
func (r *RelayPubSubCollector) handleFinalResponse(ctx context.Context, recv *Response) (err error) {

	r.logger.funcCall("debug", "handleFinalResponse", map[string]interface{}{
		"receive-from": func() string {
			if recv == nil {
				return ""
			}
			return recv.Control.Sender.Pretty()
		}(),
	})

	if recv == nil {
		err = fmt.Errorf("unexpect nil response")
	}

	var (
		reqID RequestID
		item  *reqItem
		ok    bool
	)
	reqID = recv.Control.RequestId
	item, ok, _ = r.reqWorkerPool.GetReqItem(reqID)
	if !ok {
		err = fmt.Errorf("cannot find reqItem for response ID: %s", reqID)
	}
	if err == nil && r.dedup.markSeen(recv) {
		item.finalHandler(ctx, recv)
	}

	if err != nil {
		r.logger.error(err)
	}

	return
}

type tracer RelayPubSubCollector

func (t *tracer) Trace(evt *pubsub.TraceEvent) {
}
