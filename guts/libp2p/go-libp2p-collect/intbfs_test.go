package collect

import (
	"context"
	"testing"
	"time"

	"github.com/daotl/go-libp2p-collect/mock"
	"github.com/stretchr/testify/assert"
)

/*===========================================================================*/
// intbfs test
/*===========================================================================*/

func mustNewIntBFS(mnet *mock.Net, opts *IntBFSOptions) *IntBFS {
	host := mnet.MustNewLinkedPeer()
	wire := NewHostWires(host)
	intbfs, err := NewIntBFS(wire, opts)
	if err != nil {
		panic(err)
	}
	err = mnet.ConnectAllButSelf(host)
	if err != nil {
		panic(err)
	}
	return intbfs
}

func mustNewIntBFSCollector(mnet *mock.Net) *IntBFSCollector {
	h, err := mnet.NewConnectedPeer()
	if err != nil {
		panic(err)
	}
	c, err := NewIntBFSCollector(h)
	if err != nil {
		panic(err)
	}
	return c
}

func TestIntBFS(t *testing.T) {
	t.Skip("IntBFS not yet migrated.")

	t.Run("ReqSendRecv", func(t *testing.T) {
		mnet := mock.NewMockNet()
		pubCh := make(chan struct{}, 1)
		subCh := make(chan struct{}, 1)
		data := []byte{1, 2, 3}
		pubOpts := MakeDefaultIntBFSOptions()
		pubOpts.RequestHandler = func(ctx context.Context, req *Request) *Intermediate {
			assert.Equal(t, data, req.Payload)
			pubCh <- struct{}{}
			return nil
		}
		subOpts := MakeDefaultIntBFSOptions()
		subOpts.RequestHandler = func(ctx context.Context, req *Request) *Intermediate {
			assert.Equal(t, data, req.Payload)
			subCh <- struct{}{}
			return nil
		}

		pubIntBFS := mustNewIntBFS(mnet, pubOpts)
		subIntBFS := mustNewIntBFS(mnet, subOpts)

		time.Sleep(50 * time.Millisecond)

		defer pubIntBFS.Close()
		defer subIntBFS.Close()

		var err error
		err = pubIntBFS.Publish(data)
		if err != nil {
			panic(err)
		}
		select {
		case <-pubCh:
		case <-time.After(1 * time.Second):
			t.Fatal("pub cannot receive data")
		}
		select {
		case <-subCh:
		case <-time.After(1 * time.Second):
			t.Fatal("sub cannot receive data")
		}
	})

	t.Run("RespSendRecv", func(t *testing.T) {
		mnet := mock.NewMockNet()
		okCh := make(chan struct{}, 1)
		data := []byte{1, 2, 3}
		pubOpts := MakeDefaultIntBFSOptions()
		pubOpts.RequestHandler = func(ctx context.Context, req *Request) *Intermediate {
			return &Intermediate{
				Hit: false,
			}
		}
		subOpts := MakeDefaultIntBFSOptions()
		subOpts.RequestHandler = func(ctx context.Context, req *Request) *Intermediate {
			return &Intermediate{
				Hit:     true,
				Payload: req.Payload,
			}
		}

		pubIntBFS := mustNewIntBFS(mnet, pubOpts)
		subIntBFS := mustNewIntBFS(mnet, subOpts)
		defer pubIntBFS.Close()
		defer subIntBFS.Close()

		var err error
		err = pubIntBFS.Publish(data, WithFinalRespHandler(
			func(c context.Context, r *Response) {
				assert.Equal(t, data, r.Payload)
				okCh <- struct{}{}
			}))
		if err != nil {
			panic(err)
		}
		select {
		case <-okCh:
		case <-time.After(1 * time.Second):
			t.Fatal("pub cannot receive data")
		}
	})

	t.Run("CollectorSendRecv", func(t *testing.T) {
		mnet := mock.NewMockNet()
		pubhost, err := mnet.NewLinkedPeer()
		subhost, err := mnet.NewLinkedPeer()

		// Even if hosts are connected,
		// the topics may not find the pre-exist connections.
		// We establish connections after topics are created.
		pub, err := NewIntBFSCollector(pubhost)
		assert.NoError(t, err)
		sub, err := NewIntBFSCollector(subhost)
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
	})
}
