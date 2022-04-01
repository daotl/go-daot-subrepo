package collect

import (
	"bufio"
	"context"

	host "github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	HostWiresProcotolID = "/hostwires/1.0.0"
)

type HostWires struct {
	host.Host
	up     func(peer.ID)
	down   func(peer.ID)
	msghdl MsgHandler
}

func NewHostWires(h host.Host) *HostWires {
	out := &HostWires{
		Host:   h,
		up:     func(p peer.ID) {},
		down:   func(p peer.ID) {},
		msghdl: func(from peer.ID, data []byte) {},
	}
	h.Network().Notify(out)
	h.SetStreamHandler(protocol.ID(HostWiresProcotolID), out.handleStream)
	return out
}

func (hw *HostWires) ID() peer.ID {
	return hw.Host.ID()
}
func (hw *HostWires) Neighbors() []peer.ID {
	return hw.Host.Network().Peers()
}
func (hw *HostWires) SetListener(wn WireListener) {
	hw.up = wn.HandlePeerUp
	hw.down = wn.HandlePeerDown
}
func (hw *HostWires) SendMsg(to peer.ID, data []byte) error {
	s, err := hw.Host.NewStream(context.Background(), to, protocol.ID(HostWiresProcotolID))
	defer s.Close()
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(s)
	mw := msgio.NewWriter(bw)
	err = mw.WriteMsg(data)
	if err != nil {
		return err
	}
	err = bw.Flush()
	if err != nil {
		return err
	}
	return nil
}
func (hw *HostWires) SetMsgHandler(h MsgHandler) {
	hw.msghdl = h
}

func (hw *HostWires) handleStream(s network.Stream) {
	mr := msgio.NewReader(s)
	data, err := mr.ReadMsg()
	if err != nil {
		// TODO add log
		return
	}
	hw.msghdl(s.Conn().RemotePeer(), data)
}

func (hw *HostWires) Listen(network.Network, ma.Multiaddr)           {}
func (hw *HostWires) ListenClose(network.Network, ma.Multiaddr)      {}
func (hw *HostWires) Connected(n network.Network, c network.Conn)    { hw.up(c.RemotePeer()) }
func (hw *HostWires) Disconnected(n network.Network, c network.Conn) { hw.down(c.RemotePeer()) }
func (hw *HostWires) OpenedStream(network.Network, network.Stream)   {}
func (hw *HostWires) ClosedStream(network.Network, network.Stream)   {}
