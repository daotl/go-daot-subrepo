package server

import (
	"context"
	"net"

	"github.com/daotl/go-log/v2"
	gnet "github.com/daotl/guts/net"
	ssrv "github.com/daotl/guts/service/suture"
	"google.golang.org/grpc"

	"github.com/daotl/go-acei/types"
)

type GRPCServer struct {
	*ssrv.BaseService

	proto    string
	addr     string
	listener net.Listener
	server   *grpc.Server

	app types.ACEIApplicationServer
}

// NewGRPCServer returns a new gRPC ABCI server
func NewGRPCServer(logger log.StandardLogger, protoAddr string, app types.ACEIApplicationServer,
) (ssrv.Service, error) {
	proto, addr := gnet.ProtocolAndAddress(protoAddr)
	s := &GRPCServer{
		proto: proto,
		addr:  addr,
		app:   app,
	}
	var err error
	s.BaseService, err = ssrv.NewBaseService(s.run, logger)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// OnStart starts the gRPC service.
//func (s *GRPCServer) OnStart(ctx context.Context) error {
func (s *GRPCServer) run(ctx context.Context, ready func(error)) error {

	s.listener = nil
	ln, err := net.Listen(s.proto, s.addr)
	if err != nil {
		ready(err)
		return err
	}

	s.listener = ln
	s.server = grpc.NewServer()
	types.RegisterACEIApplicationServer(s.server, s.app)

	s.Logger.Info("Listening", "proto", s.proto, "addr", s.addr)
	go func() {
		go func() {
			<-ctx.Done()
			s.server.GracefulStop()
		}()

		if err := s.server.Serve(s.listener); err != nil {
			s.Logger.Error("Error serving gRPC server", "err", err)
		}
	}()

	ready(nil)
	// Block until stopped
	<-ctx.Done()

	// OnStop stops the gRPC server.
	//func (s *GRPCServer) OnStop() {
	s.server.Stop()
	return nil
}
