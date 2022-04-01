/*
Package server is used to start a new ABCI server.

It contains two server implementation:
 * gRPC server
 * socket server

*/
package server

import (
	"fmt"

	"github.com/daotl/go-log/v2"
	ssrv "github.com/daotl/guts/service/suture"

	"github.com/daotl/go-acei/types"
)

func NewServer(logger log.StandardLogger, protoAddr, transport string, app types.Application,
) (ssrv.Service, error) {
	var s ssrv.Service
	var err error
	switch transport {
	case "socket":
		s, err = NewSocketServer(logger, protoAddr, app)
	case "grpc":
		s, err = NewGRPCServer(logger, protoAddr, types.NewGRPCApplication(app))
	default:
		err = fmt.Errorf("unknown server type %s", transport)
	}
	return s, err
}
