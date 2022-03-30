package collect

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/protocol"
)

type conf struct {
	requestProtocol  protocol.ID
	responseProtocol protocol.ID
	requestCacheSize int
}

func checkOptConfAndGetInnerConf(optConf *Conf) (inner *conf, err error) {
	if optConf.ProtocolPrefix == "" {
		err = fmt.Errorf("unexpected nil Prefix")
	}
	if optConf.RequestCacheSize < 0 {
		err = fmt.Errorf("unexpected negetive RequestCacheSize")
	}
	if err == nil {
		inner = &conf{
			requestProtocol:  protocol.ID(optConf.ProtocolPrefix + "/request"),
			responseProtocol: protocol.ID(optConf.ProtocolPrefix + "/response"),
			requestCacheSize: optConf.RequestCacheSize,
		}
	}
	return
}

// Conf is the static configuration of PubSubCollector
type Conf struct {
	// Router is an option to select different router type
	// Router must be one of `basic`, `relay` and `intbfs`
	Router string `json:"router"`
	// ProtocolPrefix is the protocol name prefix
	ProtocolPrefix string `json:"protocol"`
	// RequestCacheSize .
	// RequestCache is used to store the request control message,
	// which is for response routing.
	RequestCacheSize int `json:"requestCacheSize"`
	// ResponseCacheSize .
	// ResponseCache is used to deduplicate the response.
	ResponseCacheSize int `json:"responseCacheSize"`
}

// MakeDefaultConf returns a default Conf instance
func MakeDefaultConf() Conf {
	return Conf{
		Router:            "relay",
		ProtocolPrefix:    "/psc",
		RequestCacheSize:  512,
		ResponseCacheSize: 1024,
	}
}
