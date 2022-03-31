package crawler

import (
	"time"

	"github.com/libp2p/go-libp2p-core/protocol"
)

// Option DHT Crawler option type.
type Option func(*options) error

type options struct {
	protocols      []protocol.ID
	parallelism    int
	connectTimeout time.Duration
	perMsgTimeout  time.Duration

	/* #DAOT */
	// kbucket.RoutingTable.considerLatency
	considerLatency bool
	// kbucket.RoutingTable.avgBitsImprovedPerStep
	avgBitsImprovedPerStep float64
	// kbucket.RoutingTable.avgRoundTripsPerStepWithNewPeer
	avgRoundTripsPerStepWithNewPeer float64
}

// defaults are the default crawler options. This option will be automatically
// prepended to any options you pass to the crawler constructor.
var defaults = func(o *options) error {
	o.protocols = []protocol.ID{"/ipfs/kad/1.0.0"}
	o.parallelism = 1000
	o.connectTimeout = time.Second * 5
	o.perMsgTimeout = time.Second * 5

	o.considerLatency = false
	// Use the default values from `daotl/go-libp2p-kbucket`.
	o.avgBitsImprovedPerStep = 0
	o.avgRoundTripsPerStepWithNewPeer = 0

	return nil
}

// WithProtocols defines the ordered set of protocols the crawler will use to talk to other nodes
func WithProtocols(protocols []protocol.ID) Option {
	return func(o *options) error {
		o.protocols = append([]protocol.ID{}, protocols...)
		return nil
	}
}

// WithParallelism defines the number of queries that can be issued in parallel
func WithParallelism(parallelism int) Option {
	return func(o *options) error {
		o.parallelism = parallelism
		return nil
	}
}

// WithMsgTimeout defines the amount of time a single DHT message is allowed to take before it's deemed failed
func WithMsgTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		o.perMsgTimeout = timeout
		return nil
	}
}

// WithConnectTimeout defines the time for peer connection before timing out
func WithConnectTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		o.connectTimeout = timeout
		return nil
	}
}

// If enabled, DHT will find the nearest peers to query by taking into account not only the xor distance to the target peer,
// but also the latency to the local peer (measured in RTT).
// This strategy can be tuned with AvgBitsImprovedPerStep and AvgRoundTripPerStepWithNewPeer.
//
// Defaults to disabled.
func EnableConsiderLatency() Option {
	return func(o *options) error {
		o.considerLatency = true
		return nil
	}
}

// AvgBitsImprovedPerStep configures the estimated average number of bits improved per lookup step.
// If not set will use the default value calculated using the bucket size.
func AvgBitsImprovedPerStep(avgBitsImprovedPerStep float64) Option {
	return func(o *options) error {
		o.avgBitsImprovedPerStep = avgBitsImprovedPerStep
		return nil
	}
}

// AvgRoundTripPerStepWithNewPeer configures the average RTT count needed per lookup step to connect to a new peer and execute the lookup query,
// varies among transport protocols, reference values:
// For TCP+TLS1.3 : 4
// For QUIC : 2
//
// If not set will default to 4 (TCP+TLS1.3 settings) which will value the xor distance more in sorting.
func AvgRoundTripPerStepWithNewPeer(avgRoundTripsPerStepWithNewPeer float64) Option {
	return func(o *options) error {
		o.avgRoundTripsPerStepWithNewPeer = avgRoundTripsPerStepWithNewPeer
		return nil
	}
}
