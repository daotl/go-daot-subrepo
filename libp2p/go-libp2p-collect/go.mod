module github.com/daotl/go-libp2p-collect

go 1.16

replace github.com/libp2p/go-libp2p-pubsub => github.com/daotl/go-libp2p-pubsub v0.6.0-daot.0

require (
	github.com/gogo/protobuf v1.3.2
	github.com/hashicorp/golang-lru v0.5.4
	github.com/libp2p/go-libp2p v0.16.0
	github.com/libp2p/go-libp2p-core v0.11.0
	github.com/libp2p/go-libp2p-pubsub v0.0.0-00010101000000-000000000000
	github.com/libp2p/go-msgio v0.1.0
	github.com/multiformats/go-multiaddr v0.4.1
	github.com/stretchr/testify v1.7.0
)
