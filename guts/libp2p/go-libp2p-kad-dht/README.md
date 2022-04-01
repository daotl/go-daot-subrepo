# go-libp2p-kad-dht

Fork of [libp2p/go-libp2p-kad-dht](https://github.com/libp2p/go-libp2p-kad-dht) by DAOT Labs.

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![](https://img.shields.io/badge/project-DAOT-red.svg?style=flat-square)](https://daot.io)
[![Go Reference](https://pkg.go.dev/badge/github.com/daotl/go-libp2p-kad-dht.svg)](https://pkg.go.dev/github.com/daotl/go-libp2p-kad-dht)
[![Build Status](https://travis-ci.org/daotl/go-libp2p-kad-dht.svg?branch=master)](https://travis-ci.org/daotl/go-libp2p-kad-dht)

> A Kademlia DHT implementation on go-libp2p

## Table of Contents

- [go-libp2p-kad-dht](#go-libp2p-kad-dht)
  - [Table of Contents](#table-of-contents)
  - [Install](#install)
  - [Usage](#usage)
  - [Contribute](#contribute)
  - [License](#license)

## Install

```sh
go get github.com/daotl/go-libp2p-kad-dht
```

Add this to `go.mod`, so other libp2p modules will also use this fork:
```
replace github.com/libp2p/go-libp2p-kad-dht => github.com/daotl/go-libp2p-kad-dht {{KAD_DHT_VERSION}}
```

## Usage

Go to https://pkg.go.dev/github.com/daotl/go-libp2p-kad-dht.

## Contribute

Contributions welcome. Please check out [the issues](https://github.com/daotl/go-libp2p-kad-dht/issues).

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

[MIT](LICENSE)

Copyright for the modified portions of this fork are held by [DAOT Labs, 2020].
All other portions of this fork are held by [Protocol Labs Inc., 2016] as part of the original [go-libp2p-kbucket](https://github.com/libp2p/go-libp2p-kbucket) project.
All right reserved.
