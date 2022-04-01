# go-libp2p-collect

<p align="left">
  <a href="http://daot.io"><img src="https://img.shields.io/badge/made%20by-DAOT%20Labs-red.svg?style=flat-square" /></a>
  <a href="http://libp2p.io/"><img src="https://img.shields.io/badge/project-libp2p-yellow.svg?style=flat-square" /></a> 
</p>

<p align="left">
  <a href="https://codecov.io/gh/daotl/go-libp2p-collect"><img src="https://codecov.io/gh/daotl/go-libp2p-collect/branch/master/graph/badge.svg"></a>
  <a href="https://goreportcard.com/report/github.com/daotl/go-libp2p-collect"><img src="https://goreportcard.com/badge/github.com/daotl/go-libp2p-collect" /></a>
  <a href="https://github.com/RichardLitt/standard-readme"><img src="https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square" /></a>
  <a href="https://godoc.org/github.com/daotl/go-libp2p-collect"><img src="http://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square" /></a>
  <a href=""><img src="https://img.shields.io/badge/golang-%3E%3D1.14.0-orange.svg?style=flat-square" /></a>
  <br>
</p>

A pub-sub-collect system built on libp2p.

## Repo Lead Maintainer

[@huiscool](https://github.com/huiscool/)

## Table of Contents

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Install](#install)
- [Usage](#usage)
- [Documentation](#documentation)
- [Contribute](#contribute)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Install

```sh
go get github.com/daotl/go-libp2p-collect
```

And add the following line to your `go.mod`:
```
replace github.com/libp2p/go-libp2p-pubsub => github.com/daotl/go-libp2p-pubsub {{VERSION}}
```

## Usage

To be used for messaging and data collection in p2p infrastructure (as part of libp2p) such as IPFS, Ethereum, other blockchains, etc.

## Documentation

See the [API documentation](https://pkg.go.dev/github.com/daotl/go-libp2p-collect).

## Contribute

Contributions welcome. Please check out [the issues](https://github.com/daotl/go-libp2p-collect/issues).

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

[MIT](./LICENSE)

Copyright (c) 2020 DAOT Labs. All rights reserved.
