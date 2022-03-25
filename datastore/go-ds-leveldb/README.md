# go-ds-leveldb

DAOT Labs' fork of [ipfs/go-ds-leveldb](https://github.com/ipfs/go-ds-leveldb).

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-DAOT%20Labs-red.svg?style=flat-square)](http://github.com/daotl)
[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/daotl/go-ds-leveldb)
[![Build Status](https://travis-ci.org/daotl/go-ds-leveldb.svg?branch=master)](https://travis-ci.org/daotl/go-ds-leveldb)

> A go-datastore implementation using LevelDB

`go-ds-leveldb` implements the [go-datastore](https://github.com/daotl/go-datastore) interface using a LevelDB backend.

This fork adds support for bytes-backed keys in addition to original string-backed
keys, which could improve performance in some cases by preventing type conversion
and reducing key size.

## Lead Maintainer

[Nex](https://github.com/NexZhu)

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Install

This module can be installed like a regular go module:

```
go get github.com/daotl/go-ds-leveldb
```

## Usage

```
import "github.com/daotl/go-ds-leveldb"
```

Check the [API documentation](https://pkg.go.dev/github.com/daotl/go-ds-leveldb)

## Contribute

PRs accepted.

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

[MIT](LICENSE)

Copyright for portions of this fork are held by [Protocol Labs, 2016] as part of the original [go-ds-leveldb](https://github.com/ipfs/go-ds-leveldb) project.

All other copyright for this fork are held by [DAOT Labs, 2020].

All rights reserved.
