# go-datastore

DAOT Labs' fork of [ipfs/go-datastore](https://github.com/ipfs/go-datastore).

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-DAOT%20Labs-red.svg?style=flat-square)](http://github.com/daotl)
[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/daotl/go-datastore)
[![Build Status](https://travis-ci.com/daotl/go-datastore.svg?branch=master)](https://travis-ci.com/daotl/go-datastore)

> key-value datastore interfaces

## Lead Maintainer

[Nex](https://github.com/NexZhu)

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Documentation](#documentation)
- [Contribute](#contribute)
- [License](#license)

## Background

Datastore is a generic layer of abstraction for data store and database access.
It is a simple API with the aim to enable application development in a datastore-agnostic way,
allowing datastores to be swapped seamlessly without changing application code.
Thus, one can leverage different datastores with different strengths without
committing the application to one datastore throughout its lifetime.

In addition, grouped datastores significantly simplify interesting data access
patterns (such as caching and sharding).

This fork adds support for bytes-backed keys in addition to original string-backed
keys, which could improve performance in some cases by preventing type conversion
and reducing key size.

Based on [datastore.py](https://github.com/datastore/datastore).

## Install

```sh
go get github.com/daotl/go-datastore
```

## Documentation

See [API documentation](https://pkg.go.dev/github.com/daotl/go-datastore).

## Contribute

Feel free to join in. All welcome. Open an [issue](https://github.com/daotl/go-datastore/issues)!

## License

[MIT](LICENSE)

Copyright for portions of this fork are held by [Protocol Labs, 2016] as part of the original [go-datastore](https://github.com/ipfs/go-datastore) project.

All other copyright for this fork are held by [DAOT Labs, 2020].

All rights reserved.
