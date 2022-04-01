# go-ds-badger

DAOT Labs' fork of [textileio/go-ds-badger3](https://github.com/textileio/go-ds-badger3).

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-DAOT%20Labs-red.svg?style=flat-square)](http://github.com/daotl)
[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![go.dev reference](https://godoc.org/github.com/daotl/go-ds-badger3?status.svg)](https://pkg.go.dev/github.com/daotl/go-ds-badger3)
[![GitHub action](https://github.com/daotl/go-ds-badger3/workflows/Tests/badge.svg?style=popout-square)](https://github.com/daotl/go-ds-badger3/actions)

> Datastore implementation using [badger](https://github.com/dgraph-io/badger) as backend.

This fork adds support for bytes-backed keys in addition to original string-backed
keys, which could improve performance in some cases by preventing type conversion
and reducing key size.

## Lead Maintainer

[Nex](https://github.com/NexZhu)

## Table of Contents

- [Documentation](#documentation)
- [Contribute](#contribute)
- [License](#license)

## Documentation

See [godoc](https://pkg.go.dev/github.com/daotl/go-ds-badger3).

## Contribute

Feel free to join in. All welcome. Open an [issue](https://github.com/daotl/go-ds-badger3/issues)!

## License

MIT
