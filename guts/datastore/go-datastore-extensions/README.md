# go-datastore-extensions

DAOT Labs' fork of [textileio/go-datastore-extensions](https://github.com/textileio/go-datastore-extensions).

[![Made by Textile](https://img.shields.io/badge/made%20by-Textile-informational.svg?style=popout-square)](https://textile.io)
[![GitHub license](https://img.shields.io/github/license/daotl/go-datastore-extensions.svg?style=popout-square)](./LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/daotl/go-datastore-extensions?style=flat-square)](https://goreportcard.com/report/github.com/daotl/go-datastore-extensions?style=flat-square)

> go-datastore extension interfaces

This fork adds support for bytes-backed keys in addition to original string-backed
keys, which could improve performance in some cases by preventing type conversion
and reducing key size.

## Table of Contents

-   [Overview](#overview)
-   [License](#license)

## Overview

This repository contains some extension interfaces stacked on top of `daotl/go-datastore`.

## License

[MIT](LICENSE)
