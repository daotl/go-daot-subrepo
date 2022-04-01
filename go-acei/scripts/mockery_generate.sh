#!/bin/sh

# From: https://github.com/tendermint/tendermint/blob/4e355d80c4b44031150e1934e6f4b85ada7b8139/scripts/mockery_generate.sh

go run github.com/vektra/mockery/v2 --disable-version-string --case underscore --name $*