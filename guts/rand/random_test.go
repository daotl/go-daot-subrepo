// Copyright (c) Tendermint. All rights reserved.
// Use of this source code is governed by Apache License 2.0 that can be
// found in the LICENSE_APACHE_2.0 file.

package rand

// From: https://github.com/tendermint/tendermint/blob/9cee35bb8caed44c9f9af0a50f3d9a32454ebe76/libs/rand/random_test.go

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandStr(t *testing.T) {
	l := 243
	s := Str(l)
	assert.Equal(t, l, len(s))
}

func TestRandBytes(t *testing.T) {
	l := 243
	b := Bytes(l)
	assert.Equal(t, l, len(b))
}

func BenchmarkRandBytes10B(b *testing.B) {
	benchmarkRandBytes(b, 10)
}
func BenchmarkRandBytes100B(b *testing.B) {
	benchmarkRandBytes(b, 100)
}
func BenchmarkRandBytes1KiB(b *testing.B) {
	benchmarkRandBytes(b, 1024)
}
func BenchmarkRandBytes10KiB(b *testing.B) {
	benchmarkRandBytes(b, 10*1024)
}
func BenchmarkRandBytes100KiB(b *testing.B) {
	benchmarkRandBytes(b, 100*1024)
}
func BenchmarkRandBytes1MiB(b *testing.B) {
	benchmarkRandBytes(b, 1024*1024)
}

func benchmarkRandBytes(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		_ = Bytes(n)
	}
	b.ReportAllocs()
}
