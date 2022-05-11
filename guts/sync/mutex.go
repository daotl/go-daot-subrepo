// Copyright (c) Tendermint. All rights reserved.
// Use of this source code is governed by Apache License 2.0 that can be
// found in the LICENSE_APACHE_2.0 file.

//go:build !deadlock
// +build !deadlock

package sync

// From: https://github.com/tendermint/tendermint/blob/9cee35bb8caed44c9f9af0a50f3d9a32454ebe76/libs/sync/sync.go

import "sync"

// A Mutex is a mutual exclusion lock.
//
// Building with `deadlock` flag will use Mutex in mutex_deadlock.go instead of
// this to detect deadlocks.
type Mutex struct {
	sync.Mutex
}

// An RWMutex is a reader/writer mutual exclusion lock.
//
// Building with `deadlock` flag will use RWMutex in mutex_deadlock.go instead of
// this to detect deadlocks.
type RWMutex struct {
	sync.RWMutex
}
