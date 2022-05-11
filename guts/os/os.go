// Copyright (c) Tendermint. All rights reserved.
// Use of this source code is governed by Apache License 2.0 that can be
// found in the LICENSE_APACHE_2.0 file.

package os

// Ported from: https://github.com/tendermint/tendermint/blob/f28d629e280ddcdc0dd644ccf1586d73dddfb7a1/libs/os/os.go

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

// e.g., `log.StandardLogger` from `github.com/daotl/go-log/v2`
type logger interface {
	Info(args ...any)
}

// TrapSignal catches SIGTERM and SIGINT, executes the cleanup function,
// and exits with code 0.
func TrapSignal(logger logger, cb func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-c
		logger.Info(fmt.Sprintf("captured %v, exiting...", sig))
		if cb != nil {
			cb()
		}
		os.Exit(0)
	}()
}

// Exit prints string `s` then `os.Exit(1)`.
func Exit(s string) {
	fmt.Printf(s + "\n")
	os.Exit(1)
}

// EnsureDir ensures the given directory exists, creating it if necessary.
// Errors if the path already exists as a non-directory.
func EnsureDir(dir string, mode os.FileMode) error {
	err := os.MkdirAll(dir, mode)
	if err != nil {
		return fmt.Errorf("could not create directory %q: %w", dir, err)
	}
	return nil
}

func FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

// CopyFile copies a file. It truncates the destination file if it exists.
func CopyFile(src, dst string) error {
	srcfile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcfile.Close()

	info, err := srcfile.Stat()
	if err != nil {
		return err
	}
	if info.IsDir() {
		return errors.New("cannot read from directories")
	}

	// create new file, truncate if exists and apply same permissions as the original one
	dstfile, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	defer dstfile.Close()

	_, err = io.Copy(dstfile, srcfile)
	return err
}
