package log

import (
	"sync"
	"testing"

	"go.uber.org/zap"
)

var (
	// reuse the same logger across all tests
	testingLoggerMtx = sync.Mutex{}
	testingLogger    *zap.SugaredLogger
)

// Type check
var _ StandardLogger = (*zap.SugaredLogger)(nil)

// NopLogger returns a no-op Logger. It never writes out logs or internal errors.
func NopLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

// TestingLogger returns a Logger which writes to STDOUT if test(s) are being
// run with the verbose (-v) flag, NopLogger otherwise.
//
// NOTE:
// - A call to NewTestingLogger() must be made inside a test (not in the init func)
// because verbose flag only set at the time of testing.
func TestingLogger() *zap.SugaredLogger {
	testingLoggerMtx.Lock()
	defer testingLoggerMtx.Unlock()

	if testingLogger != nil {
		return testingLogger
	}

	if testing.Verbose() {
		if logger, err := zap.NewDevelopmentConfig().Build(); err != nil {
			panic(err)
		} else {
			return logger.Sugar()
		}
	} else {
		testingLogger = NopLogger()
	}

	return testingLogger
}
