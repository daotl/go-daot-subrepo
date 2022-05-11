package suturesrv

import (
	"context"
	"errors"

	"github.com/thejerf/suture/v4"
	"go.uber.org/zap"

	"github.com/daotl/go-log/v2"
	gsync "github.com/daotl/guts/sync"
)

var (
	ErrStoppedBeforeReady = errors.New("stopped before ready")
)

var closedchan = make(chan error)

func init() {
	close(closedchan)
}

var (
	ErrRunFnMustBeSpecified = errors.New("runFn must be specified")
	ErrAlreadyRunning       = errors.New("already running")
	ErrNotRunning           = errors.New("not running")
)

// Status of a Service
type Status uint8

// ReadyChan notifies that the Service is ready or failed to start and the error occurred.
// If the Service stops before `ready` is called, ReadyChan will send ErrStoppedBeforeReady.
type ReadyChan chan error

const (
	// Not started or stopped
	StatusStopped Status = iota
	// Started but not yet ready to serve
	StatusStarted
	// Started and ready to serve
	StatusReady
	// Failed to start
	StatusFailed
	// Stopping
	StatusStopping
)

/*
Service is a Suture-based service interface.

Service's lifecycle graph:
                                Serve()
                StatusStopped ─────────► StatusStarted
                 │                                │ │
runFn() returned │ ┌──────────────────────────────┘ │ ready() called in runFn()
                 │ ▼  Stop() or context canceled    ▼
                StatusStopping ◄───────- StatusReady/StatusFailed
*/
type Service interface {
	suture.Service

	// Serve is called by a suture.Supervisor to start the Service.
	// Returns ErrAlreadyRunning if it's already running.
	// See: https://pkg.go.dev/github.com/thejerf/suture/v4#hdr-Serve_Method
	//Serve(ctx context.Context) error

	// Status returns the status of the Service.
	Status() Status

	// Ready returns a ReadyChan to notify that the Service is ready or failed to start and the error occurred.
	// If the Service stops before `ready` is called, ReadyChan will send ErrStoppedBeforeReady.
	Ready() ReadyChan

	// Start can conveniently start the Service in a new goroutine without using a suture.Supervisor.
	// Return a ReadyChan the same as Ready, and a channel that sends the result of `Service.Serve`.
	//
	// Useful for writing tests for Service for example:
	// ```go
	// readyCh, resultCh := s.Start(context.Background())
	// require.NoError(t, <-readyCh)
	// defer require.NoError(t, <-resultCh)
	// ```
	Start(ctx context.Context) (ReadyChan, chan error)

	// Stop the Service manually if it's running by cancelling it's context , you can wait on the
	// returned channel for stopping to finish. Returns ErrNotRunning if it's not running.
	Stop() (chan struct{}, error)
}

// RunFunc is called when Service.Serve is called either by a suture.Supervisor or manually.
//
// A RunFunc should call `ready` with nil when the Service is ready or failed to start with the error occurred.
// If the Service stops before `ready` is called, Service.Ready will return a channel that sends ErrStoppedBeforeReady.
//
// The Service should execute within the goroutine that this is called in, that is, it should not spawn a
// "worker" goroutine. If this function either returns error or panics, the Supervisor will call it again.
// `ready` should be called when the Service is ready to serve.
type RunFunc = func(ctx context.Context, ready func(err error)) error

// BaseService is the base implementation of a Service which can be embedded in actual services.
type BaseService struct {
	Logger log.StandardLogger

	cancel  context.CancelFunc
	mtx     gsync.Mutex
	status  Status
	result  *gsync.ResultNotifier
	stopped chan struct{}

	// runFn will run when the Service is started.
	runFn RunFunc
}

var _ = Service(&BaseService{})

// NewBaseService creates a new BaseService.
func NewBaseService(runFn RunFunc, logger log.StandardLogger,
) (*BaseService, error) {
	if runFn == nil {
		return nil, ErrRunFnMustBeSpecified
	}
	if logger == nil {
		logger = log.NopLogger()
	}

	return &BaseService{
		Logger: logger,
		runFn:  runFn,
		result: gsync.NewResultNotifier(nil),
	}, nil
}

func (s *BaseService) Status() Status {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.status
}

func (s *BaseService) Serve(ctx context.Context) error {
	s.mtx.Lock()
	{
		if s.status != StatusStopped {
			s.mtx.Unlock()
			return ErrAlreadyRunning
		}
		s.status = StatusStarted
		s.stopped = make(chan struct{})
		ctx, s.cancel = context.WithCancel(ctx)
	}
	s.mtx.Unlock()

	go func() {
		<-ctx.Done()
		s.mtx.Lock()
		defer s.mtx.Unlock()
		if s.status == StatusStarted || s.status == StatusReady {
			s.status = StatusStopping
		}
	}()

	s.Logger.Debug("Service started running")
	defer s.Logger.Debug("Service stopped")
	err := s.runFn(ctx, func(err error) {
		s.mtx.Lock()
		defer s.mtx.Unlock()
		if s.status == StatusStarted {
			s.result.Finish(err)
			if err == nil {
				s.status = StatusReady
			} else {
				s.status = StatusFailed
				// Reset s.result so new calls to s.Ready() can wait for the service to restart
				// instead of still getting this `err`
				s.result = gsync.NewResultNotifier(nil)
			}
		}
	})

	s.mtx.Lock()
	{
		// `ready` wasn't called in `RunFunc`, unblock the waiters now
		if !(s.status == StatusReady || s.status == StatusFailed) {
			s.result.Finish(ErrStoppedBeforeReady)
		}
		// Skip because if failed, s.result would be already reset in `ready`
		if s.status != StatusFailed {
			s.result = gsync.NewResultNotifier(nil)
		}
		s.status = StatusStopped
		close(s.stopped)
	}
	s.mtx.Unlock()

	return err
}

func (s *BaseService) Ready() ReadyChan {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.result.Done()
}

func (s *BaseService) Start(ctx context.Context) (ReadyChan, chan error) {
	ch := make(chan error, 1)
	go func() {
		ch <- s.Serve(ctx)
	}()
	return s.Ready(), ch
}

func (s *BaseService) Stop() (chan struct{}, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.status == StatusStopped {
		return nil, ErrNotRunning
	}
	if s.status != StatusStopping {
		s.status = StatusStopping
		s.cancel()
	}

	return s.stopped, nil
}

// BaseSupervisor is the base implementation of a Suture Supervisor which can be embedded in actual supervisors.
type BaseSupervisor struct {
	*suture.Supervisor

	Logger log.StandardLogger

	cancel context.CancelFunc
}

// Serve starts the supervisor. You should call this on the top-level supervisor, but nothing else.
func (s *BaseSupervisor) Serve(ctx context.Context) error {
	s.Logger.Debug("Supervisor started running")
	defer s.Logger.Debug("Supervisor stopped")
	ctx, s.cancel = context.WithCancel(ctx)
	return s.Supervisor.Serve(ctx)
}

// ServeBackground starts running a supervisor in its own goroutine.
// When this method returns, the supervisor is guaranteed to be in a running state.
// The returned one-buffered channel receives the error returned by Serve.
func (s *BaseSupervisor) ServeBackground(ctx context.Context) <-chan error {
	s.Logger.Debug("Supervisor started running in the background")
	ctx, s.cancel = context.WithCancel(ctx)
	return s.Supervisor.ServeBackground(ctx)
}

// NewBaseSupervisor creates a new BaseSupervisor.
func NewBaseSupervisor(logger log.StandardLogger,
) (*BaseSupervisor, error) {
	if logger == nil {
		logger = zap.NewNop().Sugar()
	}

	return &BaseSupervisor{
		Logger: logger,
	}, nil
}

// AllReady return an error channel which can be waited on for all passed services to be ready and
// get the error if any of the service fails to be ready or the context is done.
func AllReady(ctx context.Context, srvs ...Service) chan error {
	ch := make(chan error, 1)
	go func() {
		for _, s := range srvs {
			select {
			case <-ctx.Done():
				ch <- ctx.Err()
				return
			case err := <-s.Ready():
				if err != nil {
					ch <- err
					return
				}
			}
		}
		close(ch)
	}()
	return ch
}

// WaitForAllReady will block and repeatedly try to wait for all passed services to be ready,
// it will only return `nil` when all services are ready or return an error when `ctx is done.
func WaitForAllReady(ctx context.Context, srvs ...Service) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-AllReady(ctx, srvs...):
			if err == nil {
				return nil
			}
		}
	}
}
