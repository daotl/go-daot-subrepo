package goprocesssrv

import (
	"errors"

	"github.com/jbenet/goprocess"

	"github.com/daotl/go-log/v2"
)

var (
	ErrRunFnMustBeSpecified = errors.New("runFn must be specified")
	ErrNotStarted           = errors.New("service not started")
)

// Service is a goprocess-based service interface
type Service interface {
	// Start the service, optionally with a parent service.
	Start(parent Service)

	// Stop the service， return the error from the teardown function.
	Stop() error

	// StartChild starts a child service which will stop before this service stops when this service's Stop() is called.
	StartChild(child Service) error

	// EnsureChild starts a child service which will stop before this service stops when this service's Stop() is called.
	// If any child service started with EnsureChild stops, this service will stop all children then stop itself as well.
	EnsureChild(child Service) error

	// WaitFor makes the service wait for `other` before stopping.
	WaitFor(other Service)

	// Process returns the underlying goprocess, use with caution.
	Process() goprocess.Process
}

var _ = Service(&BaseService{})

// BaseService is the base implementation of Service which can be embedded in actual services.
type BaseService struct {
	Logger log.StandardLogger

	// Err is an error or multiple errors combined using `go.uber.org/multierr` that occurred during the service lifecycle.
	Err error

	proc goprocess.Process

	// runFn will run when Service.Start() is called.
	runFn goprocess.ProcessFunc

	// stopFn will only run after Service.Stop() has been called and runFn has returned.
	stopFn goprocess.TeardownFunc

	// Waits contains the services which this service will wait for to stop before itself stops.
	waits []Service
}

// NewBaseService creates a new BaseService.
func NewBaseService(runFn goprocess.ProcessFunc, stopFn goprocess.TeardownFunc,
	waits []Service, logger log.StandardLogger) (*BaseService, error) {
	if runFn == nil {
		return nil, ErrRunFnMustBeSpecified
	}
	if logger == nil {
		logger = log.NopLogger()
	}

	return &BaseService{
		Logger: logger,
		runFn:  runFn,
		stopFn: stopFn,
		waits:  waits,
	}, nil
}

func (s *BaseService) Start(parent Service) {
	if parent != nil {
		s.setup(parent.Process().Go(s.run))
	} else {
		s.setup(goprocess.Go(s.run))
	}
}

func (s *BaseService) Stop() error {
	if s.proc == nil {
		return ErrNotStarted
	}
	return s.proc.Close()
}

func (s *BaseService) StartChild(child Service) error {
	if s.Process == nil {
		return ErrNotStarted
	}
	child.Start(s)
	return nil
}

func (s *BaseService) EnsureChild(child Service) error {
	if err := s.StartChild(child); err != nil {
		return err
	}

	// Close if child is closing
	go func() {
		<-child.Process().Closing()
		s.proc.Close()
	}()

	return nil
}

func (s *BaseService) WaitFor(other Service) {
	if s.proc == nil {
		// Service not started yet, just record waited services
		s.waits = append(s.waits, other)
	} else if p := other.Process(); p != nil {
		// Both service started, call WaitFor
		s.proc.WaitFor(p)
	}
}

func (s *BaseService) Process() goprocess.Process {
	return s.proc
}

// setup goprocess, teardown function and waited services for the service.
func (s *BaseService) setup(proc goprocess.Process) {
	if proc == nil {
		panic("cannot setup nil proc")
	}
	s.proc = proc

	if s.stopFn != nil {
		proc.SetTeardown(s.teardown)
	}

	for _, w := range s.waits {
		if p := w.Process(); p != nil {
			proc.WaitFor(p)
		}
	}
}

// run is a wrapper around the actual service startup function runFn.
func (s *BaseService) run(proc goprocess.Process) {
	s.Logger.Debug("Service started running")
	defer s.Logger.Debug("Service finished running")
	if s.runFn != nil {
		s.runFn(proc)
	}
}

// teardown is a wrapper around the actual service teardown function stopFn.
func (s *BaseService) teardown() error {
	s.Logger.Debug("Stopping service")
	defer s.Logger.Debug("Service stopped")
	if s.stopFn != nil {
		return s.stopFn()
	}
	return nil
}
