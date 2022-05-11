package sync

import (
	"sync"
)

var closedchan = make(chan error)

func init() {
	close(closedchan)
}

// ResultNotifier can be used for a goroutine to notify others that a job is done
// and pass the error occurred or nil if no error.
//
// It's based on `sync.Cond`.
type ResultNotifier struct {
	c    *sync.Cond
	done bool
	err  error
}

// NewResultNotifier creates a ResultNotifier.
func NewResultNotifier(l sync.Locker) *ResultNotifier {
	if l == nil {
		l = &Mutex{}
	}
	return &ResultNotifier{
		c:    sync.NewCond(l),
		done: false,
	}
}

// Done returns a channel that can be waited on for the job to finish and to
// receive the error occurred or nil if no error.
func (r *ResultNotifier) Done() chan error {
	// Fast path
	r.c.L.Lock()
	if r.done && r.err == nil {
		r.c.L.Unlock()
		return closedchan
	}
	r.c.L.Unlock()

	// Use buffered channel to prevent the goroutine below from leaking.
	ch := make(chan error, 1)
	go func() {
		r.c.L.Lock()
		defer r.c.L.Unlock()
		for !r.done {
			r.c.Wait()
		}
		ch <- r.err
	}()
	return ch
}

// Finish notifies that the job is done with the error occurred or nil if no error.
func (r *ResultNotifier) Finish(err error) {
	if r.done {
		return
	}

	r.c.L.Lock()
	defer r.c.L.Unlock()
	r.done = true
	r.err = err
	r.c.Broadcast()
}
