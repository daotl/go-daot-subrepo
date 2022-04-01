package lazy

import (
	"context"
	"errors"
	"sync"

	"github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
)

var ErrClosed = errors.New("Datastore is closed")

// LazyDatastore wraps a datastore with three states: active, inactive and closed.
// State can be changed by three StateChangeFuncs: activateFn, deactivateFn and closeFn.
// The relationship between states and StateChangeFuncs is as follows:
//
//    ----------(deactivateFn)----------
//    |                                |  -(ensureActive)-
//    V                                | |               |
// inactive -----(activateFn)------> active <------------
//    |                                |
//    --(closeFn)-> closed <-(closeFn)--
//
// Every datastore operation will check that the datastore is in the `active` state or try to
// activate it with the `activateFn` StateChangeFunc, an error will occur if it fails.
type LazyDatastore struct {
	wrapped      datastore.Datastore
	rw           sync.RWMutex
	active       bool
	writing      bool // Locked active and performing operations
	closed       bool
	activateFn   StateChangeFunc
	deactivateFn StateChangeFunc
	closeFn      StateChangeFunc
}

var _ datastore.Datastore = (*LazyDatastore)(nil)

// StateChangeFunc is a function that change a LazyStore's state
type StateChangeFunc func(ds datastore.Datastore) error

// DataStoreOp is a Datastore operation
type DataStoreOp func() error

// NewLazyDataStore creates a LazyStore.
func NewLazyDataStore(wrapped datastore.Datastore, activateFn, deactivateFn, closeFn StateChangeFunc,
) (*LazyDatastore, error) {
	if wrapped == nil {
		return nil, errors.New("wraooed cannot be nil")
	}
	if activateFn == nil || deactivateFn == nil || closeFn == nil {
		return nil, errors.New("Operations cannot be nil")
	}

	return &LazyDatastore{
		wrapped:      wrapped,
		rw:           sync.RWMutex{},
		active:       false,
		writing:      false,
		closed:       false,
		activateFn:   activateFn,
		deactivateFn: deactivateFn,
		closeFn:      closeFn,
	}, nil
}

// EnsureActive makes sure that LazyDatastore is in active state and runs a DataStoreOp.
// It returns an error if it fails to activate the wrapped Datastore or `op` returns an error.
func (d *LazyDatastore) EnsureActive(op DataStoreOp) error {
	err := d.activateAndLock()
	defer d.unlockActive()
	if err != nil {
		return err
	}
	return op()
}

// Activate the LazyDatastore.
// It doesn't ensure that LazyDatastore will be kept in `active` state, use EnsureActive for that.
func (d *LazyDatastore) Activate() error {
	return d.EnsureActive(func() error { return nil })
}

// Deactivate the LazyDatastore.
func (d *LazyDatastore) Deactivate() error {
	d.rw.Lock()
	defer d.rw.Unlock()
	if d.closed {
		return ErrClosed
	}
	if !d.active {
		return nil
	}
	if err := d.deactivateFn(d.wrapped); err != nil {
		return err
	}
	d.active = false
	return nil
}

// Close the LazyDatastore.
// Return `ErrClosed` if already closed.
// Close will NOT call `wrapped`'s closer, `closeFn` should do it.
func (d *LazyDatastore) Close() error {
	d.rw.Lock()
	defer d.rw.Unlock()
	if d.closed {
		return ErrClosed
	}
	if err := d.closeFn(d.wrapped); err != nil {
		return err
	}
	d.closed = true
	return nil
}

// activateAndLock checks if the datastore is active and locks it to `active` state.
// If it's not `active`, change l.rw to exclusive write lock and activate it.
// Then prevent it's state from being changed. Return any error occurred while activating.
// It is the caller's responsibility to make sure to call `unlockActive` in the same goroutine
// after calling this function.
func (d *LazyDatastore) activateAndLock() error {
	d.rw.RLock()
	if d.active {
		return nil
	}
	if d.closed {
		return ErrClosed
	}
	d.rw.RUnlock()

	// Upgrade to exclusive write lock if LazyDatastore is inactive.
	d.rw.Lock()
	// Double check
	if d.active {
		return nil
	}
	if d.closed {
		return ErrClosed
	}
	d.writing = true
	if err := d.activateFn(d.wrapped); err != nil {
		return err
	}
	d.active = true
	return nil
}

// unlockActive unlock the Datastore allowing it's state to change from `active`.
func (d *LazyDatastore) unlockActive() {
	if !d.writing {
		d.rw.RUnlock()
	} else {
		d.writing = false
		d.rw.Unlock()
	}
}

// Put implements Datastore.Put
func (d *LazyDatastore) Put(ctx context.Context, key key.Key, value []byte) (err error) {
	return d.EnsureActive(func() error {
		return d.wrapped.Put(ctx, key, value)
	})
}

// Delete implements Datastore.Delete
func (d *LazyDatastore) Delete(ctx context.Context, key key.Key) (err error) {
	return d.EnsureActive(func() error {
		return d.wrapped.Delete(ctx, key)
	})
}

// Get implements Datastore.Get
func (d *LazyDatastore) Get(ctx context.Context, key key.Key) (value []byte, err error) {
	err = d.EnsureActive(func() error {
		value, err = d.wrapped.Get(ctx, key)
		return err
	})
	return value, err
}

// Has implements Datastore.Has
func (d *LazyDatastore) Has(ctx context.Context, key key.Key) (exists bool, err error) {
	err = d.EnsureActive(func() error {
		exists, err = d.wrapped.Has(ctx, key)
		return err
	})
	return exists, err
}

// GetSize implements Datastore.GetSize
func (d *LazyDatastore) GetSize(ctx context.Context, key key.Key) (size int, err error) {
	err = d.EnsureActive(func() error {
		size, err = d.wrapped.GetSize(ctx, key)
		return err
	})
	return size, err
}

// Query implements Datastore.Query
func (d *LazyDatastore) Query(ctx context.Context, q query.Query) (rs query.Results, err error) {
	err = d.EnsureActive(func() error {
		rs, err = d.wrapped.Query(ctx, q)
		return err
	})
	return rs, err
}

// Sync implements Datastore.Sync
func (d *LazyDatastore) Sync(ctx context.Context, prefix key.Key) error {
	return d.EnsureActive(func() error {
		return d.wrapped.Sync(ctx, prefix)
	})
}
