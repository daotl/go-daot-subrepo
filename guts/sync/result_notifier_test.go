package sync

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNoError(t *testing.T) {
	assert := assert.New(t)

	r := NewResultNotifier(nil)

	var wg sync.WaitGroup
	wg.Add(2)

	// Fast notifiee calls Done() before Finish()
	go func() {
		err := <-r.Done()
		assert.NoError(err)
		wg.Done()
	}()

	// Slow notifiee calls Done() after Finish()
	go func() {
		time.Sleep(2 * time.Second)
		err := <-r.Done()
		assert.NoError(err)
		wg.Done()
	}()

	time.Sleep(1 * time.Second)
	r.Finish(nil)
	wg.Wait()
}

func TestError(t *testing.T) {
	assert := assert.New(t)

	r := NewResultNotifier(nil)
	err := errors.New("random error")

	var wg sync.WaitGroup
	wg.Add(2)

	// Fast notifiee calls Done() before Finish()
	go func() {
		err := <-r.Done()
		assert.Error(err)
		wg.Done()
	}()

	// Slow notifiee calls Done() after Finish()
	go func() {
		time.Sleep(2 * time.Second)
		err := <-r.Done()
		assert.Error(err)
		wg.Done()
	}()

	time.Sleep(1 * time.Second)
	r.Finish(err)
	wg.Wait()
}
