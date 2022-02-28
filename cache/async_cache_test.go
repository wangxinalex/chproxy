package cache

import (
	"errors"
	"github.com/contentsquare/chproxy/config"
	"os"
	"testing"
	"time"
)

const asyncTestDir = "./async-test-data"

func TestAsyncCache_Cleanup_Of_Expired_Transactions(t *testing.T) {
	graceTime := 100 * time.Millisecond
	asyncCache := newAsyncTestCache(t, graceTime)
	defer func() {
		asyncCache.Close()
		os.RemoveAll(asyncTestDir)
	}()

	key := &Key{
		Query: []byte("SELECT async cache"),
	}
	_, err := asyncCache.Status(key)

	if !errors.Is(err, ErrMissingTransaction) {
		t.Fatalf("unexpected behaviour: transaction isnt done while it wasnt even started")
	}

	if err := asyncCache.Create(key); err != nil {
		t.Fatalf("unexpected error: %s failed to register transaction", err)
	}

	status, err := asyncCache.Status(key)
	if err != nil || !status.IsPending() {
		t.Fatalf("unexpected behaviour: transaction isnt finished")
	}

	time.Sleep(graceTime * 2)

	status, err = asyncCache.Status(key)
	if !errors.Is(err, ErrMissingTransaction) || status.IsPending() {
		t.Fatalf("unexpected behaviour: transaction grace time elapsed and yet it was still pending")
	}
}

func TestAsyncCache_AwaitForConcurrentTransaction_GraceTimeWithoutTransactionCompletion(t *testing.T) {
	graceTime := 300 * time.Millisecond
	asyncCache := newAsyncTestCache(t, graceTime)

	defer func() {
		asyncCache.Close()
		os.RemoveAll(asyncTestDir)
	}()

	key := &Key{
		Query: []byte("SELECT async cache AwaitForConcurrentTransaction"),
	}

	_, err := asyncCache.Status(key)
	if !errors.Is(err, ErrMissingTransaction) {
		t.Fatalf("unexpected behaviour: transaction isnt done while it wasnt even started")
	}

	if err := asyncCache.Create(key); err != nil {
		t.Fatalf("unexpected error: %s failed to register transaction", err)
	}

	status, err := asyncCache.Status(key)
	if err != nil || !status.IsPending() {
		t.Fatalf("unexpected behaviour: transaction isnt finished")
	}

	startTime := time.Now()
	transactionResult, err := asyncCache.AwaitForConcurrentTransaction(key)
	if err != nil {
		t.Fatalf("unexpected behaviour while awaiting concurrent transaction: %v", err)
	}
	elapsedTime := time.Since(startTime)

	// in order to let the cleaner swipe the transaction
	time.Sleep(100 * time.Millisecond)
	if !transactionResult.State.IsPending() {
		t.Fatalf("unexpected behaviour: transaction awaiting time elapsed %s", elapsedTime.String())
	}
}

func TestAsyncCache_AwaitForConcurrentTransaction_TransactionCompletedWhileAwaiting(t *testing.T) {
	graceTime := 300 * time.Millisecond
	asyncCache := newAsyncTestCache(t, graceTime)

	defer func() {
		asyncCache.Close()
		os.RemoveAll(asyncTestDir)
	}()
	key := &Key{
		Query: []byte("SELECT async cache AwaitForConcurrentTransactionCompleted"),
	}

	if err := asyncCache.Create(key); err != nil {
		t.Fatalf("unexpected error: %s failed to register transaction", err)
	}

	errs := make(chan error)
	go func() {
		time.Sleep(graceTime / 2)
		if err := asyncCache.Complete(key); err != nil {
			errs <- err
		} else {
			errs <- nil
		}
	}()

	startTime := time.Now()
	transactionResult, err := asyncCache.AwaitForConcurrentTransaction(key)
	if err != nil {
		t.Fatalf("unexpected error: %s failed to unregister transaction", err)
	}

	elapsedTime := time.Since(startTime)

	err = <-errs
	if err != nil {
		t.Fatalf("unexpected error: %s failed to unregister transaction", err)
	}

	if !transactionResult.State.IsCompleted() || elapsedTime >= graceTime {
		t.Fatalf("unexpected behaviour: transaction awaiting time elapsed %s", elapsedTime.String())
	}
}

func TestAsyncCache_AwaitForConcurrentTransaction_TransactionFailedWhileAwaiting(t *testing.T) {
	graceTime := 300 * time.Millisecond
	asyncCache := newAsyncTestCache(t, graceTime)

	defer func() {
		asyncCache.Close()
		os.RemoveAll(asyncTestDir)
	}()

	key := &Key{
		Query: []byte("SELECT async cache AwaitForConcurrentTransactionCompleted"),
	}

	if err := asyncCache.Create(key); err != nil {
		t.Fatalf("unexpected error: %s failed to register transaction", err)
	}

	errs := make(chan error)
	go func() {
		time.Sleep(graceTime / 2)
		if err := asyncCache.Fail(key); err != nil {
			errs <- err
		} else {
			errs <- nil
		}
	}()

	startTime := time.Now()
	transactionResult, err := asyncCache.AwaitForConcurrentTransaction(key)
	if err != nil {
		t.Fatalf("unexpected error: %s failed to unregister transaction", err)
	}

	elapsedTime := time.Since(startTime)

	err = <-errs
	if err != nil {
		t.Fatalf("unexpected error: %s failed to unregister transaction", err)
	}

	if !transactionResult.State.IsFailed() || elapsedTime >= graceTime {
		t.Fatalf("unexpected behaviour: transaction awaiting time elapsed %s", elapsedTime.String())
	}
}

func newAsyncTestCache(t *testing.T, graceTime time.Duration) *AsyncCache {
	t.Helper()
	cfg := config.Cache{
		Name: "foobar",
		FileSystem: config.FileSystemCacheConfig{
			Dir:     asyncTestDir,
			MaxSize: 1e6,
		},
		Expire: config.Duration(time.Minute),
	}
	c, err := newFilesSystemCache(cfg, graceTime)
	if err != nil {
		t.Fatal(err)
	}

	asyncC := &AsyncCache{
		Cache:               c,
		TransactionRegistry: newInMemoryTransactionRegistry(graceTime),
		graceTime:           graceTime,
	}
	return asyncC
}
