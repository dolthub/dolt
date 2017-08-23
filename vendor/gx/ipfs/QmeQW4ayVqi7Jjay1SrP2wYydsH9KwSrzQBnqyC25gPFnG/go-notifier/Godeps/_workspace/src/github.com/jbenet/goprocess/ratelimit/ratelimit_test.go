package ratelimit

import (
	"testing"
	"time"

	process "gx/ipfs/QmeQW4ayVqi7Jjay1SrP2wYydsH9KwSrzQBnqyC25gPFnG/go-notifier/Godeps/_workspace/src/github.com/jbenet/goprocess"
)

func TestRateLimitLimitedGoBlocks(t *testing.T) {
	numChildren := 6

	t.Logf("create a rate limiter with limit of %d", numChildren/2)
	rl := NewRateLimiter(process.Background(), numChildren/2)

	doneSpawning := make(chan struct{})
	childClosing := make(chan struct{})

	t.Log("spawn 6 children with LimitedGo.")
	go func() {
		for i := 0; i < numChildren; i++ {
			rl.LimitedGo(func(child process.Process) {
				// hang until we drain childClosing
				childClosing <- struct{}{}
			})
			t.Logf("spawned %d", i)
		}
		close(doneSpawning)
	}()

	t.Log("should have blocked.")
	select {
	case <-doneSpawning:
		t.Error("did not block")
	case <-time.After(time.Millisecond): // for scheduler
		t.Log("blocked")
	}

	t.Logf("drain %d children so they close", numChildren/2)
	for i := 0; i < numChildren/2; i++ {
		t.Logf("closing %d", i)
		<-childClosing // consume child cloing
		t.Logf("closed %d", i)
	}

	t.Log("should be done spawning.")
	select {
	case <-doneSpawning:
	case <-time.After(100 * time.Millisecond): // for scheduler
		t.Error("still blocked...")
	}

	t.Logf("drain %d children so they close", numChildren/2)
	for i := 0; i < numChildren/2; i++ {
		<-childClosing
		t.Logf("closed %d", i)
	}

	rl.Close() // ensure everyone's closed.
}

func TestRateLimitGoDoesntBlock(t *testing.T) {
	numChildren := 6

	t.Logf("create a rate limiter with limit of %d", numChildren/2)
	rl := NewRateLimiter(process.Background(), numChildren/2)

	doneSpawning := make(chan struct{})
	childClosing := make(chan struct{})

	t.Log("spawn 6 children with usual Process.Go.")
	go func() {
		for i := 0; i < numChildren; i++ {
			rl.Go(func(child process.Process) {
				// hang until we drain childClosing
				childClosing <- struct{}{}
			})
			t.Logf("spawned %d", i)
		}
		close(doneSpawning)
	}()

	t.Log("should not have blocked.")
	select {
	case <-doneSpawning:
		t.Log("did not block")
	case <-time.After(100 * time.Millisecond): // for scheduler
		t.Error("process.Go blocked. it should not.")
	}

	t.Log("drain children so they close")
	for i := 0; i < numChildren; i++ {
		<-childClosing
		t.Logf("closed %d", i)
	}

	rl.Close() // ensure everyone's closed.
}
