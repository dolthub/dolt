package periodicproc

import (
	"testing"
	"time"

	ci "github.com/jbenet/go-cienv"
	gp "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"
)

var (
	grace    = time.Millisecond * 5
	interval = time.Millisecond * 10
	timeout  = time.Second * 5
)

func init() {
	if ci.IsRunning() {
		grace = time.Millisecond * 500
		interval = time.Millisecond * 1000
		timeout = time.Second * 15
	}
}

func between(min, diff, max time.Duration) bool {
	return min <= diff && diff <= max
}

func testBetween(t *testing.T, min, diff, max time.Duration) {
	if !between(min, diff, max) {
		t.Error("time diff incorrect:", min, diff, max)
	}
}

type intervalFunc func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process)

func testSeq(t *testing.T, toTest intervalFunc) {
	t.Parallel()

	last := time.Now()
	times := make(chan time.Time, 10)
	p := toTest(times, nil)

	for i := 0; i < 5; i++ {
		next := <-times
		testBetween(t, interval-grace, next.Sub(last), interval+grace)
		last = next
	}

	go p.Close()
	select {
	case <-p.Closed():
	case <-time.After(timeout):
		t.Error("proc failed to close")
	}
}

func testSeqWait(t *testing.T, toTest intervalFunc) {
	t.Parallel()

	last := time.Now()
	times := make(chan time.Time, 10)
	wait := make(chan struct{})
	p := toTest(times, wait)

	for i := 0; i < 5; i++ {
		next := <-times
		testBetween(t, interval-grace, next.Sub(last), interval+grace)

		<-time.After(interval * 2) // make it wait.
		last = time.Now()          // make it now (sequential)
		wait <- struct{}{}         // release it.
	}

	go p.Close()

	select {
	case <-p.Closed():
	case <-time.After(timeout):
		t.Error("proc failed to close")
	}
}

func testSeqNoWait(t *testing.T, toTest intervalFunc) {
	t.Parallel()

	last := time.Now()
	times := make(chan time.Time, 10)
	wait := make(chan struct{})
	p := toTest(times, wait)

	for i := 0; i < 5; i++ {
		next := <-times
		testBetween(t, 0, next.Sub(last), interval+grace) // min of 0

		<-time.After(interval * 2) // make it wait.
		last = time.Now()          // make it now (sequential)
		wait <- struct{}{}         // release it.
	}

	go p.Close()

end:
	select {
	case wait <- struct{}{}: // drain any extras.
		goto end
	case <-p.Closed():
	case <-time.After(timeout):
		t.Error("proc failed to close")
	}
}

func testParallel(t *testing.T, toTest intervalFunc) {
	t.Parallel()

	last := time.Now()
	times := make(chan time.Time, 10)
	wait := make(chan struct{})
	p := toTest(times, wait)

	for i := 0; i < 5; i++ {
		next := <-times
		testBetween(t, interval-grace, next.Sub(last), interval+grace)
		last = next

		<-time.After(interval * 2) // make it wait.
		wait <- struct{}{}         // release it.
	}

	go p.Close()

end:
	select {
	case wait <- struct{}{}: // drain any extras.
		goto end
	case <-p.Closed():
	case <-time.After(timeout):
		t.Error("proc failed to close")
	}
}

func TestEverySeq(t *testing.T) {
	testSeq(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return Every(interval, func(proc gp.Process) {
			times <- time.Now()
		})
	})
}

func TestEverySeqWait(t *testing.T) {
	testSeqWait(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return Every(interval, func(proc gp.Process) {
			times <- time.Now()
			select {
			case <-wait:
			case <-proc.Closing():
			}
		})
	})
}

func TestEveryGoSeq(t *testing.T) {
	testSeq(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return EveryGo(interval, func(proc gp.Process) {
			times <- time.Now()
		})
	})
}

func TestEveryGoSeqParallel(t *testing.T) {
	testParallel(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return EveryGo(interval, func(proc gp.Process) {
			times <- time.Now()
			select {
			case <-wait:
			case <-proc.Closing():
			}
		})
	})
}

func TestTickSeq(t *testing.T) {
	testSeq(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return Tick(interval, func(proc gp.Process) {
			times <- time.Now()
		})
	})
}

func TestTickSeqNoWait(t *testing.T) {
	testSeqNoWait(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return Tick(interval, func(proc gp.Process) {
			times <- time.Now()
			select {
			case <-wait:
			case <-proc.Closing():
			}
		})
	})
}

func TestTickGoSeq(t *testing.T) {
	testSeq(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return TickGo(interval, func(proc gp.Process) {
			times <- time.Now()
		})
	})
}

func TestTickGoSeqParallel(t *testing.T) {
	testParallel(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return TickGo(interval, func(proc gp.Process) {
			times <- time.Now()
			select {
			case <-wait:
			case <-proc.Closing():
			}
		})
	})
}

func TestTickerSeq(t *testing.T) {
	testSeq(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return Ticker(time.Tick(interval), func(proc gp.Process) {
			times <- time.Now()
		})
	})
}

func TestTickerSeqNoWait(t *testing.T) {
	testSeqNoWait(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return Ticker(time.Tick(interval), func(proc gp.Process) {
			times <- time.Now()
			select {
			case <-wait:
			case <-proc.Closing():
			}
		})
	})
}

func TestTickerGoSeq(t *testing.T) {
	testSeq(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return TickerGo(time.Tick(interval), func(proc gp.Process) {
			times <- time.Now()
		})
	})
}

func TestTickerGoParallel(t *testing.T) {
	testParallel(t, func(times chan<- time.Time, wait <-chan struct{}) (proc gp.Process) {
		return TickerGo(time.Tick(interval), func(proc gp.Process) {
			times <- time.Now()
			select {
			case <-wait:
			case <-proc.Closing():
			}
		})
	})
}
