package chunks

import (
	"sync"

	"github.com/attic-labs/noms/d"
)

type statKeeper struct {
	stats   map[string]int64
	chans   map[string]chan int64
	width   int
	wg      *sync.WaitGroup
	stopped bool
}

func newStatKeeper(w int) *statKeeper {
	return &statKeeper{
		stats: map[string]int64{},
		chans: map[string]chan int64{},
		width: w,
		wg:    &sync.WaitGroup{},
	}
}

// AddStat prepares the keeper to record samples for a stat named n
func (sk *statKeeper) AddStat(n string) {
	if _, present := sk.chans[n]; !present {
		sk.wg.Add(1)
		sk.chans[n] = make(chan int64, sk.width)
		go func() {
			for sample := range sk.chans[n] {
				sk.stats[n] += sample
			}
			sk.wg.Done()
		}()
	}
	return
}

// Chan returns a channel over which the caller can pass samples to be added to the accumulator for n
func (sk *statKeeper) Chan(n string) chan<- int64 {
	c, ok := sk.chans[n]
	d.Chk.True(ok, "Stat %s is unknown", n)
	return c
}

// Get returns the accumulated value for n. It's an error to call Get() before Stop()
func (sk *statKeeper) Get(n string) int64 {
	d.Chk.True(sk.stopped, "Calling Get() before Stop() is an error")
	return sk.stats[n]
}

// Has returns whether there's an accumulator for n. It's an error to call Has() before Stop()
func (sk *statKeeper) Has(n string) (has bool) {
	d.Chk.True(sk.stopped, "Calling Has() before Stop() is an error")
	_, has = sk.stats[n]
	return
}

// Stop stops accumulating samples. It's an error to try to use channels returned by Chan() after Stop() until you've re-added new stats using AddStat
func (sk *statKeeper) Stop() {
	for _, c := range sk.chans {
		close(c)
	}
	sk.stopped = true
	sk.wg.Wait()
	sk.chans = nil
}
