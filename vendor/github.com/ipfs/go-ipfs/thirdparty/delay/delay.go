package delay

import (
	"math/rand"
	"sync"
	"time"
)

var sharedRNG = rand.New(rand.NewSource(time.Now().UnixNano()))

// Delay makes it easy to add (threadsafe) configurable delays to other
// objects.
type D interface {
	Set(time.Duration) time.Duration
	Wait()
	Get() time.Duration
}

// Fixed returns a delay with fixed latency
func Fixed(t time.Duration) D {
	return &delay{t: t}
}

type delay struct {
	l sync.RWMutex
	t time.Duration
}

func (d *delay) Set(t time.Duration) time.Duration {
	d.l.Lock()
	defer d.l.Unlock()
	prev := d.t
	d.t = t
	return prev
}

func (d *delay) Wait() {
	d.l.RLock()
	defer d.l.RUnlock()
	time.Sleep(d.t)
}

func (d *delay) Get() time.Duration {
	d.l.Lock()
	defer d.l.Unlock()
	return d.t
}

// VariableNormal is a delay following a normal distribution
// Notice that to implement the D interface Set can only change the mean delay
// the standard deviation is set only at initialization
func VariableNormal(t, std time.Duration, rng *rand.Rand) D {
	if rng == nil {
		rng = sharedRNG
	}

	v := &variableNormal{
		std: std,
		rng: rng,
	}
	v.t = t
	return v
}

type variableNormal struct {
	delay
	std time.Duration
	rng *rand.Rand
}

func (d *variableNormal) Wait() {
	d.l.RLock()
	defer d.l.RUnlock()
	randomDelay := time.Duration(d.rng.NormFloat64() * float64(d.std))
	time.Sleep(randomDelay + d.t)
}

// VariableUniform is a delay following a uniform distribution
// Notice that to implement the D interface Set can only change the minimum delay
// the delta is set only at initialization
func VariableUniform(t, d time.Duration, rng *rand.Rand) D {
	if rng == nil {
		rng = sharedRNG
	}

	v := &variableUniform{
		d:   d,
		rng: rng,
	}
	v.t = t
	return v
}

type variableUniform struct {
	delay
	d   time.Duration // max delta
	rng *rand.Rand
}

func (d *variableUniform) Wait() {
	d.l.RLock()
	defer d.l.RUnlock()
	randomDelay := time.Duration(d.rng.Float64() * float64(d.d))
	time.Sleep(randomDelay + d.t)
}
