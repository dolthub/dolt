package testutil

import (
	"math/rand"
	"sync"
	"time"
)

var SeededRand *rand.Rand

func init() {
	SeededRand = NewSeededRand(time.Now().UTC().UnixNano())
}

func NewSeededRand(seed int64) *rand.Rand {
	src := rand.NewSource(seed)
	return rand.New(&LockedRandSource{src: src})
}

type LockedRandSource struct {
	lk  sync.Mutex
	src rand.Source
}

func (r *LockedRandSource) Int63() (n int64) {
	r.lk.Lock()
	n = r.src.Int63()
	r.lk.Unlock()
	return
}

func (r *LockedRandSource) Seed(seed int64) {
	r.lk.Lock()
	r.src.Seed(seed)
	r.lk.Unlock()
}
