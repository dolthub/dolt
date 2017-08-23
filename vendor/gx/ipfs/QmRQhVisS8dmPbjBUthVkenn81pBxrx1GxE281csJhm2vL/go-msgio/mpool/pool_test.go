// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Pool is no-op under race detector, so all these tests do not work.
// +build !race

package mpool

import (
	"fmt"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	// disable GC so we can control when it happens.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	var p Pool
	if p.Get(10) != nil {
		t.Fatal("expected empty")
	}
	p.Put(16, "a")
	p.Put(2048, "b")
	if g := p.Get(16); g != "a" {
		t.Fatalf("got %#v; want a", g)
	}
	if g := p.Get(2048); g != "b" {
		t.Fatalf("got %#v; want b", g)
	}
	if g := p.Get(16); g != nil {
		t.Fatalf("got %#v; want nil", g)
	}
	if g := p.Get(2048); g != nil {
		t.Fatalf("got %#v; want nil", g)
	}
	if g := p.Get(1); g != nil {
		t.Fatalf("got %#v; want nil", g)
	}
	p.Put(1023, "d")
	if g := p.Get(1024); g != nil {
		t.Fatalf("got %#v; want nil", g)
	}
	if g := p.Get(512); g != "d" {
		t.Fatalf("got %#v; want d", g)
	}

	debug.SetGCPercent(100) // to allow following GC to actually run
	runtime.GC()
	if g := p.Get(10); g != nil {
		t.Fatalf("got %#v; want nil after GC", g)
	}
}

func TestPoolNew(t *testing.T) {
	// disable GC so we can control when it happens.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	s := [32]int{}
	p := Pool{
		New: func(length int) interface{} {
			idx := nextPowerOfTwo(uint32(length))
			s[idx]++
			return s[idx]
		},
	}
	if v := p.Get(1 << 5); v != 1 {
		t.Fatalf("got %v; want 1", v)
	}
	if v := p.Get(1 << 2); v != 1 {
		t.Fatalf("got %v; want 1", v)
	}
	if v := p.Get(1 << 2); v != 2 {
		t.Fatalf("got %v; want 2", v)
	}
	if v := p.Get(1 << 5); v != 2 {
		t.Fatalf("got %v; want 2", v)
	}
	p.Put(1<<2, 42)
	p.Put(1<<5, 42)
	if v := p.Get(1 << 2); v != 42 {
		t.Fatalf("got %v; want 42", v)
	}
	if v := p.Get(1 << 2); v != 3 {
		t.Fatalf("got %v; want 3", v)
	}
	if v := p.Get(1 << 5); v != 42 {
		t.Fatalf("got %v; want 42", v)
	}
	if v := p.Get(1 << 5); v != 3 {
		t.Fatalf("got %v; want 3", v)
	}
}

// Test that Pool does not hold pointers to previously cached
// resources
func TestPoolGC(t *testing.T) {
	var p Pool
	var fin uint32
	const N = 100
	for i := 0; i < N; i++ {
		v := new(string)
		runtime.SetFinalizer(v, func(vv *string) {
			atomic.AddUint32(&fin, 1)
		})
		p.Put(uint32(i), v)
	}
	for i := 0; i < N; i++ {
		p.Get(uint32(i))
	}
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(time.Duration(i*100+10) * time.Millisecond)
		// 1 pointer can remain on stack or elsewhere
		if atomic.LoadUint32(&fin) >= N-1 {
			return
		}
	}
	t.Fatalf("only %v out of %v resources are finalized",
		atomic.LoadUint32(&fin), N)
}

func TestPoolStress(t *testing.T) {
	const P = 10
	N := int(1e6)
	if testing.Short() {
		N /= 100
	}
	var p Pool
	done := make(chan bool)
	for i := 0; i < P; i++ {
		go func() {
			var v interface{} = 0
			for j := 0; j < N; j++ {
				if v == nil {
					v = 0
				}
				p.Put(uint32(j), v)
				v = p.Get(uint32(j))
				if v != nil && v.(int) != 0 {
					t.Fatalf("expect 0, got %v", v)
				}
			}
			done <- true
		}()
	}
	for i := 0; i < P; i++ {
		// fmt.Printf("%d/%d\n", i, P)
		<-done
	}
}

func TestPoolStressByteSlicePool(t *testing.T) {
	const P = 10
	chs := 10
	maxSize := uint32(1 << 16)
	N := int(1e4)
	if testing.Short() {
		N /= 100
	}
	p := ByteSlicePool
	done := make(chan bool)
	errs := make(chan error)
	for i := 0; i < P; i++ {
		go func() {
			ch := make(chan []byte, chs+1)

			for i := 0; i < chs; i++ {
				j := rand.Uint32() % maxSize
				ch <- p.Get(j).([]byte)
			}

			for j := 0; j < N; j++ {
				r := uint32(0)
				for i := 0; i < chs; i++ {
					v := <-ch
					p.Put(uint32(cap(v)), v)
					r = rand.Uint32() % maxSize
					v = p.Get(r).([]byte)
					if uint32(len(v)) < r {
						errs <- fmt.Errorf("expect len(v) >= %d, got %d", j, len(v))
					}
					ch <- v
				}

				if r%1000 == 0 {
					runtime.GC()
				}
			}
			done <- true
		}()
	}

	for i := 0; i < P; {
		select {
		case <-done:
			i++
			// fmt.Printf("%d/%d\n", i, P)
		case err := <-errs:
			t.Error(err)
		}
	}
}

func BenchmarkPool(b *testing.B) {
	var p Pool
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i = i << 1
			p.Put(uint32(i), 1)
			p.Get(uint32(i))
		}
	})
}

func BenchmarkPoolOverlflow(b *testing.B) {
	var p Pool
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for pow := uint32(0); pow < 32; pow++ {
				for b := 0; b < 100; b++ {
					p.Put(uint32(1<<pow), 1)
				}
			}
			for pow := uint32(0); pow < 32; pow++ {
				for b := 0; b < 100; b++ {
					p.Get(uint32(1 << pow))
				}
			}
		}
	})
}

func ExamplePool() {
	var p Pool

	small := make([]byte, 1024)
	large := make([]byte, 4194304)
	p.Put(uint32(len(small)), small)
	p.Put(uint32(len(large)), large)

	small2 := p.Get(uint32(len(small))).([]byte)
	large2 := p.Get(uint32(len(large))).([]byte)
	fmt.Println("small2 len:", len(small2))
	fmt.Println("large2 len:", len(large2))
	// Output:
	// small2 len: 1024
	// large2 len: 4194304
}
