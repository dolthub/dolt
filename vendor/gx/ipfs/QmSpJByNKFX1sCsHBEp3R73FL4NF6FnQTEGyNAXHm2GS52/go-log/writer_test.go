package log

import (
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type hangwriter struct {
	c      chan struct{}
	closer *sync.Once
}

func newHangWriter() *hangwriter {
	return &hangwriter{
		c:      make(chan struct{}),
		closer: new(sync.Once),
	}
}

func (hw *hangwriter) Write([]byte) (int, error) {
	<-hw.c
	return 0, fmt.Errorf("write on closed writer")
}

func (hw *hangwriter) Close() error {
	hw.closer.Do(func() {
		close(hw.c)
	})

	return nil
}

func TestMirrorWriterHang(t *testing.T) {
	mw := NewMirrorWriter()

	hw := newHangWriter()
	pr, pw := io.Pipe()

	mw.AddWriter(hw)
	mw.AddWriter(pw)

	msg := "Hello!"
	mw.Write([]byte(msg))

	// make sure writes through can happen even with one writer hanging
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 10)
		n, err := pr.Read(buf)
		if err != nil {
			t.Fatal(err)
		}

		if n != len(msg) {
			t.Fatal("read wrong amount")
		}

		if string(buf[:n]) != msg {
			t.Fatal("didnt read right content")
		}

		done <- struct{}{}
	}()

	select {
	case <-time.After(time.Second * 5):
		t.Fatal("write to mirrorwriter hung")
	case <-done:
	}

	if !mw.Active() {
		t.Fatal("writer should still be active")
	}

	pw.Close()

	if !mw.Active() {
		t.Fatal("writer should still be active")
	}

	// now we just have the hangwriter

	// write a bunch to it
	buf := make([]byte, 8192)
	for i := 0; i < 128; i++ {
		mw.Write(buf)
	}

	// wait for goroutines to sync up
	time.Sleep(time.Millisecond * 500)

	// the hangwriter should have been killed, causing the mirrorwriter to be inactive now
	if mw.Active() {
		t.Fatal("should be inactive now")
	}
}

func TestStress(t *testing.T) {
	mw := NewMirrorWriter()

	nreaders := 20

	var readers []io.Reader
	for i := 0; i < nreaders; i++ {
		pr, pw := io.Pipe()
		mw.AddWriter(pw)
		readers = append(readers, pr)
	}

	hashout := make(chan []byte)

	numwriters := 20
	writesize := 1024
	writecount := 300

	f := func(r io.Reader) {
		h := fnv.New64a()
		sum, err := io.Copy(h, r)
		if err != nil {
			t.Fatal(err)
		}

		if sum != int64(numwriters*writesize*writecount) {
			t.Fatal("read wrong number of bytes")
		}

		hashout <- h.Sum(nil)
	}

	for _, r := range readers {
		go f(r)
	}

	work := sync.WaitGroup{}
	for i := 0; i < numwriters; i++ {
		work.Add(1)
		go func() {
			defer work.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			buf := make([]byte, writesize)
			for j := 0; j < writecount; j++ {
				r.Read(buf)
				mw.Write(buf)
				time.Sleep(time.Millisecond * 5)
			}
		}()
	}

	work.Wait()
	mw.Close()

	check := make(map[string]bool)
	for i := 0; i < nreaders; i++ {
		h := <-hashout
		check[string(h)] = true
	}

	if len(check) > 1 {
		t.Fatal("writers received different data!")
	}
}
