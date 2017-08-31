// Copyright 2014 The Go Authors.
// See https://code.google.com/p/go/source/browse/CONTRIBUTORS
// Licensed under the same terms as Go itself:
// https://code.google.com/p/go/source/browse/LICENSE

package bufpipe

import (
	"sync"
)

type Pipe struct {
	b Buffer
	c sync.Cond
	m sync.Mutex
}

// Read waits until data is available and copies bytes
// from the buffer into p.
func (r *Pipe) Read(p []byte) (n int, err error) {
	r.c.L.Lock()
	defer r.c.L.Unlock()
	for r.b.Len() == 0 && !r.b.closed {
		r.c.Wait()
	}
	return r.b.Read(p)
}

// Write copies bytes from p into the buffer and wakes a reader.
// It is an error to write more data than the buffer can hold.
func (w *Pipe) Write(p []byte) (n int, err error) {
	w.c.L.Lock()
	defer w.c.L.Unlock()
	defer w.c.Signal()
	return w.b.Write(p)
}

func (c *Pipe) Close(err error) {
	c.c.L.Lock()
	defer c.c.L.Unlock()
	defer c.c.Signal()
	c.b.Close(err)
}

func NewBufferedPipe(size int) *Pipe {
	p := &Pipe{
		b: Buffer{buf: make([]byte, size)},
	}

	p.c.L = &p.m
	return p
}
