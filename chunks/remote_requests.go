package chunks

import "github.com/attic-labs/noms/ref"

type readRequest interface {
	Ref() ref.Ref
	Outstanding() outstandingRequest
}

type getRequest struct {
	r  ref.Ref
	ch chan Chunk
}

type hasRequest struct {
	r  ref.Ref
	ch chan bool
}

func (g getRequest) Ref() ref.Ref {
	return g.r
}

func (g getRequest) Outstanding() outstandingRequest {
	return outstandingGet(g.ch)
}

func (h hasRequest) Ref() ref.Ref {
	return h.r
}

func (h hasRequest) Outstanding() outstandingRequest {
	return outstandingHas(h.ch)
}

type outstandingRequest interface {
	Satisfy(c Chunk)
	Fail()
}

type outstandingGet chan Chunk
type outstandingHas chan bool

func (r outstandingGet) Satisfy(c Chunk) {
	r <- c
	close(r)
}

func (r outstandingGet) Fail() {
	r <- EmptyChunk
	close(r)
}

func (h outstandingHas) Satisfy(c Chunk) {
	h <- true
	close(h)
}

func (h outstandingHas) Fail() {
	h <- false
	close(h)
}

// readBatch represents a set of queued Get/Has requests, each of which are blocking on a receive channel for a response.
type readBatch map[ref.Ref][]outstandingRequest

// getBatch represents a set of queued Get requests, each of which are blocking on a receive channel for a response.
type getBatch map[ref.Ref][]chan Chunk
type hasBatch map[ref.Ref][]chan bool

// Close ensures that callers to Get() and Has() are failed correctly if the corresponding chunk wasn't in the response from the server (i.e. it wasn't found).
func (rb *readBatch) Close() error {
	for _, reqs := range *rb {
		for _, req := range reqs {
			req.Fail()
		}
	}
	return nil
}

// Close ensures that callers to Get() must receive nil if the corresponding chunk wasn't in the response from the server (i.e. it wasn't found).
func (gb *getBatch) Close() error {
	for _, chs := range *gb {
		for _, ch := range chs {
			ch <- EmptyChunk
		}
	}
	return nil
}

// Put is implemented so that getBatch implements the ChunkSink interface.
func (gb *getBatch) Put(c Chunk) {
	for _, ch := range (*gb)[c.Ref()] {
		ch <- c
	}

	delete(*gb, c.Ref())
}
