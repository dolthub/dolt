// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import "github.com/attic-labs/noms/go/hash"

type ReadRequest interface {
	Hash() hash.Hash
	Outstanding() OutstandingRequest
}

func NewGetRequest(r hash.Hash, ch chan Chunk) GetRequest {
	return GetRequest{r, ch}
}

type GetRequest struct {
	r  hash.Hash
	ch chan Chunk
}

func NewHasRequest(r hash.Hash, ch chan bool) HasRequest {
	return HasRequest{r, ch}
}

type HasRequest struct {
	r  hash.Hash
	ch chan bool
}

func (g GetRequest) Hash() hash.Hash {
	return g.r
}

func (g GetRequest) Outstanding() OutstandingRequest {
	return OutstandingGet(g.ch)
}

func (h HasRequest) Hash() hash.Hash {
	return h.r
}

func (h HasRequest) Outstanding() OutstandingRequest {
	return OutstandingHas(h.ch)
}

type OutstandingRequest interface {
	Satisfy(c Chunk)
	Fail()
}

type OutstandingGet chan Chunk
type OutstandingHas chan bool

func (r OutstandingGet) Satisfy(c Chunk) {
	r <- c
	close(r)
}

func (r OutstandingGet) Fail() {
	r <- EmptyChunk
	close(r)
}

func (h OutstandingHas) Satisfy(c Chunk) {
	h <- true
	close(h)
}

func (h OutstandingHas) Fail() {
	h <- false
	close(h)
}

// ReadBatch represents a set of queued Get/Has requests, each of which are blocking on a receive channel for a response.
type ReadBatch map[hash.Hash][]OutstandingRequest

// Close ensures that callers to Get() and Has() are failed correctly if the corresponding chunk wasn't in the response from the server (i.e. it wasn't found).
func (rb *ReadBatch) Close() error {
	for _, reqs := range *rb {
		for _, req := range reqs {
			req.Fail()
		}
	}
	return nil
}
