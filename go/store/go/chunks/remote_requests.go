// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"sync"

	"github.com/attic-labs/noms/go/hash"
)

type ReadRequest interface {
	Hashes() hash.HashSet
	Outstanding() OutstandingRequest
}

func NewGetRequest(r hash.Hash, ch chan<- *Chunk) GetRequest {
	return GetRequest{hash.HashSet{r: struct{}{}}, ch}
}

type GetRequest struct {
	hashes hash.HashSet
	ch     chan<- *Chunk
}

func NewGetManyRequest(hashes hash.HashSet, wg *sync.WaitGroup, ch chan<- *Chunk) GetManyRequest {
	return GetManyRequest{hashes, wg, ch}
}

type GetManyRequest struct {
	hashes hash.HashSet
	wg     *sync.WaitGroup
	ch     chan<- *Chunk
}

func NewAbsentRequest(r hash.Hash, ch chan<- bool) AbsentRequest {
	return AbsentRequest{hash.HashSet{r: struct{}{}}, ch}
}

type AbsentRequest struct {
	hashes hash.HashSet
	ch     chan<- bool
}

func NewAbsentManyRequest(hashes hash.HashSet, wg *sync.WaitGroup, ch chan<- hash.Hash) AbsentManyRequest {
	return AbsentManyRequest{hashes, wg, ch}
}

type AbsentManyRequest struct {
	hashes hash.HashSet
	wg     *sync.WaitGroup
	ch     chan<- hash.Hash
}

func (g GetRequest) Hashes() hash.HashSet {
	return g.hashes
}

func (g GetRequest) Outstanding() OutstandingRequest {
	return OutstandingGet(g.ch)
}

func (g GetManyRequest) Hashes() hash.HashSet {
	return g.hashes
}

func (g GetManyRequest) Outstanding() OutstandingRequest {
	return OutstandingGetMany{g.wg, g.ch}
}

func (h AbsentRequest) Hashes() hash.HashSet {
	return h.hashes
}

func (h AbsentRequest) Outstanding() OutstandingRequest {
	return OutstandingAbsent(h.ch)
}

func (h AbsentManyRequest) Hashes() hash.HashSet {
	return h.hashes
}

func (h AbsentManyRequest) Outstanding() OutstandingRequest {
	return OutstandingAbsentMany{h.wg, h.ch}
}

type OutstandingRequest interface {
	Satisfy(h hash.Hash, c *Chunk)
	Fail()
}

type OutstandingGet chan<- *Chunk
type OutstandingGetMany struct {
	wg *sync.WaitGroup
	ch chan<- *Chunk
}
type OutstandingAbsent chan<- bool
type OutstandingAbsentMany struct {
	wg *sync.WaitGroup
	ch chan<- hash.Hash
}

func (r OutstandingGet) Satisfy(h hash.Hash, c *Chunk) {
	r <- c
}

func (r OutstandingGet) Fail() {
	r <- &EmptyChunk
}

func (ogm OutstandingGetMany) Satisfy(h hash.Hash, c *Chunk) {
	ogm.ch <- c
	ogm.wg.Done()
}

func (ogm OutstandingGetMany) Fail() {
	ogm.wg.Done()
}

func (oh OutstandingAbsent) Satisfy(h hash.Hash, c *Chunk) {
	oh <- false
}

func (oh OutstandingAbsent) Fail() {
	oh <- true
}

func (ohm OutstandingAbsentMany) Satisfy(h hash.Hash, c *Chunk) {
	ohm.ch <- h
	ohm.wg.Done()
}

func (ohm OutstandingAbsentMany) Fail() {
	ohm.wg.Done()
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
