// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/hash"
)

func TestGetRequestBatch(t *testing.T) {
	assert := assert.New(t)
	h0 := hash.Parse("00000000000000000000000000000000")
	c1 := NewChunk([]byte("abc"))
	h1 := c1.Hash()
	c2 := NewChunk([]byte("123"))
	h2 := c2.Hash()

	tally := func(b bool, trueCnt, falseCnt *int) {
		if b {
			*trueCnt++
		} else {
			*falseCnt++
		}
	}

	req0chan := make(chan bool, 1)
	req1chan := make(chan *Chunk, 1)
	req2chan := make(chan bool, 1)
	req3chan := make(chan bool, 1)
	req4chan := make(chan *Chunk, 1)
	defer func() { close(req0chan); close(req1chan); close(req2chan); close(req3chan); close(req4chan) }()

	batch := ReadBatch{
		h0: []OutstandingRequest{OutstandingAbsent(req0chan), OutstandingGet(req1chan)},
		h1: []OutstandingRequest{OutstandingAbsent(req2chan)},
		h2: []OutstandingRequest{OutstandingAbsent(req3chan), OutstandingGet(req4chan)},
	}
	go func() {
		for requestedHash, reqs := range batch {
			for _, req := range reqs {
				if requestedHash == h1 {
					req.Satisfy(h1, &c1)
					delete(batch, h1)
				} else if requestedHash == h2 {
					req.Satisfy(h2, &c2)
					delete(batch, h2)
				}
			}
		}
		batch.Close()
	}()

	var r0True, r0False, r2True, r2False, r3True, r3False int
	b := <-req0chan
	tally(b, &r0True, &r0False)
	c := <-req1chan
	assert.EqualValues(EmptyChunk.Hash(), c.Hash())
	b = <-req2chan
	tally(b, &r2True, &r2False)
	b = <-req3chan
	tally(b, &r3True, &r3False)
	c = <-req4chan
	assert.EqualValues(c2.Hash(), c.Hash())

	assert.Equal(1, r0True)
	assert.Equal(0, r0False)
	assert.Equal(0, r2True)
	assert.Equal(1, r2False)
	assert.Equal(0, r3True)
	assert.Equal(1, r3False)
}

func TestGetManyRequestBatch(t *testing.T) {
	assert := assert.New(t)
	h0 := hash.Parse("00000000000000000000000000000000")
	c1 := NewChunk([]byte("abc"))
	h1 := c1.Hash()
	c2 := NewChunk([]byte("123"))
	h2 := c2.Hash()

	chunks := make(chan *Chunk)
	hashes := hash.NewHashSet(h0, h1, h2)
	wg := &sync.WaitGroup{}
	wg.Add(len(hashes))
	go func() { wg.Wait(); close(chunks) }()

	req := NewGetManyRequest(hashes, wg, chunks)
	batch := ReadBatch{
		h0: {req.Outstanding()},
		h1: {req.Outstanding()},
		h2: {req.Outstanding()},
	}
	go func() {
		for reqHash, reqs := range batch {
			for _, req := range reqs {
				if reqHash == h1 {
					req.Satisfy(h1, &c1)
					delete(batch, h1)
				} else if reqHash == h2 {
					req.Satisfy(h2, &c2)
					delete(batch, h2)
				}
			}
		}
		batch.Close()
	}()

	for c := range chunks {
		hashes.Remove(c.Hash())
	}
	assert.Len(hashes, 1)
	assert.True(hashes.Has(h0))
}

func TestAbsentManyRequestBatch(t *testing.T) {
	assert := assert.New(t)
	h0 := hash.Parse("00000000000000000000000000000000")
	c1 := NewChunk([]byte("abc"))
	h1 := c1.Hash()
	c2 := NewChunk([]byte("123"))
	h2 := c2.Hash()

	found := make(chan hash.Hash)
	hashes := hash.NewHashSet(h0, h1, h2)
	wg := &sync.WaitGroup{}
	wg.Add(len(hashes))
	go func() { wg.Wait(); close(found) }()

	req := NewAbsentManyRequest(hashes, wg, found)
	batch := ReadBatch{}
	for h := range req.Hashes() {
		batch[h] = []OutstandingRequest{req.Outstanding()}
	}
	go func() {
		for reqHash, reqs := range batch {
			for _, req := range reqs {
				if reqHash == h1 {
					req.Satisfy(h1, &EmptyChunk)
					delete(batch, h1)
				} else if reqHash == h2 {
					req.Satisfy(h2, &EmptyChunk)
					delete(batch, h2)
				}
			}
		}
		batch.Close()
	}()

	for h := range found {
		hashes.Remove(h)
	}
	assert.Len(hashes, 1)
	assert.True(hashes.Has(h0))
}
