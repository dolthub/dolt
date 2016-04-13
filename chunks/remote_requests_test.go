package chunks

import (
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestGetRequestBatch(t *testing.T) {
	assert := assert.New(t)
	r0 := ref.Parse("sha1-0000000000000000000000000000000000000000")
	c1 := NewChunk([]byte("abc"))
	r1 := c1.Ref()
	c2 := NewChunk([]byte("123"))
	r2 := c2.Ref()

	tally := func(b bool, trueCnt, falseCnt *int) {
		if b {
			*trueCnt++
		} else {
			*falseCnt++
		}
	}

	req0chan := make(chan bool, 1)
	req1chan := make(chan Chunk, 1)
	req2chan := make(chan bool, 1)
	req3chan := make(chan bool, 1)
	req4chan := make(chan Chunk, 1)

	batch := ReadBatch{
		r0: []OutstandingRequest{OutstandingHas(req0chan), OutstandingGet(req1chan)},
		r1: []OutstandingRequest{OutstandingHas(req2chan)},
		r2: []OutstandingRequest{OutstandingHas(req3chan), OutstandingGet(req4chan)},
	}
	go func() {
		for requestedRef, reqs := range batch {
			for _, req := range reqs {
				if requestedRef == r1 {
					req.Satisfy(c1)
					delete(batch, r1)
				} else if requestedRef == r2 {
					req.Satisfy(c2)
					delete(batch, r2)
				}
			}
		}
	}()
	var r1True, r1False, r2True, r2False int
	for b := range req2chan {
		tally(b, &r1True, &r1False)
	}
	for b := range req3chan {
		tally(b, &r2True, &r2False)
	}
	for c := range req4chan {
		assert.EqualValues(c2.Ref(), c.Ref())
	}

	assert.Equal(1, r1True)
	assert.Equal(0, r1False)
	assert.Equal(1, r2True)
	assert.Equal(0, r2False)

	go batch.Close()
	var r0True, r0False int
	for b := range req0chan {
		tally(b, &r0True, &r0False)
	}
	for c := range req1chan {
		assert.EqualValues(EmptyChunk.Ref(), c.Ref())
	}
	assert.Equal(0, r0True)
	assert.Equal(1, r0False)
}
