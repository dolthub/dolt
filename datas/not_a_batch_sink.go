package datas

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/url"
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/julienschmidt/httprouter"
)

// notABatchSink exists solely to provide a way to pull chunks into a remote data store without validation, because doing it with validation efficiently requires some serialization changes we have yet to make. Once we land BUG 822, we can come back here and undo this.
type notABatchSink struct {
	host          *url.URL
	httpClient    httpDoer
	auth          string
	writeQueue    chan chunks.Chunk
	flushChan     chan struct{}
	finishedChan  chan struct{}
	rateLimit     chan struct{}
	requestWg     *sync.WaitGroup
	workerWg      *sync.WaitGroup
	unwrittenPuts *unwrittenPutCache
}

func newNotABatchSink(host *url.URL, auth string) *notABatchSink {
	sink := &notABatchSink{
		host:          host,
		httpClient:    makeHTTPClient(httpChunkSinkConcurrency),
		auth:          auth,
		writeQueue:    make(chan chunks.Chunk, writeBufferSize),
		flushChan:     make(chan struct{}),
		finishedChan:  make(chan struct{}),
		rateLimit:     make(chan struct{}, httpChunkSinkConcurrency),
		requestWg:     &sync.WaitGroup{},
		workerWg:      &sync.WaitGroup{},
		unwrittenPuts: newUnwrittenPutCache(),
	}
	sink.batchPutRequests()
	return sink
}

func (bhcs *notABatchSink) Flush() {
	bhcs.flushChan <- struct{}{}
	bhcs.requestWg.Wait()
	return
}

func (bhcs *notABatchSink) Close() (e error) {
	close(bhcs.finishedChan)
	bhcs.requestWg.Wait()
	bhcs.workerWg.Wait()

	close(bhcs.flushChan)
	close(bhcs.writeQueue)
	close(bhcs.rateLimit)
	return
}

func (bhcs *notABatchSink) SchedulePut(c chunks.Chunk, hints types.Hints) {
	if !bhcs.unwrittenPuts.Add(c) {
		return
	}

	bhcs.requestWg.Add(1)
	bhcs.writeQueue <- c
}

func (bhcs *notABatchSink) batchPutRequests() {
	bhcs.workerWg.Add(1)
	go func() {
		defer bhcs.workerWg.Done()

		var chunks []chunks.Chunk
		sendAndReset := func() {
			bhcs.sendWriteRequests(chunks) // Takes ownership of chunks
			chunks = nil
		}

		for done := false; !done; {
			drainAndSend := false
			select {
			case c := <-bhcs.writeQueue:
				chunks = append(chunks, c)
				if len(chunks) == writeBufferSize {
					sendAndReset()
				}
			case <-bhcs.flushChan:
				drainAndSend = true
			case <-bhcs.finishedChan:
				drainAndSend = true
				done = true
			}

			if drainAndSend {
				for drained := false; !drained; {
					select {
					case c := <-bhcs.writeQueue:
						chunks = append(chunks, c)
					default:
						drained = true
					}
					if len(chunks) == writeBufferSize || (drained && chunks != nil) {
						sendAndReset()
					}
				}
			}
		}
		d.Chk.Nil(chunks, "%d chunks were never sent to server", len(chunks))
	}()
}

func (bhcs *notABatchSink) sendWriteRequests(chnx []chunks.Chunk) {
	bhcs.rateLimit <- struct{}{}
	go func() {
		defer func() {
			bhcs.unwrittenPuts.Clear(chnx)
			bhcs.requestWg.Add(-len(chnx))
		}()

		body := &bytes.Buffer{}
		gw := gzip.NewWriter(body)
		sz := chunks.NewSerializer(gw)
		for _, chunk := range chnx {
			sz.Put(chunk)
		}
		sz.Close()
		gw.Close()

		url := *bhcs.host
		url.Path = httprouter.CleanPath(bhcs.host.Path + constants.PostRefsPath)
		req := newRequest("POST", bhcs.auth, url.String(), body, http.Header{
			"Content-Encoding": {"gzip"},
			"Content-Type":     {"application/octet-stream"},
		})

		res, err := bhcs.httpClient.Do(req)
		d.Chk.NoError(err)

		d.Chk.Equal(res.StatusCode, http.StatusCreated, "Unexpected response: %s", http.StatusText(res.StatusCode))
		closeResponse(res)
		<-bhcs.rateLimit
	}()
}

func (bhcs *notABatchSink) Root() ref.Ref {
	panic("Not Reached")
}

func (bhcs *notABatchSink) UpdateRoot(current, last ref.Ref) bool {
	panic("Not Reached")
}
