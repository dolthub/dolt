// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/nbs"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/golang/snappy"
	"github.com/julienschmidt/httprouter"
)

const (
	httpChunkSinkConcurrency = 6
	writeBufferSize          = 1 << 12 // 4K
	readBufferSize           = 1 << 12 // 4K
)

// httpBatchStore implements types.BatchStore
type httpBatchStore struct {
	host         *url.URL
	httpClient   httpDoer
	auth         string
	getQueue     chan chunks.ReadRequest
	hasQueue     chan chunks.ReadRequest
	writeQueue   chan writeRequest
	finishedChan chan struct{}
	rateLimit    chan struct{}
	requestWg    *sync.WaitGroup
	workerWg     *sync.WaitGroup
	flushOrder   nbs.EnumerationOrder

	cacheMu       *sync.RWMutex
	unwrittenPuts *nbs.NomsBlockCache
	hints         types.Hints
}

func newHTTPBatchStore(baseURL, auth string) *httpBatchStore {
	u, err := url.Parse(baseURL)
	d.PanicIfError(err)
	if u.Scheme != "http" && u.Scheme != "https" {
		d.Panic("Unrecognized scheme: %s", u.Scheme)
	}
	buffSink := &httpBatchStore{
		host:          u,
		httpClient:    makeHTTPClient(httpChunkSinkConcurrency),
		auth:          auth,
		getQueue:      make(chan chunks.ReadRequest, readBufferSize),
		hasQueue:      make(chan chunks.ReadRequest, readBufferSize),
		writeQueue:    make(chan writeRequest, writeBufferSize),
		finishedChan:  make(chan struct{}),
		rateLimit:     make(chan struct{}, httpChunkSinkConcurrency),
		requestWg:     &sync.WaitGroup{},
		workerWg:      &sync.WaitGroup{},
		flushOrder:    nbs.InsertOrder,
		cacheMu:       &sync.RWMutex{},
		unwrittenPuts: nbs.NewCache(),
		hints:         types.Hints{},
	}
	buffSink.batchGetRequests()
	buffSink.batchHasRequests()
	return buffSink
}

type httpDoer interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

type writeRequest struct {
	hints     types.Hints
	justHints bool
}

// Use a custom http client rather than http.DefaultClient. We limit ourselves to a maximum of |requestLimit| concurrent http requests, the custom httpClient ups the maxIdleConnsPerHost value so that one connection stays open for each concurrent request.
func makeHTTPClient(requestLimit int) *http.Client {
	t := http.Transport(*http.DefaultTransport.(*http.Transport))
	t.MaxIdleConnsPerHost = requestLimit
	// This sets, essentially, an idle-timeout. The timer starts counting AFTER the client has finished sending the entire request to the server. As soon as the client receives the server's response headers, the timeout is canceled.
	t.ResponseHeaderTimeout = time.Duration(4) * time.Minute

	return &http.Client{Transport: &t}
}

func (bhcs *httpBatchStore) SetReverseFlushOrder() {
	bhcs.cacheMu.Lock()
	defer bhcs.cacheMu.Unlock()
	bhcs.flushOrder = nbs.ReverseOrder
}

func (bhcs *httpBatchStore) Flush() {
	bhcs.sendWriteRequests()
	bhcs.requestWg.Wait()
	return
}

func (bhcs *httpBatchStore) Close() (e error) {
	close(bhcs.finishedChan)
	bhcs.requestWg.Wait()
	bhcs.workerWg.Wait()

	close(bhcs.getQueue)
	close(bhcs.hasQueue)
	close(bhcs.writeQueue)
	close(bhcs.rateLimit)

	bhcs.cacheMu.Lock()
	defer bhcs.cacheMu.Unlock()
	bhcs.unwrittenPuts.Destroy()
	return
}

func (bhcs *httpBatchStore) Get(h hash.Hash) chunks.Chunk {
	checkCache := func(h hash.Hash) chunks.Chunk {
		bhcs.cacheMu.RLock()
		defer bhcs.cacheMu.RUnlock()
		return bhcs.unwrittenPuts.Get(h)
	}
	if pending := checkCache(h); !pending.IsEmpty() {
		return pending
	}

	ch := make(chan *chunks.Chunk)
	bhcs.requestWg.Add(1)
	bhcs.getQueue <- chunks.NewGetRequest(h, ch)
	return *(<-ch)
}

func (bhcs *httpBatchStore) GetMany(hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
	cachedChunks := make(chan *chunks.Chunk)
	go func() {
		bhcs.cacheMu.RLock()
		defer bhcs.cacheMu.RUnlock()
		defer close(cachedChunks)
		bhcs.unwrittenPuts.GetMany(hashes, cachedChunks)
	}()
	remaining := hash.HashSet{}
	for h := range hashes {
		remaining.Insert(h)
	}
	for c := range cachedChunks {
		remaining.Remove(c.Hash())
		foundChunks <- c
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(remaining))
	bhcs.requestWg.Add(1)
	bhcs.getQueue <- chunks.NewGetManyRequest(hashes, wg, foundChunks)
	wg.Wait()
}

func (bhcs *httpBatchStore) batchGetRequests() {
	bhcs.batchReadRequests(bhcs.getQueue, bhcs.getRefs)
}

func (bhcs *httpBatchStore) Has(h hash.Hash) bool {
	checkCache := func(h hash.Hash) bool {
		bhcs.cacheMu.RLock()
		defer bhcs.cacheMu.RUnlock()
		return bhcs.unwrittenPuts.Has(h)
	}
	if checkCache(h) {
		return true
	}

	ch := make(chan bool)
	bhcs.requestWg.Add(1)
	bhcs.hasQueue <- chunks.NewHasRequest(h, ch)
	return <-ch
}

func (bhcs *httpBatchStore) batchHasRequests() {
	bhcs.batchReadRequests(bhcs.hasQueue, bhcs.hasRefs)
}

type batchGetter func(hashes hash.HashSet, batch chunks.ReadBatch)

func (bhcs *httpBatchStore) batchReadRequests(queue <-chan chunks.ReadRequest, getter batchGetter) {
	bhcs.workerWg.Add(1)
	go func() {
		defer bhcs.workerWg.Done()

		for done := false; !done; {
			select {
			case req := <-queue:
				bhcs.sendReadRequests(req, queue, getter)
			case <-bhcs.finishedChan:
				done = true
			}
			// Drain queue before returning
			select {
			case req := <-queue:
				bhcs.sendReadRequests(req, queue, getter)
			default:
				//drained!
			}
		}
	}()
}

func (bhcs *httpBatchStore) sendReadRequests(req chunks.ReadRequest, queue <-chan chunks.ReadRequest, getter batchGetter) {
	batch := chunks.ReadBatch{}
	hashes := hash.HashSet{}

	count := 0
	addReq := func(req chunks.ReadRequest) {
		for h := range req.Hashes() {
			batch[h] = append(batch[h], req.Outstanding())
			hashes.Insert(h)
		}
		count++
	}

	addReq(req)
	for drained := false; !drained && len(hashes) < readBufferSize; {
		select {
		case req := <-queue:
			addReq(req)
		default:
			drained = true
		}
	}

	bhcs.rateLimit <- struct{}{}
	go func() {
		defer func() {
			bhcs.requestWg.Add(-count)
			batch.Close()
		}()

		getter(hashes, batch)
		<-bhcs.rateLimit
	}()
}

func (bhcs *httpBatchStore) getRefs(hashes hash.HashSet, batch chunks.ReadBatch) {
	// POST http://<host>/getRefs/. Post body: ref=hash0&ref=hash1& Response will be chunk data if present, 404 if absent.
	u := *bhcs.host
	u.Path = httprouter.CleanPath(bhcs.host.Path + constants.GetRefsPath)

	req := newRequest("POST", bhcs.auth, u.String(), buildHashesRequest(hashes), http.Header{
		"Accept-Encoding": {"x-snappy-framed"},
		"Content-Type":    {"application/x-www-form-urlencoded"},
	})

	res, err := bhcs.httpClient.Do(req)
	d.Chk.NoError(err)
	expectVersion(res)
	reader := resBodyReader(res)
	defer closeResponse(reader)

	if http.StatusOK != res.StatusCode {
		d.Panic("Unexpected response: %s", http.StatusText(res.StatusCode))
	}

	chunkChan := make(chan interface{}, 16)
	go func() { defer close(chunkChan); chunks.Deserialize(reader, chunkChan) }()

	for i := range chunkChan {
		cp := i.(*chunks.Chunk)
		for _, or := range batch[cp.Hash()] {
			go or.Satisfy(cp)
		}
		delete(batch, cp.Hash())
	}
}

func (bhcs *httpBatchStore) hasRefs(hashes hash.HashSet, batch chunks.ReadBatch) {
	// POST http://<host>/hasRefs/. Post body: ref=sha1---&ref=sha1---& Response will be text of lines containing "|ref| |bool|".
	u := *bhcs.host
	u.Path = httprouter.CleanPath(bhcs.host.Path + constants.HasRefsPath)

	req := newRequest("POST", bhcs.auth, u.String(), buildHashesRequest(hashes), http.Header{
		"Accept-Encoding": {"x-snappy-framed"},
		"Content-Type":    {"application/x-www-form-urlencoded"},
	})

	res, err := bhcs.httpClient.Do(req)
	d.Chk.NoError(err)
	expectVersion(res)
	reader := resBodyReader(res)
	defer closeResponse(reader)

	if http.StatusOK != res.StatusCode {
		d.Panic("Unexpected response: %s", http.StatusText(res.StatusCode))
	}

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		h := hash.Parse(scanner.Text())
		d.PanicIfFalse(scanner.Scan())
		if scanner.Text() == "true" {
			for _, outstanding := range batch[h] {
				// This is a little gross, but OutstandingHas.Satisfy() expects a chunk. It ignores it, though, and just sends 'true' over the channel it's holding.
				outstanding.Satisfy(&chunks.EmptyChunk)
			}
		} else {
			for _, outstanding := range batch[h] {
				outstanding.Fail()
			}
		}
		delete(batch, h)
	}
}

func resBodyReader(res *http.Response) (reader io.ReadCloser) {
	reader = res.Body
	if strings.Contains(res.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(reader)
		d.Chk.NoError(err)
		reader = gr
	} else if strings.Contains(res.Header.Get("Content-Encoding"), "x-snappy-framed") {
		sr := snappy.NewReader(reader)
		reader = ioutil.NopCloser(sr)
	}
	return
}

func (bhcs *httpBatchStore) SchedulePut(c chunks.Chunk, refHeight uint64, hints types.Hints) {
	bhcs.cacheMu.RLock()
	defer bhcs.cacheMu.RUnlock()
	bhcs.unwrittenPuts.Insert(c)
	for hint := range hints {
		bhcs.hints[hint] = struct{}{}
	}
}

func (bhcs *httpBatchStore) AddHints(hints types.Hints) {
	bhcs.cacheMu.RLock()
	defer bhcs.cacheMu.RUnlock()
	for hint := range hints {
		bhcs.hints[hint] = struct{}{}
	}
}

func (bhcs *httpBatchStore) sendWriteRequests() {
	bhcs.rateLimit <- struct{}{}
	defer func() { <-bhcs.rateLimit }()

	bhcs.cacheMu.Lock()
	defer func() {
		bhcs.flushOrder = nbs.InsertOrder // This needs to happen even if no chunks get written.
		bhcs.cacheMu.Unlock()
	}()

	count := bhcs.unwrittenPuts.Count()
	if count == 0 {
		return
	}
	defer func() {
		bhcs.unwrittenPuts.Destroy()
		bhcs.unwrittenPuts = nbs.NewCache()
		bhcs.hints = types.Hints{}
	}()

	var res *http.Response
	var err error
	for tryAgain := true; tryAgain; {
		verbose.Log("Sending %d chunks", count)
		chunkChan := make(chan *chunks.Chunk, 1024)
		go func() {
			bhcs.unwrittenPuts.ExtractChunks(bhcs.flushOrder, chunkChan)
			close(chunkChan)
		}()

		body := buildWriteValueRequest(chunkChan, bhcs.hints)
		url := *bhcs.host
		url.Path = httprouter.CleanPath(bhcs.host.Path + constants.WriteValuePath)
		// TODO: Make this accept snappy encoding
		req := newRequest("POST", bhcs.auth, url.String(), body, http.Header{
			"Accept-Encoding":  {"gzip"},
			"Content-Encoding": {"x-snappy-framed"},
			"Content-Type":     {"application/octet-stream"},
		})

		res, err = bhcs.httpClient.Do(req)
		d.PanicIfError(err)
		expectVersion(res)
		defer closeResponse(res.Body)

		if tryAgain = res.StatusCode == http.StatusTooManyRequests; tryAgain {
			reader := res.Body
			if strings.Contains(res.Header.Get("Content-Encoding"), "gzip") {
				gr, err := gzip.NewReader(reader)
				d.PanicIfError(err)
				defer gr.Close()
				reader = gr
			}
			/*hashes :=*/ deserializeHashes(reader)
			// TODO: BUG 1259 Since the client must currently send all chunks in one batch, the only thing to do in response to backpressure is send EVERYTHING again. Once batching is again possible, this code should figure out how to resend the chunks indicated by hashes.
			verbose.Log("Retrying...")
		}
	}

	if http.StatusCreated != res.StatusCode {
		d.Panic("Unexpected response: %s", formatErrorResponse(res))
	}
	verbose.Log("Finished sending %d hashes", count)
}

func (bhcs *httpBatchStore) Root() hash.Hash {
	// GET http://<host>/root. Response will be ref of root.
	res := bhcs.requestRoot("GET", hash.Hash{}, hash.Hash{})
	expectVersion(res)
	defer closeResponse(res.Body)

	if http.StatusOK != res.StatusCode {
		d.Panic("Unexpected response: %s", http.StatusText(res.StatusCode))
	}
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	return hash.Parse(string(data))
}

// UpdateRoot flushes outstanding writes to the backing ChunkStore before updating its Root, because it's almost certainly the case that the caller wants to point that root at some recently-Put Chunk.
func (bhcs *httpBatchStore) UpdateRoot(current, last hash.Hash) bool {
	// POST http://<host>/root?current=<ref>&last=<ref>. Response will be 200 on success, 409 if current is outdated.
	bhcs.Flush()

	res := bhcs.requestRoot("POST", current, last)
	expectVersion(res)
	defer closeResponse(res.Body)

	switch res.StatusCode {
	case http.StatusOK:
		return true
	case http.StatusConflict:
		return false
	default:
		buf := bytes.Buffer{}
		buf.ReadFrom(res.Body)
		body := buf.String()
		d.Chk.Fail(
			fmt.Sprintf("Unexpected response: %s: %s",
				http.StatusText(res.StatusCode),
				body))
		return false
	}
}

func (bhcs *httpBatchStore) requestRoot(method string, current, last hash.Hash) *http.Response {
	u := *bhcs.host
	u.Path = httprouter.CleanPath(bhcs.host.Path + constants.RootPath)
	if method == "POST" {
		if current.IsEmpty() {
			d.Panic("Unexpected empty value")
		}
		params := u.Query()
		params.Add("last", last.String())
		params.Add("current", current.String())
		u.RawQuery = params.Encode()
	}

	req := newRequest(method, bhcs.auth, u.String(), nil, nil)

	res, err := bhcs.httpClient.Do(req)
	d.PanicIfError(err)

	return res
}

func newRequest(method, auth, url string, body io.Reader, header http.Header) *http.Request {
	req, err := http.NewRequest(method, url, body)
	d.Chk.NoError(err)
	req.Header.Set(NomsVersionHeader, constants.NomsVersion)
	for k, vals := range header {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	return req
}

func formatErrorResponse(res *http.Response) string {
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	return fmt.Sprintf("%s:\n%s\n", res.Status, data)
}

func expectVersion(res *http.Response) {
	dataVersion := res.Header.Get(NomsVersionHeader)
	if constants.NomsVersion != dataVersion {
		b, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		d.PanicIfError(fmt.Errorf(
			"Version mismatch\n\r"+
				"\tSDK version '%s' is incompatible with data of version: '%s'\n\r"+
				"\tHTTP Response: %d (%s): %s\n",
			constants.NomsVersion, dataVersion,
			res.StatusCode, res.Status, string(b)))
	}
}

// In order for keep alive to work we must read to EOF on every response. We may want to add a timeout so that a server that left its connection open can't cause all of ports to be eaten up.
func closeResponse(rc io.ReadCloser) error {
	ioutil.ReadAll(rc)
	// Bug #2069. It's not clear what the behavior is here. These checks are currently not enabled because they are shadowing information about a failure which occurs earlier.
	// d.Chk.NoError(err)
	// d.PanicIfFalse(0 == len(data), string(data))
	return rc.Close()
}
