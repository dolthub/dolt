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
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/golang/snappy"
	"github.com/julienschmidt/httprouter"
)

const (
	httpChunkSinkConcurrency = 6
	writeBufferSize          = 1 << 12 // 4K
	readBufferSize           = 1 << 12 // 4K
)

var customHTTPTransport = http.Transport{
	// Since we limit ourselves to a maximum of httpChunkSinkConcurrency concurrent http requests, we think it's OK to up MaxIdleConnsPerHost so that one connection stays open for each concurrent request
	MaxIdleConnsPerHost: httpChunkSinkConcurrency,
	// This sets, essentially, an idle-timeout. The timer starts counting AFTER the client has finished sending the entire request to the server. As soon as the client receives the server's response headers, the timeout is canceled.
	ResponseHeaderTimeout: time.Duration(4) * time.Minute,
}

type httpChunkStore struct {
	host         *url.URL
	httpClient   httpDoer
	auth         string
	getQueue     chan chunks.ReadRequest
	hasQueue     chan chunks.ReadRequest
	finishedChan chan struct{}
	rateLimit    chan struct{}
	requestWg    *sync.WaitGroup
	workerWg     *sync.WaitGroup

	cacheMu       *sync.RWMutex
	unwrittenPuts *nbs.NomsBlockCache

	rootMu  *sync.RWMutex
	root    hash.Hash
	version string
}

func NewHTTPChunkStore(baseURL, auth string) chunks.ChunkStore {
	// Custom http.Client to give control of idle connections and timeouts
	return newHTTPChunkStoreWithClient(baseURL, auth, &http.Client{Transport: &customHTTPTransport})
}

func newHTTPChunkStoreWithClient(baseURL, auth string, client httpDoer) *httpChunkStore {
	u, err := url.Parse(baseURL)
	d.PanicIfError(err)
	if u.Scheme != "http" && u.Scheme != "https" {
		d.Panic("Unrecognized scheme: %s", u.Scheme)
	}
	hcs := &httpChunkStore{
		host:          u,
		httpClient:    client,
		auth:          auth,
		getQueue:      make(chan chunks.ReadRequest, readBufferSize),
		hasQueue:      make(chan chunks.ReadRequest, readBufferSize),
		finishedChan:  make(chan struct{}),
		rateLimit:     make(chan struct{}, httpChunkSinkConcurrency),
		requestWg:     &sync.WaitGroup{},
		workerWg:      &sync.WaitGroup{},
		cacheMu:       &sync.RWMutex{},
		unwrittenPuts: nbs.NewCache(),
		rootMu:        &sync.RWMutex{},
	}
	hcs.root, hcs.version = hcs.getRoot(false)
	hcs.batchGetRequests()
	hcs.batchHasRequests()
	return hcs
}

type httpDoer interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

func (hcs *httpChunkStore) Version() string {
	return hcs.version
}

func (hcs *httpChunkStore) Flush() {
	hcs.sendWriteRequests()
	hcs.requestWg.Wait()
	return
}

func (hcs *httpChunkStore) Close() (e error) {
	close(hcs.finishedChan)
	hcs.requestWg.Wait()
	hcs.workerWg.Wait()

	close(hcs.getQueue)
	close(hcs.hasQueue)
	close(hcs.rateLimit)

	hcs.cacheMu.Lock()
	defer hcs.cacheMu.Unlock()
	hcs.unwrittenPuts.Destroy()
	return
}

func (hcs *httpChunkStore) Stats() interface{} {
	return nil
}

func (hcs *httpChunkStore) Get(h hash.Hash) chunks.Chunk {
	checkCache := func(h hash.Hash) chunks.Chunk {
		hcs.cacheMu.RLock()
		defer hcs.cacheMu.RUnlock()
		return hcs.unwrittenPuts.Get(h)
	}
	if pending := checkCache(h); !pending.IsEmpty() {
		return pending
	}

	ch := make(chan *chunks.Chunk)
	hcs.requestWg.Add(1)
	hcs.getQueue <- chunks.NewGetRequest(h, ch)
	return *(<-ch)
}

func (hcs *httpChunkStore) GetMany(hashes hash.HashSet, foundChunks chan *chunks.Chunk) {
	cachedChunks := make(chan *chunks.Chunk)
	go func() {
		hcs.cacheMu.RLock()
		defer hcs.cacheMu.RUnlock()
		defer close(cachedChunks)
		hcs.unwrittenPuts.GetMany(hashes, cachedChunks)
	}()
	remaining := hash.HashSet{}
	for h := range hashes {
		remaining.Insert(h)
	}
	for c := range cachedChunks {
		remaining.Remove(c.Hash())
		foundChunks <- c
	}

	if len(remaining) == 0 {
		return
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(remaining))
	hcs.requestWg.Add(1)
	hcs.getQueue <- chunks.NewGetManyRequest(remaining, wg, foundChunks)
	wg.Wait()
}

func (hcs *httpChunkStore) batchGetRequests() {
	hcs.batchReadRequests(hcs.getQueue, hcs.getRefs)
}

func (hcs *httpChunkStore) Has(h hash.Hash) bool {
	checkCache := func(h hash.Hash) bool {
		hcs.cacheMu.RLock()
		defer hcs.cacheMu.RUnlock()
		return hcs.unwrittenPuts.Has(h)
	}
	if checkCache(h) {
		return true
	}

	ch := make(chan bool)
	hcs.requestWg.Add(1)
	hcs.hasQueue <- chunks.NewAbsentRequest(h, ch)
	return <-ch
}

func (hcs *httpChunkStore) HasMany(hashes hash.HashSet) (absent hash.HashSet) {
	var remaining hash.HashSet
	func() {
		hcs.cacheMu.RLock()
		defer hcs.cacheMu.RUnlock()
		remaining = hcs.unwrittenPuts.HasMany(hashes)
	}()
	if len(remaining) == 0 {
		return remaining
	}

	foundChunks := make(chan hash.Hash)
	wg := &sync.WaitGroup{}
	wg.Add(len(remaining))
	hcs.requestWg.Add(1)
	hcs.hasQueue <- chunks.NewAbsentManyRequest(remaining, wg, foundChunks)
	go func() { defer close(foundChunks); wg.Wait() }()

	absent = hash.HashSet{}
	for found := range foundChunks {
		absent.Insert(found)
	}
	return absent
}

func (hcs *httpChunkStore) batchHasRequests() {
	hcs.batchReadRequests(hcs.hasQueue, hcs.hasRefs)
}

type batchGetter func(hashes hash.HashSet, batch chunks.ReadBatch)

func (hcs *httpChunkStore) batchReadRequests(queue <-chan chunks.ReadRequest, getter batchGetter) {
	hcs.workerWg.Add(1)
	go func() {
		defer hcs.workerWg.Done()

		for done := false; !done; {
			select {
			case req := <-queue:
				hcs.sendReadRequests(req, queue, getter)
			case <-hcs.finishedChan:
				done = true
			}
			// Drain queue before returning
			select {
			case req := <-queue:
				hcs.sendReadRequests(req, queue, getter)
			default:
				//drained!
			}
		}
	}()
}

func (hcs *httpChunkStore) sendReadRequests(req chunks.ReadRequest, queue <-chan chunks.ReadRequest, getter batchGetter) {
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

	hcs.rateLimit <- struct{}{}
	go func() {
		defer func() {
			hcs.requestWg.Add(-count)
			batch.Close()
		}()

		getter(hashes, batch)
		<-hcs.rateLimit
	}()
}

func (hcs *httpChunkStore) getRefs(hashes hash.HashSet, batch chunks.ReadBatch) {
	// POST http://<host>/getRefs/. Post body: ref=hash0&ref=hash1& Response will be chunk data if present, 404 if absent.
	u := *hcs.host
	u.Path = httprouter.CleanPath(hcs.host.Path + constants.GetRefsPath)

	req := newRequest("POST", hcs.auth, u.String(), buildHashesRequest(hashes), http.Header{
		"Accept-Encoding": {"x-snappy-framed"},
		"Content-Type":    {"application/x-www-form-urlencoded"},
	})

	res, err := hcs.httpClient.Do(req)
	d.Chk.NoError(err)
	expectVersion(hcs.version, res)
	reader := resBodyReader(res)
	defer closeResponse(reader)

	if http.StatusOK != res.StatusCode {
		d.Panic("Unexpected response: %s", http.StatusText(res.StatusCode))
	}

	chunkChan := make(chan *chunks.Chunk, 16)
	go func() { defer close(chunkChan); chunks.Deserialize(reader, chunkChan) }()

	for c := range chunkChan {
		h := c.Hash()
		for _, or := range batch[h] {
			go or.Satisfy(h, c)
		}
		delete(batch, c.Hash())
	}
}

func (hcs *httpChunkStore) hasRefs(hashes hash.HashSet, batch chunks.ReadBatch) {
	// POST http://<host>/hasRefs/. Post body: ref=sha1---&ref=sha1---& Response will be text of lines containing "|ref| |bool|".
	u := *hcs.host
	u.Path = httprouter.CleanPath(hcs.host.Path + constants.HasRefsPath)

	req := newRequest("POST", hcs.auth, u.String(), buildHashesRequest(hashes), http.Header{
		"Accept-Encoding": {"x-snappy-framed"},
		"Content-Type":    {"application/x-www-form-urlencoded"},
	})

	res, err := hcs.httpClient.Do(req)
	d.Chk.NoError(err)
	expectVersion(hcs.version, res)
	reader := resBodyReader(res)
	defer closeResponse(reader)

	if http.StatusOK != res.StatusCode {
		d.Panic("Unexpected response: %s", http.StatusText(res.StatusCode))
	}

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		h := hash.Parse(scanner.Text())
		for _, outstanding := range batch[h] {
			outstanding.Satisfy(h, &chunks.EmptyChunk)
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

func (hcs *httpChunkStore) Put(c chunks.Chunk) {
	hcs.cacheMu.RLock()
	defer hcs.cacheMu.RUnlock()
	hcs.unwrittenPuts.Insert(c)
}

func (hcs *httpChunkStore) sendWriteRequests() {
	hcs.rateLimit <- struct{}{}
	defer func() { <-hcs.rateLimit }()

	hcs.cacheMu.Lock()
	defer func() {
		hcs.cacheMu.Unlock()
	}()

	count := hcs.unwrittenPuts.Count()
	if count == 0 {
		return
	}
	defer func() {
		hcs.unwrittenPuts.Destroy()
		hcs.unwrittenPuts = nbs.NewCache()
	}()

	verbose.Log("Sending %d chunks", count)
	chunkChan := make(chan *chunks.Chunk, 1024)
	go func() {
		hcs.unwrittenPuts.ExtractChunks(chunkChan)
		close(chunkChan)
	}()

	body := buildWriteValueRequest(chunkChan)
	url := *hcs.host
	url.Path = httprouter.CleanPath(hcs.host.Path + constants.WriteValuePath)
	// TODO: Make this accept snappy encoding
	req := newRequest("POST", hcs.auth, url.String(), body, http.Header{
		"Accept-Encoding":  {"gzip"},
		"Content-Encoding": {"x-snappy-framed"},
		"Content-Type":     {"application/octet-stream"},
	})

	res, err := hcs.httpClient.Do(req)
	d.PanicIfError(err)
	expectVersion(hcs.version, res)
	defer closeResponse(res.Body)

	if http.StatusCreated != res.StatusCode {
		d.Panic("Unexpected response: %s", formatErrorResponse(res))
	}
	verbose.Log("Finished sending %d hashes", count)
}

func (hcs *httpChunkStore) Root() hash.Hash {
	hcs.rootMu.RLock()
	defer hcs.rootMu.RUnlock()
	return hcs.root
}

func (hcs *httpChunkStore) Rebase() {
	root, _ := hcs.getRoot(true)
	hcs.rootMu.Lock()
	defer hcs.rootMu.Unlock()
	hcs.root = root
}

func (hcs *httpChunkStore) getRoot(checkVers bool) (root hash.Hash, vers string) {
	// GET http://<host>/root. Response will be ref of root.
	res := hcs.requestRoot("GET", hash.Hash{}, hash.Hash{})
	if checkVers {
		expectVersion(hcs.version, res)
	}
	defer closeResponse(res.Body)

	if http.StatusOK != res.StatusCode {
		d.Panic("Unexpected response: %s", http.StatusText(res.StatusCode))
	}
	data, err := ioutil.ReadAll(res.Body)
	d.PanicIfError(err)

	return hash.Parse(string(data)), res.Header.Get(NomsVersionHeader)
}

func (hcs *httpChunkStore) Commit(current, last hash.Hash) bool {
	hcs.rootMu.Lock()
	defer hcs.rootMu.Unlock()
	hcs.Flush()

	// POST http://<host>/root?current=<ref>&last=<ref>. Response will be 200 on success, 409 if current is outdated.
	res := hcs.requestRoot("POST", current, last)
	expectVersion(hcs.version, res)
	defer closeResponse(res.Body)

	switch res.StatusCode {
	case http.StatusOK:
		hcs.root = current
		return true
	case http.StatusConflict:
		data, err := ioutil.ReadAll(res.Body)
		d.PanicIfError(err)
		hcs.root = hash.Parse(string(data))
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

func (hcs *httpChunkStore) requestRoot(method string, current, last hash.Hash) *http.Response {
	u := *hcs.host
	u.Path = httprouter.CleanPath(hcs.host.Path + constants.RootPath)
	if method == "POST" {
		params := u.Query()
		params.Add("last", last.String())
		params.Add("current", current.String())
		u.RawQuery = params.Encode()
	}

	req := newRequest(method, hcs.auth, u.String(), nil, nil)

	res, err := hcs.httpClient.Do(req)
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

func expectVersion(expected string, res *http.Response) {
	dataVersion := res.Header.Get(NomsVersionHeader)
	if expected != dataVersion {
		b, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		d.Panic(
			"Version skew\n\r"+
				"\tServer data version changed from '%s' to '%s'\n\r"+
				"\tHTTP Response: %d (%s): %s\n",
			expected, dataVersion,
			res.StatusCode, res.Status, string(b))
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
