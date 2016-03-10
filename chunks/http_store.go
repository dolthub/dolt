package chunks

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/julienschmidt/httprouter"
)

const (
	readBufferSize  = 1 << 12 // 4k
	hasBufferSize   = 1 << 12 // 4k
	writeBufferSize = 1 << 12 // 4k
	requestLimit    = 6       // max number of active http requests
)

// Use a custom http client rather than http.DefaultClient. We limit ourselves to a maximum of |requestLimit| concurrent http requests, the custom httpClient ups the maxIdleConnsPerHost value so that one connection stays open for each concurrent request.
func makeHTTPClient() *http.Client {
	t := http.Transport(*http.DefaultTransport.(*http.Transport))
	t.MaxIdleConnsPerHost = requestLimit

	return &http.Client{
		Transport: &t,
		Timeout:   time.Duration(30) * time.Second,
	}
}

type HTTPStore struct {
	host          *url.URL
	httpClient    *http.Client
	auth          string
	getQueue      chan getRequest
	hasQueue      chan hasRequest
	writeQueue    chan Chunk
	finishedChan  chan struct{}
	requestWg     *sync.WaitGroup
	workerWg      *sync.WaitGroup
	unwrittenPuts *unwrittenPutCache
}

func NewHTTPStore(baseURL, auth string) *HTTPStore {
	u, err := url.Parse(baseURL)
	d.Exp.NoError(err)
	d.Exp.True(u.Scheme == "http" || u.Scheme == "https")

	client := &HTTPStore{
		host:          u,
		httpClient:    makeHTTPClient(),
		auth:          auth,
		getQueue:      make(chan getRequest, readBufferSize),
		hasQueue:      make(chan hasRequest, hasBufferSize),
		writeQueue:    make(chan Chunk, writeBufferSize),
		finishedChan:  make(chan struct{}),
		requestWg:     &sync.WaitGroup{},
		workerWg:      &sync.WaitGroup{},
		unwrittenPuts: newUnwrittenPutCache(),
	}

	for i := 0; i < requestLimit; i++ {
		go client.batchRequests()
	}

	return client
}

func (h *HTTPStore) Host() *url.URL {
	return &url.URL{Host: h.host.Host, Scheme: h.host.Scheme}
}

func (h *HTTPStore) Get(r ref.Ref) Chunk {
	pending := h.unwrittenPuts.Get(r)
	if !pending.IsEmpty() {
		return pending
	}

	ch := make(chan Chunk)
	h.requestWg.Add(1)
	h.getQueue <- getRequest{r, ch}
	return <-ch
}

func (h *HTTPStore) sendReadRequests(req getRequest) {
	batch := getBatch{}
	refs := map[ref.Ref]bool{}

	addReq := func(req getRequest) {
		batch[req.r] = append(batch[req.r], req.ch)
		refs[req.r] = true
		h.requestWg.Done()
	}

	addReq(req)
	for done := false; !done; {
		select {
		case req := <-h.getQueue:
			addReq(req)
		default:
			done = true
		}
	}
	h.getRefs(refs, &batch)
	batch.Close()
}

func (h *HTTPStore) Has(ref ref.Ref) bool {
	pending := h.unwrittenPuts.Get(ref)
	if !pending.IsEmpty() {
		return true
	}

	ch := make(chan bool)
	h.requestWg.Add(1)
	h.hasQueue <- hasRequest{ref, ch}
	return <-ch
}

func (h *HTTPStore) sendHasRequests(req hasRequest) {
	batch := hasBatch{}
	refs := map[ref.Ref]bool{}

	addReq := func(req hasRequest) {
		batch[req.r] = append(batch[req.r], req.ch)
		refs[req.r] = true
		h.requestWg.Done()
	}

	addReq(req)
	for done := false; !done; {
		select {
		case req := <-h.hasQueue:
			addReq(req)
		default:
			done = true
		}
	}
	h.getHasRefs(refs, batch)
}

func (h *HTTPStore) Put(chunk Chunk) {
	if !h.unwrittenPuts.Add(chunk) {
		return
	}

	h.requestWg.Add(1)
	h.writeQueue <- chunk
}

func (h *HTTPStore) sendWriteRequests(chunk Chunk) {
	chunks := []Chunk{}

	chunks = append(chunks, chunk)
	for done := false; !done; {
		select {
		case chunk := <-h.writeQueue:
			chunks = append(chunks, chunk)
		default:
			done = true
		}
	}

	h.requestWg.Add(-len(chunks))
	h.postRefs(chunks)
}

func (h *HTTPStore) batchRequests() {
	h.workerWg.Add(1)
	defer h.workerWg.Done()

	for done := false; !done; {
		select {
		case req := <-h.hasQueue:
			h.sendHasRequests(req)
		case req := <-h.getQueue:
			h.sendReadRequests(req)
		case chunk := <-h.writeQueue:
			h.sendWriteRequests(chunk)
		case <-h.finishedChan:
			done = true
		}
	}
}

func (h *HTTPStore) newRequest(method, url string, body io.Reader, header http.Header) *http.Request {
	req, err := http.NewRequest(method, url, body)
	d.Chk.NoError(err)
	for k, vals := range header {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	if h.auth != "" {
		req.Header.Set("Authorization", h.auth)
	}
	return req
}

func (h *HTTPStore) postRefs(chs []Chunk) {
	body := &bytes.Buffer{}
	gw := gzip.NewWriter(body)
	sz := NewSerializer(gw)
	for _, chunk := range chs {
		sz.Put(chunk)
	}
	sz.Close()
	gw.Close()

	url := *h.host
	url.Path = httprouter.CleanPath(h.host.Path + constants.PostRefsPath)
	req := h.newRequest("POST", url.String(), body, http.Header{
		"Content-Encoding": {"gzip"},
		"Content-Type":     {"application/octet-stream"},
	})

	res, err := h.httpClient.Do(req)
	d.Chk.NoError(err)

	d.Chk.Equal(res.StatusCode, http.StatusCreated, "Unexpected response: %s", http.StatusText(res.StatusCode))
	closeResponse(res)
	h.unwrittenPuts.Clear(chs)
}

func (h *HTTPStore) requestRef(r ref.Ref, method string, body io.Reader) *http.Response {
	url := *h.host
	url.Path = httprouter.CleanPath(h.host.Path + constants.RefPath)
	if !r.IsEmpty() {
		url.Path = path.Join(url.Path, r.String())
	}

	header := http.Header{}
	if body != nil {
		header.Set("Content-Type", "application/octet-stream")
	}
	req := h.newRequest(method, url.String(), body, header)

	res, err := h.httpClient.Do(req)
	d.Chk.NoError(err)
	return res
}

func (h *HTTPStore) getHasRefs(refs map[ref.Ref]bool, reqs hasBatch) {
	// POST http://<host>/getHasRefs/. Post body: ref=sha1---&ref=sha1---& Response will be text of lines containing "|ref| |bool|"
	u := *h.host
	u.Path = httprouter.CleanPath(h.host.Path + constants.GetHasPath)
	values := &url.Values{}
	for r, _ := range refs {
		values.Add("ref", r.String())
	}

	req := h.newRequest("POST", u.String(), strings.NewReader(values.Encode()), http.Header{
		"Accept-Encoding": {"gzip"},
		"Content-Type":    {"application/x-www-form-urlencoded"},
	})

	res, err := h.httpClient.Do(req)
	d.Chk.NoError(err)
	defer closeResponse(res)
	d.Chk.Equal(http.StatusOK, res.StatusCode, "Unexpected response: %s", http.StatusText(res.StatusCode))

	reader := res.Body
	if strings.Contains(res.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(reader)
		d.Chk.NoError(err)
		defer gr.Close()
		reader = gr
	}

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		r := ref.Parse(scanner.Text())
		scanner.Scan()
		has := scanner.Text() == "true"
		for _, ch := range reqs[r] {
			ch <- has
		}
	}
}

func (h *HTTPStore) getRefs(refs map[ref.Ref]bool, cs ChunkSink) {
	// POST http://<host>/getRefs/. Post body: ref=sha1---&ref=sha1---& Response will be chunk data if present, 404 if absent.
	u := *h.host
	u.Path = httprouter.CleanPath(h.host.Path + constants.GetRefsPath)
	values := &url.Values{}
	for r, _ := range refs {
		values.Add("ref", r.String())
	}

	req := h.newRequest("POST", u.String(), strings.NewReader(values.Encode()), http.Header{
		"Accept-Encoding": {"gzip"},
		"Content-Type":    {"application/x-www-form-urlencoded"},
	})

	res, err := h.httpClient.Do(req)
	d.Chk.NoError(err)
	defer closeResponse(res)
	d.Chk.Equal(http.StatusOK, res.StatusCode, "Unexpected response: %s", http.StatusText(res.StatusCode))

	reader := res.Body
	if strings.Contains(res.Header.Get("Content-Encoding"), "gzip") {
		gr, err := gzip.NewReader(reader)
		d.Chk.NoError(err)
		defer gr.Close()
		reader = gr
	}

	rl := make(chan struct{}, 1) // Rate limit to 1 because there are already N goroutines waiting on responses, all we need to do is send the Chunks back through their channels.
	Deserialize(reader, cs, rl)
}

func (h *HTTPStore) Root() ref.Ref {
	// GET http://<host>/root. Response will be ref of root.
	res := h.requestRoot("GET", ref.Ref{}, ref.Ref{})
	defer closeResponse(res)

	d.Chk.Equal(http.StatusOK, res.StatusCode, "Unexpected response: %s", http.StatusText(res.StatusCode))
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	return ref.Parse(string(data))
}

func (h *HTTPStore) UpdateRoot(current, last ref.Ref) bool {
	// POST http://<host>root?current=<ref>&last=<ref>. Response will be 200 on success, 409 if current is outdated.
	h.requestWg.Wait()

	res := h.requestRoot("POST", current, last)
	defer closeResponse(res)

	d.Chk.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusConflict, "Unexpected response: %s", http.StatusText(res.StatusCode))
	return res.StatusCode == http.StatusOK
}

func (h *HTTPStore) requestRoot(method string, current, last ref.Ref) *http.Response {
	u := *h.host
	u.Path = httprouter.CleanPath(h.host.Path + constants.RootPath)
	if method == "POST" {
		d.Exp.False(current.IsEmpty())
		params := url.Values{}
		params.Add("last", last.String())
		params.Add("current", current.String())
		u.RawQuery = params.Encode()
	}

	req := h.newRequest(method, u.String(), nil, nil)

	res, err := h.httpClient.Do(req)
	d.Chk.NoError(err)

	return res
}

func (h *HTTPStore) Close() error {
	h.requestWg.Wait()

	close(h.finishedChan)
	h.workerWg.Wait()

	close(h.hasQueue)
	close(h.getQueue)
	close(h.writeQueue)
	return nil
}

// In order for keep alive to work we must read to EOF on every response. We may want to add a timeout so that a server that left its connection open can't cause all of ports to be eaten up.
func closeResponse(res *http.Response) error {
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	d.Chk.Equal(0, len(data), string(data))
	return res.Body.Close()
}

type HTTPStoreFlags struct {
	host *string
	auth *string
}

func HTTPFlags(prefix string) HTTPStoreFlags {
	return HTTPStoreFlags{
		flag.String(prefix+"h", "", "http host to connect to"),
		flag.String(prefix+"h-auth", "", "\"Authorization\" http header"),
	}
}

func (h HTTPStoreFlags) CreateStore(ns string) ChunkStore {
	if h.check() {
		return NewHTTPStore(*h.host+httprouter.CleanPath(ns), *h.auth)
	}
	return nil
}

func (h HTTPStoreFlags) Shutter() {}

func (h HTTPStoreFlags) CreateFactory() Factory {
	if h.check() {
		return h
	}
	return nil
}

func (h HTTPStoreFlags) check() bool {
	return *h.host != ""
}
