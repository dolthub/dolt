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

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/julienschmidt/httprouter"
	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
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
	host            *url.URL
	httpClient      *http.Client
	getQueue        chan getRequest
	hasQueue        chan hasRequest
	writeQueue      chan Chunk
	finishedChan    chan struct{}
	wg              *sync.WaitGroup
	wgFinished      *sync.WaitGroup
	unwrittenPuts   map[ref.Ref]Chunk
	unwrittenPutsMu *sync.Mutex
}

func NewHTTPStore(baseURL string) *HTTPStore {
	u, err := url.Parse(baseURL)
	d.Exp.NoError(err)
	d.Exp.True(u.Scheme == "http" || u.Scheme == "https")

	client := &HTTPStore{
		host:            u,
		httpClient:      makeHTTPClient(),
		getQueue:        make(chan getRequest, readBufferSize),
		hasQueue:        make(chan hasRequest, hasBufferSize),
		writeQueue:      make(chan Chunk, writeBufferSize),
		finishedChan:    make(chan struct{}),
		wg:              &sync.WaitGroup{},
		wgFinished:      &sync.WaitGroup{},
		unwrittenPuts:   map[ref.Ref]Chunk{},
		unwrittenPutsMu: &sync.Mutex{},
	}

	for i := 0; i < requestLimit; i++ {
		go client.batchRequests()
	}

	return client
}

func (c *HTTPStore) Host() *url.URL {
	return &url.URL{Host: c.host.Host, Scheme: c.host.Scheme}
}

func (c *HTTPStore) addUnwrittenPut(chunk Chunk) bool {
	c.unwrittenPutsMu.Lock()
	defer c.unwrittenPutsMu.Unlock()
	if _, ok := c.unwrittenPuts[chunk.Ref()]; !ok {
		c.unwrittenPuts[chunk.Ref()] = chunk
		return true
	}

	return false
}

func (c *HTTPStore) getUnwrittenPut(r ref.Ref) Chunk {
	c.unwrittenPutsMu.Lock()
	defer c.unwrittenPutsMu.Unlock()
	if c, ok := c.unwrittenPuts[r]; ok {
		return c
	}
	return EmptyChunk
}

func (c *HTTPStore) clearUnwrittenPuts(chunks []Chunk) {
	c.unwrittenPutsMu.Lock()
	defer c.unwrittenPutsMu.Unlock()
	for _, chunk := range chunks {
		delete(c.unwrittenPuts, chunk.Ref())
	}
}

func (c *HTTPStore) Get(r ref.Ref) Chunk {
	pending := c.getUnwrittenPut(r)
	if !pending.IsEmpty() {
		return pending
	}

	ch := make(chan Chunk)
	c.wg.Add(1)
	c.getQueue <- getRequest{r, ch}
	return <-ch
}

func (c *HTTPStore) sendReadRequests(req getRequest) {
	batch := getBatch{}
	refs := map[ref.Ref]bool{}

	addReq := func(req getRequest) {
		batch[req.r] = append(batch[req.r], req.ch)
		refs[req.r] = true
		c.wg.Done()
	}

	addReq(req)
	for done := false; !done; {
		select {
		case req := <-c.getQueue:
			addReq(req)
		default:
			done = true
		}
	}
	c.getRefs(refs, &batch)
	batch.Close()
}

func (c *HTTPStore) Has(ref ref.Ref) bool {
	pending := c.getUnwrittenPut(ref)
	if !pending.IsEmpty() {
		return true
	}

	ch := make(chan bool)
	c.wg.Add(1)
	c.hasQueue <- hasRequest{ref, ch}
	return <-ch
}

func (c *HTTPStore) sendHasRequests(req hasRequest) {
	batch := hasBatch{}
	refs := map[ref.Ref]bool{}

	addReq := func(req hasRequest) {
		batch[req.r] = append(batch[req.r], req.ch)
		refs[req.r] = true
		c.wg.Done()
	}

	addReq(req)
	for done := false; !done; {
		select {
		case req := <-c.hasQueue:
			addReq(req)
		default:
			done = true
		}
	}
	c.getHasRefs(refs, batch)
}

func (c *HTTPStore) Put(chunk Chunk) {
	if !c.addUnwrittenPut(chunk) {
		return
	}

	c.wg.Add(1)
	c.writeQueue <- chunk
}

func (c *HTTPStore) sendWriteRequests(chunk Chunk) {
	chunks := []Chunk{}

	chunks = append(chunks, chunk)
	for done := false; !done; {
		select {
		case chunk := <-c.writeQueue:
			chunks = append(chunks, chunk)
		default:
			done = true
		}
	}

	c.wg.Add(-len(chunks))
	c.postRefs(chunks)
}

func (c *HTTPStore) batchRequests() {
	c.wgFinished.Add(1)
	defer c.wgFinished.Done()

	for done := false; !done; {
		select {
		case req := <-c.hasQueue:
			c.sendHasRequests(req)
		case req := <-c.getQueue:
			c.sendReadRequests(req)
		case chunk := <-c.writeQueue:
			c.sendWriteRequests(chunk)
		case <-c.finishedChan:
			done = true
		}
	}
}

func (c *HTTPStore) postRefs(chs []Chunk) {
	body := &bytes.Buffer{}
	gw := gzip.NewWriter(body)
	sz := NewSerializer(gw)
	for _, chunk := range chs {
		sz.Put(chunk)
	}
	sz.Close()
	gw.Close()

	url := *c.host
	url.Path = httprouter.CleanPath(c.host.Path + constants.PostRefsPath)
	req, err := http.NewRequest("POST", url.String(), body)
	d.Chk.NoError(err)

	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/octet-stream")

	res, err := c.httpClient.Do(req)
	d.Chk.NoError(err)

	d.Chk.Equal(res.StatusCode, http.StatusCreated, "Unexpected response: %s", http.StatusText(res.StatusCode))
	closeResponse(res)
	c.clearUnwrittenPuts(chs)
}

func (c *HTTPStore) requestRef(r ref.Ref, method string, body io.Reader) *http.Response {
	url := *c.host
	url.Path = httprouter.CleanPath(c.host.Path + constants.RefPath)
	if !r.IsEmpty() {
		url.Path = path.Join(url.Path, r.String())
	}

	req, err := http.NewRequest(method, url.String(), body)
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	d.Chk.NoError(err)

	res, err := c.httpClient.Do(req)
	d.Chk.NoError(err)
	return res
}

func (c *HTTPStore) getHasRefs(refs map[ref.Ref]bool, reqs hasBatch) {
	// POST http://<host>/getHasRefs/. Post body: ref=sha1---&ref=sha1---& Response will be text of lines containing "|ref| |bool|"
	u := *c.host
	u.Path = httprouter.CleanPath(c.host.Path + constants.GetHasPath)
	values := &url.Values{}
	for r, _ := range refs {
		values.Add("ref", r.String())
	}

	req, err := http.NewRequest("POST", u.String(), strings.NewReader(values.Encode()))
	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	d.Chk.NoError(err)

	res, err := c.httpClient.Do(req)
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

func (c *HTTPStore) getRefs(refs map[ref.Ref]bool, cs ChunkSink) {
	// POST http://<host>/getRefs/. Post body: ref=sha1---&ref=sha1---& Response will be chunk data if present, 404 if absent.
	u := *c.host
	u.Path = httprouter.CleanPath(c.host.Path + constants.GetRefsPath)
	values := &url.Values{}
	for r, _ := range refs {
		values.Add("ref", r.String())
	}

	req, err := http.NewRequest("POST", u.String(), strings.NewReader(values.Encode()))
	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	d.Chk.NoError(err)

	res, err := c.httpClient.Do(req)
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

func (c *HTTPStore) Root() ref.Ref {
	// GET http://<host>/root. Response will be ref of root.
	res := c.requestRoot("GET", ref.Ref{}, ref.Ref{})
	defer closeResponse(res)

	d.Chk.Equal(http.StatusOK, res.StatusCode, "Unexpected response: %s", http.StatusText(res.StatusCode))
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	return ref.Parse(string(data))
}

func (c *HTTPStore) UpdateRoot(current, last ref.Ref) bool {
	// POST http://<host>root?current=<ref>&last=<ref>. Response will be 200 on success, 409 if current is outdated.
	c.wg.Wait()

	res := c.requestRoot("POST", current, last)
	defer closeResponse(res)

	d.Chk.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusConflict, "Unexpected response: %s", http.StatusText(res.StatusCode))
	return res.StatusCode == http.StatusOK
}

func (c *HTTPStore) requestRoot(method string, current, last ref.Ref) *http.Response {
	u := *c.host
	u.Path = httprouter.CleanPath(c.host.Path + constants.RootPath)
	if method == "POST" {
		d.Exp.False(current.IsEmpty())
		params := url.Values{}
		params.Add("last", last.String())
		params.Add("current", current.String())
		u.RawQuery = params.Encode()
	}

	req, err := http.NewRequest(method, u.String(), nil)
	d.Chk.NoError(err)

	res, err := c.httpClient.Do(req)
	d.Chk.NoError(err)

	return res
}

func (c *HTTPStore) Close() error {
	c.wg.Wait()

	close(c.finishedChan)
	c.wgFinished.Wait()

	close(c.hasQueue)
	close(c.getQueue)
	close(c.writeQueue)
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
}

func HTTPFlags(prefix string) HTTPStoreFlags {
	return HTTPStoreFlags{
		flag.String(prefix+"h", "", "http host to connect to"),
	}
}

func (h HTTPStoreFlags) CreateStore() ChunkStore {
	return h.CreateNamespacedStore("")
}

func (h HTTPStoreFlags) CreateNamespacedStore(ns string) ChunkStore {
	if h.check() {
		return NewHTTPStore(*h.host + httprouter.CleanPath(ns))
	}
	return nil
}

func (h HTTPStoreFlags) CreateFactory() Factory {
	if h.check() {
		return h
	}
	return nil
}

func (h HTTPStoreFlags) check() bool {
	return *h.host != ""
}
