package chunks

import (
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

	"bufio"

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
func makeHttpClient() *http.Client {
	t := http.Transport(*http.DefaultTransport.(*http.Transport))
	t.MaxIdleConnsPerHost = requestLimit

	return &http.Client{
		Transport: &t,
		Timeout:   time.Duration(30) * time.Second,
	}
}

type HttpStore struct {
	host         *url.URL
	httpClient   *http.Client
	getQueue     chan getRequest
	hasQueue     chan hasRequest
	writeQueue   chan Chunk
	finishedChan chan struct{}
	wg           *sync.WaitGroup
	wgFinished   *sync.WaitGroup
	written      map[ref.Ref]bool
	wmu          *sync.Mutex
}

func NewHttpStore(host string) *HttpStore {
	u, err := url.Parse(host)
	d.Exp.NoError(err)
	d.Exp.True(u.Scheme == "http" || u.Scheme == "https")
	d.Exp.Equal(*u, url.URL{Scheme: u.Scheme, Host: u.Host})
	client := &HttpStore{
		host:         u,
		httpClient:   makeHttpClient(),
		getQueue:     make(chan getRequest, readBufferSize),
		hasQueue:     make(chan hasRequest, hasBufferSize),
		writeQueue:   make(chan Chunk, writeBufferSize),
		finishedChan: make(chan struct{}),
		wg:           &sync.WaitGroup{},
		wgFinished:   &sync.WaitGroup{},
		written:      map[ref.Ref]bool{},
		wmu:          &sync.Mutex{},
	}

	for i := 0; i < requestLimit; i++ {
		go client.batchRequests()
	}

	return client
}

func (c *HttpStore) Host() *url.URL {
	return c.host
}

func (c *HttpStore) Get(r ref.Ref) Chunk {
	ch := make(chan Chunk)
	c.wg.Add(1)
	c.getQueue <- getRequest{r, ch}
	return <-ch
}

func (c *HttpStore) sendReadRequests(req getRequest) {
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

func (c *HttpStore) Has(ref ref.Ref) bool {
	ch := make(chan bool)
	c.wg.Add(1)
	c.hasQueue <- hasRequest{ref, ch}
	return <-ch
}

func (c *HttpStore) sendHasRequests(req hasRequest) {
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

func (c *HttpStore) Put(chunk Chunk) {
	// POST http://<host>/ref/. Body is a serialized chunkBuffer. Response will be 201.
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.written[chunk.Ref()] {
		return
	}
	c.written[chunk.Ref()] = true

	c.wg.Add(1)
	c.writeQueue <- chunk
}

func (c *HttpStore) sendWriteRequests(chunk Chunk) {
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

func (c *HttpStore) batchRequests() {
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

func (c *HttpStore) postRefs(chs []Chunk) {
	body := &bytes.Buffer{}
	gw := gzip.NewWriter(body)
	sz := NewSerializer(gw)
	for _, chunk := range chs {
		sz.Put(chunk)
	}
	sz.Close()
	gw.Close()

	url := *c.host
	url.Path = constants.PostRefsPath
	req, err := http.NewRequest("POST", url.String(), body)
	d.Chk.NoError(err)

	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/octet-stream")

	res, err := c.httpClient.Do(req)
	d.Chk.NoError(err)

	d.Chk.Equal(res.StatusCode, http.StatusCreated, "Unexpected response: %s", http.StatusText(res.StatusCode))
	closeResponse(res)
}

func (c *HttpStore) requestRef(r ref.Ref, method string, body io.Reader) *http.Response {
	url := *c.host
	url.Path = constants.RefPath
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

func (c *HttpStore) getHasRefs(refs map[ref.Ref]bool, reqs hasBatch) {
	// POST http://<host>/getHasRefs/. Post body: ref=sha1---&ref=sha1---& Response will be text of lines containing "|ref| |bool|"
	u := *c.host
	u.Path = constants.GetHasPath
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

func (c *HttpStore) getRefs(refs map[ref.Ref]bool, cs ChunkSink) {
	// POST http://<host>/getRefs/. Post body: ref=sha1---&ref=sha1---& Response will be chunk data if present, 404 if absent.
	u := *c.host
	u.Path = constants.GetRefsPath
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

func (c *HttpStore) Root() ref.Ref {
	// GET http://<host>/root. Response will be ref of root.
	res := c.requestRoot("GET", ref.Ref{}, ref.Ref{})
	defer closeResponse(res)

	d.Chk.Equal(http.StatusOK, res.StatusCode, "Unexpected response: %s", http.StatusText(res.StatusCode))
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	return ref.Parse(string(data))
}

func (c *HttpStore) UpdateRoot(current, last ref.Ref) bool {
	// POST http://<host>root?current=<ref>&last=<ref>. Response will be 200 on success, 409 if current is outdated.
	c.wg.Wait()

	c.wmu.Lock()
	c.written = map[ref.Ref]bool{}
	c.wmu.Unlock()

	res := c.requestRoot("POST", current, last)
	defer closeResponse(res)

	d.Chk.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusConflict, "Unexpected response: %s", http.StatusText(res.StatusCode))
	return res.StatusCode == http.StatusOK
}

func (c *HttpStore) requestRoot(method string, current, last ref.Ref) *http.Response {
	u := *c.host
	u.Path = constants.RootPath
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

func (c *HttpStore) Close() error {
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
	d.Chk.Equal(0, len(data))
	return res.Body.Close()
}

type HttpStoreFlags struct {
	host *string
}

func HttpFlags(prefix string) HttpStoreFlags {
	return HttpStoreFlags{
		flag.String(prefix+"h", "", "http host to connect to"),
	}
}

func (h HttpStoreFlags) CreateStore() ChunkStore {
	if *h.host == "" {
		return nil
	} else {
		return NewHttpStore(*h.host)
	}
}
