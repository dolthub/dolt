package http

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

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	targetBufferSize = 1 << 16 // 64k (compressed)
	readBufferSize   = 1 << 12 // 4k
	writeBufferSize  = 1 << 12 // 4k
	readLimit        = 6       // Experimentally, 5 was dimishing returns, 1 for good measure
	writeLimit       = 6
)

type readRequest struct {
	r  ref.Ref
	ch chan chunks.Chunk
}

// readBatch represents a set of queued read requests, each of which are blocking on a receive channel for a response.
type readBatch map[ref.Ref][]chan chunks.Chunk

func (rrg *readBatch) Put(c chunks.Chunk) {
	for _, ch := range (*rrg)[c.Ref()] {
		ch <- c
	}

	delete(*rrg, c.Ref())
}

// Callers to Get() must receive nil if the corresponding chunk wasn't in the response from the server (i.e. it wasn't found).
func (rrq *readBatch) Close() error {
	for _, chs := range *rrq {
		for _, ch := range chs {
			ch <- chunks.EmptyChunk
		}
	}
	return nil
}

type HttpClient struct {
	host       *url.URL
	readQueue  chan readRequest
	writeQueue chan chunks.Chunk
	wg         *sync.WaitGroup
	written    map[ref.Ref]bool
	wmu        *sync.Mutex
}

func NewHttpClient(host string) *HttpClient {
	u, err := url.Parse(host)
	d.Exp.NoError(err)
	d.Exp.True(u.Scheme == "http" || u.Scheme == "https")
	d.Exp.Equal(*u, url.URL{Scheme: u.Scheme, Host: u.Host})
	client := &HttpClient{
		u,
		make(chan readRequest, readBufferSize),
		make(chan chunks.Chunk, writeBufferSize),
		&sync.WaitGroup{},
		map[ref.Ref]bool{},
		&sync.Mutex{},
	}

	for i := 0; i < readLimit; i++ {
		go client.sendReadRequests()
	}

	for i := 0; i < writeLimit; i++ {
		go client.sendWriteRequests()
	}

	return client
}

func (c *HttpClient) Get(r ref.Ref) chunks.Chunk {
	ch := make(chan chunks.Chunk)
	c.readQueue <- readRequest{r, ch}
	return <-ch
}

func (c *HttpClient) sendReadRequests() {
	for req := range c.readQueue {
		reqs := readBatch{}
		refs := map[ref.Ref]bool{}

		addReq := func(req readRequest) {
			reqs[req.r] = append(reqs[req.r], req.ch)
			refs[req.r] = true
		}
		addReq(req)

	loop:
		for {
			select {
			case req := <-c.readQueue:
				addReq(req)
			default:
				break loop
			}
		}

		c.getRefs(refs, &reqs)
		reqs.Close()
	}
}

func (c *HttpClient) Has(ref ref.Ref) bool {
	// HEAD http://<host>/ref/<sha1-xxx>. Response will be 200 if present, 404 if absent.
	res := c.requestRef(ref, "HEAD", nil)
	defer closeResponse(res)

	d.Chk.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound, "Unexpected response: %s", http.StatusText(res.StatusCode))
	return res.StatusCode == http.StatusOK
}

func (c *HttpClient) Put(chunk chunks.Chunk) {
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

func (c *HttpClient) sendWriteRequests() {
	for chunk := range c.writeQueue {
		chs := make([]chunks.Chunk, 0)
		chs = append(chs, chunk)

	loop:
		for {
			select {
			case chunk := <-c.writeQueue:
				chs = append(chs, chunk)
			default:
				break loop
			}
		}

		c.postRefs(chs)
		for _, _ = range chs {
			c.wg.Done()
		}
	}
}

func (c *HttpClient) postRefs(chs []chunks.Chunk) {
	body := &bytes.Buffer{}
	gw := gzip.NewWriter(body)
	sz := chunks.NewSerializer(gw)
	for _, chunk := range chs {
		sz.Put(chunk)
	}
	sz.Close()
	gw.Close()

	url := *c.host
	url.Path = postRefsPath
	req, err := http.NewRequest("POST", url.String(), body)
	d.Chk.NoError(err)

	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/octet-stream")

	res, err := http.DefaultClient.Do(req)
	d.Chk.NoError(err)

	d.Chk.Equal(res.StatusCode, http.StatusCreated, "Unexpected response: %s", http.StatusText(res.StatusCode))
	closeResponse(res)
}

func (c *HttpClient) requestRef(r ref.Ref, method string, body io.Reader) *http.Response {
	url := *c.host
	url.Path = refPath
	if (r != ref.Ref{}) {
		url.Path = path.Join(url.Path, r.String())
	}

	req, err := http.NewRequest(method, url.String(), body)
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	d.Chk.NoError(err)

	res, err := http.DefaultClient.Do(req)
	d.Chk.NoError(err)
	return res
}

func (c *HttpClient) getRefs(refs map[ref.Ref]bool, cs chunks.ChunkSink) {
	// POST http://<host>/getRefs/. Post body: ref=sha1---&ref=sha1---& Response will be chunk data if present, 404 if absent.
	u := *c.host
	u.Path = getRefsPath
	values := &url.Values{}
	for r, _ := range refs {
		values.Add("ref", r.String())
	}

	req, err := http.NewRequest("POST", u.String(), strings.NewReader(values.Encode()))
	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	d.Chk.NoError(err)

	res, err := http.DefaultClient.Do(req)
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

	chunks.Deserialize(reader, cs)
}

func (c *HttpClient) Root() ref.Ref {
	// GET http://<host>/root. Response will be ref of root.
	res := c.requestRoot("GET", ref.Ref{}, ref.Ref{})
	defer closeResponse(res)

	d.Chk.Equal(http.StatusOK, res.StatusCode, "Unexpected response: %s", http.StatusText(res.StatusCode))
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	return ref.Parse(string(data))
}

func (c *HttpClient) UpdateRoot(current, last ref.Ref) bool {
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

func (c *HttpClient) requestRoot(method string, current, last ref.Ref) *http.Response {
	u := *c.host
	u.Path = rootPath
	if method == "POST" {
		d.Exp.True(current != ref.Ref{})
		params := url.Values{}
		params.Add("last", last.String())
		params.Add("current", current.String())
		u.RawQuery = params.Encode()
	}

	req, err := http.NewRequest(method, u.String(), nil)
	d.Chk.NoError(err)

	res, err := http.DefaultClient.Do(req)
	d.Chk.NoError(err)

	return res
}

func (c *HttpClient) Close() error {
	c.wg.Wait()
	close(c.readQueue)
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

type Flags struct {
	host *string
}

func NewFlagsWithPrefix(prefix string) Flags {
	return Flags{
		flag.String(prefix+"h", "", "http host to connect to"),
	}
}

func (h Flags) CreateClient() *HttpClient {
	if *h.host == "" {
		return nil
	} else {
		return NewHttpClient(*h.host)
	}
}
