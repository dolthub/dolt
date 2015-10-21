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

	"github.com/attic-labs/noms/constants"
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
	ch chan Chunk
}

// readBatch represents a set of queued read requests, each of which are blocking on a receive channel for a response.
type readBatch map[ref.Ref][]chan Chunk

func (rrg *readBatch) Put(c Chunk) {
	for _, ch := range (*rrg)[c.Ref()] {
		ch <- c
	}

	delete(*rrg, c.Ref())
}

// Callers to Get() must receive nil if the corresponding chunk wasn't in the response from the server (i.e. it wasn't found).
func (rrq *readBatch) Close() error {
	for _, chs := range *rrq {
		for _, ch := range chs {
			ch <- EmptyChunk
		}
	}
	return nil
}

type HttpStore struct {
	host       *url.URL
	readQueue  chan readRequest
	writeQueue chan Chunk
	wg         *sync.WaitGroup
	written    map[ref.Ref]bool
	wmu        *sync.Mutex
}

func NewHttpStore(host string) *HttpStore {
	u, err := url.Parse(host)
	d.Exp.NoError(err)
	d.Exp.True(u.Scheme == "http" || u.Scheme == "https")
	d.Exp.Equal(*u, url.URL{Scheme: u.Scheme, Host: u.Host})
	client := &HttpStore{
		u,
		make(chan readRequest, readBufferSize),
		make(chan Chunk, writeBufferSize),
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

func (c *HttpStore) Host() *url.URL {
	return c.host
}

func (c *HttpStore) Get(r ref.Ref) Chunk {
	ch := make(chan Chunk)
	c.readQueue <- readRequest{r, ch}
	return <-ch
}

func (c *HttpStore) sendReadRequests() {
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

func (c *HttpStore) Has(ref ref.Ref) bool {
	// HEAD http://<host>/ref/<sha1-xxx>. Response will be 200 if present, 404 if absent.
	res := c.requestRef(ref, "HEAD", nil)
	defer closeResponse(res)

	d.Chk.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound, "Unexpected response: %s", http.StatusText(res.StatusCode))
	return res.StatusCode == http.StatusOK
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

func (c *HttpStore) sendWriteRequests() {
	for chunk := range c.writeQueue {
		chs := make([]Chunk, 0)
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

	res, err := http.DefaultClient.Do(req)
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

	res, err := http.DefaultClient.Do(req)
	d.Chk.NoError(err)
	return res
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

	res, err := http.DefaultClient.Do(req)
	d.Chk.NoError(err)

	return res
}

func (c *HttpStore) Close() error {
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
