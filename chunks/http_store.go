package chunks

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	rootPath         = "/root/"
	refPath          = "/ref/"
	getRefsPath      = "/getRefs/"
	postRefsPath     = "/postRefs/"
	targetBufferSize = 1 << 16 // 64k (compressed)
	readBufferSize   = 1 << 12 // 4k
	writeBufferSize  = 1 << 12 // 4k
	readLimit        = 6       // Experimentally, 5 was dimishing returns, 1 for good measure
	writeLimit       = 6
)

type readRequest struct {
	r  ref.Ref
	ch chan io.ReadCloser
}

// readBatch represents a set of queued read requests, each of which are blocking on a receive channel for a response. It implements ChunkSink so that the responses can be directly deserialized and streamed back to callers.
type readBatch map[ref.Ref][]chan io.ReadCloser

func (rrg *readBatch) Put() ChunkWriter {
	return newChunkWriter(rrg.write)
}

func (rrg *readBatch) write(r ref.Ref, data []byte) {
	for _, ch := range (*rrg)[r] {
		ch <- ioutil.NopCloser(bytes.NewReader(data))
	}

	delete(*rrg, r)
}

// Callers to Get() must receive nil if the corresponding chunk wasn't in the response from the server (i.e. it wasn't found).
func (rrq *readBatch) respondToFailedReads() {
	for _, chs := range *rrq {
		for _, ch := range chs {
			ch <- nil
		}
	}
}

type writeRequest struct {
	r    ref.Ref
	data []byte
}

type httpStoreClient struct {
	host       *url.URL
	readQueue  chan readRequest
	writeQueue chan writeRequest
	wg         *sync.WaitGroup
	written    map[ref.Ref]bool
	wmu        *sync.Mutex
}

type httpStoreServer struct {
	cs    ChunkStore
	port  int
	l     *net.Listener
	conns map[net.Conn]http.ConnState
}

func NewHttpStoreClient(host string) *httpStoreClient {
	u, err := url.Parse(host)
	d.Exp.NoError(err)
	d.Exp.True(u.Scheme == "http" || u.Scheme == "https")
	d.Exp.Equal(*u, url.URL{Scheme: u.Scheme, Host: u.Host})
	client := &httpStoreClient{
		u,
		make(chan readRequest, readBufferSize),
		make(chan writeRequest, writeBufferSize),
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

func NewHttpStoreServer(cs ChunkStore, port int) *httpStoreServer {
	return &httpStoreServer{
		cs, port, nil, map[net.Conn]http.ConnState{},
	}
}

func (c *httpStoreClient) Get(r ref.Ref) io.ReadCloser {
	ch := make(chan io.ReadCloser)
	c.readQueue <- readRequest{r, ch}
	return <-ch
}

func (c *httpStoreClient) sendReadRequests() {
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
		reqs.respondToFailedReads()
	}
}

func (c *httpStoreClient) Has(ref ref.Ref) bool {
	// HEAD http://<host>/ref/<sha1-xxx>. Response will be 200 if present, 404 if absent.
	res := c.requestRef(ref, "HEAD", nil)
	defer closeResponse(res)

	d.Chk.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound, "Unexpected response: %s", http.StatusText(res.StatusCode))
	return res.StatusCode == http.StatusOK
}

func (c *httpStoreClient) Put() ChunkWriter {
	// POST http://<host>/ref/. Body is a serialized chunkBuffer. Response will be 201.
	return newChunkWriter(c.write)
}

func (c *httpStoreClient) write(r ref.Ref, data []byte) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.written[r] {
		return
	}
	c.written[r] = true

	c.wg.Add(1)
	c.writeQueue <- writeRequest{r, data}
}

func (c *httpStoreClient) sendWriteRequests() {
	for req := range c.writeQueue {
		ms := &MemoryStore{}
		refs := map[ref.Ref]bool{}

		addReq := func(req writeRequest) {
			ms.write(req.r, req.data)
			refs[req.r] = true
		}
		addReq(req)

	loop:
		for {
			select {
			case req := <-c.writeQueue:
				addReq(req)
			default:
				break loop
			}
		}

		c.postRefs(refs, ms)

		for _, _ = range refs {
			c.wg.Done()
		}
	}
}

func (c *httpStoreClient) postRefs(refs map[ref.Ref]bool, cs ChunkSource) {
	body := &bytes.Buffer{}
	gw := gzip.NewWriter(body)
	Serialize(gw, refs, cs)
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

func (c *httpStoreClient) requestRef(r ref.Ref, method string, body io.Reader) *http.Response {
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

func (c *httpStoreClient) getRefs(refs map[ref.Ref]bool, cs ChunkSink) {
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

	Deserialize(reader, cs)
}

func (c *httpStoreClient) Root() ref.Ref {
	// GET http://<host>/root. Response will be ref of root.
	res := c.requestRoot("GET", ref.Ref{}, ref.Ref{})
	defer closeResponse(res)

	d.Chk.Equal(http.StatusOK, res.StatusCode, "Unexpected response: %s", http.StatusText(res.StatusCode))
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	return ref.Parse(string(data))
}

func (c *httpStoreClient) UpdateRoot(current, last ref.Ref) bool {
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

func (c *httpStoreClient) requestRoot(method string, current, last ref.Ref) *http.Response {
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

func (c *httpStoreClient) Close() error {
	c.wg.Wait()
	close(c.readQueue)
	close(c.writeQueue)
	return nil
}

func (s *httpStoreServer) handleRef(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		refStr := ""
		pathParts := strings.Split(req.URL.Path[1:], "/")
		if len(pathParts) > 1 {
			refStr = pathParts[1]
		}
		r := ref.Parse(refStr)

		switch req.Method {
		case "GET":
			reader := s.cs.Get(r)
			if reader == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			defer reader.Close()
			_, err := io.Copy(w, reader)
			d.Chk.NoError(err)
			w.Header().Add("Content-Type", "application/octet-stream")
			w.Header().Add("Cache-Control", "max-age=31536000") // 1 year

		case "HEAD":
			if !s.cs.Has(r) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		default:
			d.Exp.Fail("Unexpected method: ", req.Method)
		}
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func (s *httpStoreServer) handlePostRefs(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		var reader io.Reader = req.Body
		if strings.Contains(req.Header.Get("Content-Encoding"), "gzip") {
			gr, err := gzip.NewReader(reader)
			d.Exp.NoError(err)
			defer gr.Close()
			reader = gr
		}

		Deserialize(reader, s.cs)
		w.WriteHeader(http.StatusCreated)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func (s *httpStoreServer) handleGetRefs(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		req.ParseForm()
		refStrs := req.PostForm["ref"]
		d.Exp.True(len(refStrs) > 0)

		refs := map[ref.Ref]bool{}
		for _, refStr := range refStrs {
			r := ref.Parse(refStr)
			refs[r] = true
		}

		w.Header().Add("Content-Type", "application/octet-stream")

		writer := w.(io.Writer)
		if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Add("Content-Encoding", "gzip")
			gw := gzip.NewWriter(w)
			defer gw.Close()
			writer = gw
		}

		Serialize(writer, refs, s.cs)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func (s *httpStoreServer) handleRoot(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		switch req.Method {
		case "GET":
			rootRef := s.cs.Root()
			fmt.Fprintf(w, "%v", rootRef.String())
			w.Header().Add("content-type", "text/plain")

		case "POST":
			params := req.URL.Query()
			tokens := params["last"]
			d.Exp.Len(tokens, 1)
			last := ref.Parse(tokens[0])
			tokens = params["current"]
			d.Exp.Len(tokens, 1)
			current := ref.Parse(tokens[0])

			if !s.cs.UpdateRoot(current, last) {
				w.WriteHeader(http.StatusConflict)
				return
			}
		}
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

// In order for keep alive to work we must read to EOF on every response. We may want to add a timeout so that a server that left its connection open can't cause all of ports to be eaten up.
func closeResponse(res *http.Response) error {
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	d.Chk.Equal(0, len(data))
	return res.Body.Close()
}

func (s *httpStoreServer) connState(c net.Conn, cs http.ConnState) {
	switch cs {
	case http.StateNew, http.StateActive, http.StateIdle:
		s.conns[c] = cs
	default:
		delete(s.conns, c)
	}
}

// Blocks while the server is listening. Running on a separate go routine is supported.
func (s *httpStoreServer) Run() {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	d.Chk.NoError(err)
	s.l = &l

	mux := http.NewServeMux()

	mux.HandleFunc(refPath, http.HandlerFunc(s.handleRef))
	mux.HandleFunc(getRefsPath, http.HandlerFunc(s.handleGetRefs))
	mux.HandleFunc(postRefsPath, http.HandlerFunc(s.handlePostRefs))
	mux.HandleFunc(rootPath, http.HandlerFunc(s.handleRoot))

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Access-Control-Allow-Origin", "*")
			mux.ServeHTTP(w, r)
		}),
		ConnState: s.connState,
	}
	srv.Serve(l)
}

// Will cause the server to stop listening and an existing call to Run() to continue.
func (s *httpStoreServer) Stop() {
	(*s.l).Close()
	for c, _ := range s.conns {
		c.Close()
	}
}

type httpStoreFlags struct {
	host *string
}

func httpFlags(prefix string) httpStoreFlags {
	return httpStoreFlags{
		flag.String(prefix+"h", "", "httpstore host to connect to"),
	}
}

func (h httpStoreFlags) createStore() ChunkStore {
	if *h.host == "" {
		return nil
	} else {
		return NewHttpStoreClient(*h.host)
	}
}
