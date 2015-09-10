package chunks

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
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
	targetBufferSize = 1 << 16 // 64k (compressed)
	readBufferSize   = 1 << 12 // 4k
	readLimit        = 6       // Experimentally, 5 was dimishing returns, 1 for good measure
	writeLimit       = 6
)

type readRequest struct {
	r  ref.Ref
	ch chan io.ReadCloser
}

type httpStoreClient struct {
	host       *url.URL
	readQueue  chan readRequest
	cb         *chunkBuffer
	wg         *sync.WaitGroup
	writeLimit chan int
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
		newChunkBuffer(),
		&sync.WaitGroup{},
		make(chan int, writeLimit),
		&sync.Mutex{},
	}

	for i := 0; i < readLimit; i++ {
		go client.sendReadRequests()
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
		reqs := []readRequest{req}

	loop:
		for {
			select {
			case req := <-c.readQueue:
				reqs = append(reqs, req)
			default:
				break loop
			}
		}

		refs := make(map[ref.Ref]bool)
		for _, req := range reqs {
			refs[req.r] = true
		}

		cs := &MemoryStore{}
		c.getRefs(refs, cs)

		for _, req := range reqs {
			req.ch <- cs.Get(req.r)
		}
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

func (c *httpStoreClient) write(r ref.Ref, buff *bytes.Buffer) {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	c.cb.appendChunk(buff)
	if c.cb.isFull() {
		c.flushBuffered()
	}
}

func (c *httpStoreClient) flushBuffered() {
	if c.cb.count == 0 {
		return
	}

	c.cb.finish()

	c.wg.Add(1)
	c.writeLimit <- 1 // TODO: This may block writes, fix so that when the upload limit is hit, incoming writes simply buffer but return immediately
	go func(body *bytes.Buffer) {
		res := c.requestRef(ref.Ref{}, "POST", body)
		defer closeResponse(res)

		d.Chk.Equal(res.StatusCode, http.StatusCreated, "Unexpected response: %s", http.StatusText(res.StatusCode))

		<-c.writeLimit
		c.wg.Done()
	}(c.cb.buff)
	c.cb = newChunkBuffer()
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

func (c *httpStoreClient) getRefs(refs map[ref.Ref]bool, cs ChunkStore) {
	// POST http://<host>/getRefs/. Post query: ref=sha1---&ref=sha1---& Response will be chunk data if present, 404 if absent.
	u := *c.host
	u.Path = getRefsPath
	values := &url.Values{}
	for r, _ := range refs {
		values.Add("ref", r.String())
	}

	req, err := http.NewRequest("POST", u.String(), strings.NewReader(values.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	d.Chk.NoError(err)

	res, err := http.DefaultClient.Do(req)
	d.Chk.NoError(err)
	d.Chk.Equal(http.StatusOK, res.StatusCode, "Unexpected response: %s", http.StatusText(res.StatusCode))

	deserializeToChunkStore(res.Body, cs)
	closeResponse(res)
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
	c.flushBuffered()
	c.wg.Wait()
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
	return nil
}

func (s *httpStoreServer) handleRef(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		if req.Method == "POST" {
			deserializeToChunkStore(req.Body, s.cs)
			w.WriteHeader(http.StatusCreated)
			return
		}

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
		}
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
		refs := req.Form["ref"]
		d.Exp.True(len(refs) > 0)

		cb := newChunkBuffer()
		for _, refStr := range refs {
			r := ref.Parse(refStr)
			reader := s.cs.Get(r)
			if reader != nil {
				buff := &bytes.Buffer{}
				_, err := io.Copy(buff, reader)
				d.Chk.NoError(err)
				reader.Close()
				cb.appendChunk(buff)
			}
		}
		cb.finish()

		_, err := io.Copy(w, cb.buff)
		d.Chk.NoError(err)

		w.Header().Add("Content-Type", "application/octet-stream")
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

/*
	ChunkBuffer:
		Chunk 0
		Chunk 1
		 ..
		Chunk N
		Footer

	Chunk:
		Len   // 4-byte int
		Data  // len(Data) == Len

	The entire ChunkBuffer is gzip'd when serialized and un-gzip'd on deserializeToChunkStore
*/

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type chunkBuffer struct {
	buff  *bytes.Buffer
	w     io.WriteCloser
	count uint32
}

func newChunkBuffer() *chunkBuffer {
	buff := &bytes.Buffer{}
	return &chunkBuffer{buff, gzip.NewWriter(buff), 0}
}

func (cb *chunkBuffer) appendChunk(chunk *bytes.Buffer) {
	d.Chk.True(len(chunk.Bytes()) < 1<<32) // Because of chunking at higher levels, no chunk should never be more than 4GB
	cb.count++

	chunkSize := uint32(chunk.Len())
	err := binary.Write(cb.w, binary.LittleEndian, chunkSize)
	d.Chk.NoError(err)

	n, err := io.Copy(cb.w, chunk)
	d.Chk.NoError(err)
	d.Chk.Equal(uint32(n), chunkSize)
}

func (cb *chunkBuffer) isFull() bool {
	return cb.buff.Len() >= targetBufferSize
}

func (cb *chunkBuffer) finish() {
	cb.w.Close()
	cb.w = nil
}

func deserializeToChunkStore(body io.Reader, cs ChunkStore) {
	r, err := gzip.NewReader(body)
	d.Chk.NoError(err)

	for true {
		chunkSize := uint32(0)
		err = binary.Read(r, binary.LittleEndian, &chunkSize)
		if err == io.EOF {
			break
		}
		d.Chk.NoError(err)

		// BUG 206 - Validate the resulting refs match the client's expectation.
		w := cs.Put()
		_, err := io.CopyN(w, r, int64(chunkSize))
		d.Chk.NoError(err)
		w.Close()
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
