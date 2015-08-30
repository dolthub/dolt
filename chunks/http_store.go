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
	targetBufferSize = 1 << 16 // 64k (compressed)
)

type httpStoreClient struct {
	host        *url.URL
	cb          *chunkBuffer
	wg          *sync.WaitGroup
	uploadLimit chan int
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
	return &httpStoreClient{
		u,
		newChunkBuffer(),
		&sync.WaitGroup{},
		make(chan int, 8),
	}
}

func NewHttpStoreServer(cs ChunkStore, port int) *httpStoreServer {
	return &httpStoreServer{
		cs, port, nil, map[net.Conn]http.ConnState{},
	}
}

func (c *httpStoreClient) Get(ref ref.Ref) io.ReadCloser {
	// GET http://<host>/ref/<sha1-xxx>. Response will be chunk data if present, 404 if absent.
	res := c.requestRef(ref, "GET", nil)

	d.Chk.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound, "Unexpected response: %s", http.StatusText(res.StatusCode))
	if res.StatusCode == http.StatusOK {
		return res.Body
	}

	closeResponse(res)
	return nil
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
	c.uploadLimit <- 1
	go func(body *bytes.Buffer) {
		res := c.requestRef(ref.Ref{}, "POST", body)
		defer closeResponse(res)

		d.Chk.Equal(res.StatusCode, http.StatusCreated, "Unexpected response: %s", http.StatusText(res.StatusCode))

		<-c.uploadLimit
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

func (s *httpStoreServer) handleRequestRef(w http.ResponseWriter, req *http.Request) {
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

func (s *httpStoreServer) handleRequestRoot(w http.ResponseWriter, req *http.Request) {
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
	mux.HandleFunc(refPath, http.HandlerFunc(s.handleRequestRef))
	mux.HandleFunc(rootPath, http.HandlerFunc(s.handleRequestRoot))

	srv := &http.Server{
		Handler:   mux,
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
