package chunks

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	rootPath = "root"
	refPath  = "ref"
)

type HttpStoreClient struct {
	host *url.URL
}

type HttpStoreServer struct {
	cs   ChunkStore
	port int
	l    *net.TCPListener
}

func NewHttpStoreClient(host string) *HttpStoreClient {
	u, err := url.Parse(host)
	d.Exp.NoError(err)
	d.Exp.True(u.Scheme == "http" || u.Scheme == "https")
	d.Exp.Equal("", u.Path)
	d.Exp.Equal("", u.RawQuery)

	return &HttpStoreClient{u}
}

func NewHttpStoreServer(cs ChunkStore, port int) *HttpStoreServer {
	return &HttpStoreServer{
		cs, port, nil,
	}
}

// GET http://<host>/ref/<sha1-xxx>. Response will be chunk data if present, 404 if absent.
func (c *HttpStoreClient) Get(ref ref.Ref) io.ReadCloser {
	res := c.requestRef(ref, "GET", nil)

	d.Chk.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound, "Unexpected response: %s", http.StatusText(res.StatusCode))
	if res.StatusCode == http.StatusOK {
		return res.Body
	}

	closeResponse(res)
	return nil
}

// HEAD http://<host>/ref/<sha1-xxx>. Response will be 200 if present, 404 if absent.
func (c *HttpStoreClient) Has(ref ref.Ref) bool {
	res := c.requestRef(ref, "HEAD", nil)
	defer closeResponse(res)

	d.Chk.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNotFound, "Unexpected response: %s", http.StatusText(res.StatusCode))
	return res.StatusCode == http.StatusOK
}

// PUT http://<host>/ref/<sha1-xxx>. Response will be 201.
func (c *HttpStoreClient) Put() ChunkWriter {
	return newChunkWriter(c.Has, c.write)
}

func (c *HttpStoreClient) write(r ref.Ref, buff *bytes.Buffer) {
	res := c.requestRef(r, "PUT", buff)
	defer closeResponse(res)

	d.Chk.Equal(res.StatusCode, http.StatusCreated, "Unexpected response: %s", http.StatusText(res.StatusCode))
}

func (c *HttpStoreClient) requestRef(ref ref.Ref, method string, body io.Reader) *http.Response {
	url := *c.host
	url.Path = path.Join(refPath, ref.String())
	req, err := http.NewRequest(method, url.String(), body)
	d.Chk.NoError(err)

	res, err := http.DefaultClient.Do(req)
	d.Chk.NoError(err)
	return res
}

// GET http://<host>/root. Response will be ref of root.
func (c *HttpStoreClient) Root() ref.Ref {
	res := c.requestRoot("GET", ref.Ref{}, ref.Ref{})
	defer closeResponse(res)

	d.Chk.Equal(http.StatusOK, res.StatusCode, "Unexpected response: %s", http.StatusText(res.StatusCode))
	buff := &bytes.Buffer{}
	io.Copy(buff, res.Body)
	return ref.Parse(string(buff.Bytes()))
}

// POST http://<host>root?current=<ref>&last=<ref>. Response will be 200 on success, 409 if current is outdated.
func (c *HttpStoreClient) UpdateRoot(current, last ref.Ref) bool {
	res := c.requestRoot("POST", current, last)
	defer closeResponse(res)

	d.Chk.True(res.StatusCode == http.StatusOK || res.StatusCode == http.StatusConflict, "Unexpected response: %s", http.StatusText(res.StatusCode))
	return res.StatusCode == http.StatusOK
}

func (c *HttpStoreClient) requestRoot(method string, current, last ref.Ref) *http.Response {
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

func (s *HttpStoreServer) HandleRequestRef(w http.ResponseWriter, req *http.Request, refStr string) {
	err := d.Try(func() {
		r := ref.Parse(refStr)

		switch req.Method {
		case "GET":
			reader := s.cs.Get(r)
			if reader == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			io.Copy(w, reader)
			w.Header().Add("content-type", "application/octet-stream")
			w.Header().Add("cache-control", "max-age=31536000") // 1 year

		case "HEAD":
			if !s.cs.Has(r) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		case "PUT":
			writer := s.cs.Put()
			defer writer.Close()
			io.Copy(writer, req.Body)
			writer.Ref() // BUG 206
			w.WriteHeader(http.StatusCreated)
		}
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func (s *HttpStoreServer) HandleRequestRoot(w http.ResponseWriter, req *http.Request) {
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

func (s *HttpStoreServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")

	firstPart := ""
	pathParts := strings.Split(r.URL.Path[1:], "/")
	if len(pathParts) > 0 {
		firstPart = pathParts[0]
	}

	switch firstPart {
	case rootPath:
		s.HandleRequestRoot(w, r)
	case refPath:
		refStr := ""
		if len(pathParts) > 1 {
			refStr = pathParts[1]
		}

		s.HandleRequestRef(w, r, refStr)
	default:
		http.Error(w, fmt.Sprintf("Unrecognized: %v", r.URL.Path[1:]), http.StatusBadRequest)
	}
}

// In order for keep alive to work we must read to EOF on every response. We may want to add a timeout so that a server that left it's connection open can't cause all of ports to be eaten up.
func closeResponse(res *http.Response) error {
	data, err := ioutil.ReadAll(res.Body)
	d.Chk.NoError(err)
	d.Chk.Equal(0, len(data))
	return res.Body.Close()
}

// Start blocks while the server is listening. Running on a separate go routine is supported.
func (s *HttpStoreServer) Start() {
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: s.port})
	d.Chk.NoError(err)
	s.l = l

	srv := &http.Server{Handler: http.HandlerFunc(s.handleRequest)}
	srv.Serve(l)
}

// Will cause the server to stop listening and an existing call to Start() to continue.
func (s *HttpStoreServer) Stop() {
	s.l.Close()
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
