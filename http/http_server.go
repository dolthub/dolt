package http

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	rootPath          = "/root/"
	refPath           = "/ref/"
	getRefsPath       = "/getRefs/"
	postRefsPath      = "/postRefs/"
	maxConcurrentPuts = 64
)

type httpServer struct {
	cs         chunks.ChunkStore
	port       int
	l          *net.Listener
	conns      map[net.Conn]http.ConnState
	writeLimit chan struct{}
}

func NewHttpServer(cs chunks.ChunkStore, port int) *httpServer {
	return &httpServer{
		cs, port, nil, map[net.Conn]http.ConnState{}, make(chan struct{}, maxConcurrentPuts),
	}
}

func (s *httpServer) handleRef(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		refStr := ""
		pathParts := strings.Split(req.URL.Path[1:], "/")
		if len(pathParts) > 1 {
			refStr = pathParts[1]
		}
		r := ref.Parse(refStr)

		switch req.Method {
		case "GET":
			data := s.cs.Get(r)
			if data == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			_, err := io.Copy(w, bytes.NewReader(data))
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

func (s *httpServer) handlePostRefs(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		var reader io.Reader = req.Body
		if strings.Contains(req.Header.Get("Content-Encoding"), "gzip") {
			gr, err := gzip.NewReader(reader)
			d.Exp.NoError(err)
			defer gr.Close()
			reader = gr
		}

		ch := make(chan chunks.Chunk, 64)
		go chunks.Deserialize(reader, ch)

		wg := sync.WaitGroup{}
		for c := range ch {
			wg.Add(1)

			s.writeLimit <- struct{}{}
			c := c
			go func() {
				defer func() {
					wg.Done()
					<-s.writeLimit
				}()

				w := s.cs.Put()
				defer w.Close()
				_, err := io.Copy(w, bytes.NewReader(c.Data))
				d.Chk.NoError(err)
			}()
		}

		wg.Wait()
		w.WriteHeader(http.StatusCreated)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func (s *httpServer) handleGetRefs(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		req.ParseForm()
		refStrs := req.PostForm["ref"]
		d.Exp.True(len(refStrs) > 0)

		w.Header().Add("Content-Type", "application/octet-stream")
		writer := w.(io.Writer)
		if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Add("Content-Encoding", "gzip")
			gw := gzip.NewWriter(w)
			defer gw.Close()
			writer = gw
		}

		data := make(chan chunks.Chunk, 64)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			chunks.Serialize(writer, data)
			wg.Done()
		}()
		for _, refStr := range refStrs {
			r := ref.Parse(refStr)
			d := s.cs.Get(r)
			if d == nil {
				continue
			}

			data <- chunks.Chunk{r, d}
		}
		close(data)
		wg.Wait()
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func (s *httpServer) handleRoot(w http.ResponseWriter, req *http.Request) {
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

func (s *httpServer) connState(c net.Conn, cs http.ConnState) {
	switch cs {
	case http.StateNew, http.StateActive, http.StateIdle:
		s.conns[c] = cs
	default:
		delete(s.conns, c)
	}
}

// Blocks while the server is listening. Running on a separate go routine is supported.
func (s *httpServer) Run() {
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
func (s *httpServer) Stop() {
	(*s.l).Close()
	for c, _ := range s.conns {
		c.Close()
	}
}
