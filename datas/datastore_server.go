package datas

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type dataStoreServer struct {
	ds    DataStore
	port  int
	l     *net.Listener
	conns map[net.Conn]http.ConnState
}

func NewDataStoreServer(ds DataStore, port int) *dataStoreServer {
	return &dataStoreServer{
		ds, port, nil, map[net.Conn]http.ConnState{},
	}
}

func (s *dataStoreServer) handleGetReachable(r ref.Ref, w http.ResponseWriter, req *http.Request) {
	excludeRef := ref.Ref{}
	exclude := req.URL.Query().Get("exclude")
	if exclude != "" {
		excludeRef = ref.Parse(exclude)
	}

	w.Header().Add("Content-Type", "application/octet-stream")
	writer := w.(io.Writer)
	if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Add("Content-Encoding", "gzip")
		gw := gzip.NewWriter(w)
		defer gw.Close()
		writer = gw
	}

	sz := chunks.NewSerializer(writer)
	s.ds.CopyReachableChunksP(r, excludeRef, sz, 512)
	sz.Close()
}

func (s *dataStoreServer) handleRef(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		refStr := ""
		pathParts := strings.Split(req.URL.Path[1:], "/")
		if len(pathParts) > 1 {
			refStr = pathParts[1]
		}
		r := ref.Parse(refStr)

		switch req.Method {
		case "GET":
			all := req.URL.Query().Get("all")
			if all == "true" {
				s.handleGetReachable(r, w, req)
				return
			}
			chunk := s.ds.Get(r)
			if chunk.IsEmpty() {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			_, err := io.Copy(w, bytes.NewReader(chunk.Data()))
			d.Chk.NoError(err)
			w.Header().Add("Content-Type", "application/octet-stream")
			w.Header().Add("Cache-Control", "max-age=31536000") // 1 year

		case "HEAD":
			if !s.ds.Has(r) {
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

func (s *dataStoreServer) handlePostRefs(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		var reader io.Reader = req.Body
		if strings.Contains(req.Header.Get("Content-Encoding"), "gzip") {
			gr, err := gzip.NewReader(reader)
			d.Exp.NoError(err)
			defer gr.Close()
			reader = gr
		}

		chunks.Deserialize(reader, s.ds)
		w.WriteHeader(http.StatusCreated)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func (s *dataStoreServer) handleGetRefs(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		req.ParseForm()
		refStrs := req.PostForm["ref"]
		d.Exp.True(len(refStrs) > 0)

		refs := make([]ref.Ref, 0)
		for _, refStr := range refStrs {
			refs = append(refs, ref.Parse(refStr))
		}

		w.Header().Add("Content-Type", "application/octet-stream")
		writer := w.(io.Writer)
		if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Add("Content-Encoding", "gzip")
			gw := gzip.NewWriter(w)
			defer gw.Close()
			writer = gw
		}

		sz := chunks.NewSerializer(writer)
		for _, r := range refs {
			c := s.ds.Get(r)
			if !c.IsEmpty() {
				sz.Put(c)
			}
		}
		sz.Close()
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func (s *dataStoreServer) handleRoot(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		switch req.Method {
		case "GET":
			rootRef := s.ds.Root()
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

			if !s.ds.UpdateRoot(current, last) {
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

func (s *dataStoreServer) connState(c net.Conn, cs http.ConnState) {
	switch cs {
	case http.StateNew, http.StateActive, http.StateIdle:
		s.conns[c] = cs
	default:
		delete(s.conns, c)
	}
}

// Blocks while the dataStoreServer is listening. Running on a separate go routine is supported.
func (s *dataStoreServer) Run() {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	d.Chk.NoError(err)
	s.l = &l

	mux := http.NewServeMux()

	mux.HandleFunc(constants.RefPath, http.HandlerFunc(s.handleRef))
	mux.HandleFunc(constants.GetRefsPath, http.HandlerFunc(s.handleGetRefs))
	mux.HandleFunc(constants.PostRefsPath, http.HandlerFunc(s.handlePostRefs))
	mux.HandleFunc(constants.RootPath, http.HandlerFunc(s.handleRoot))

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Access-Control-Allow-Origin", "*")
			mux.ServeHTTP(w, r)
		}),
		ConnState: s.connState,
	}
	srv.Serve(l)
}

// Will cause the dataStoreServer to stop listening and an existing call to Run() to continue.
func (s *dataStoreServer) Stop() {
	(*s.l).Close()
	for c, _ := range s.conns {
		c.Close()
	}
}
