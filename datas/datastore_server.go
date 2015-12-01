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

type connectionState struct {
	c  net.Conn
	cs http.ConnState
}

type dataStoreServer struct {
	ds     DataStore
	port   int
	l      *net.Listener
	csChan chan *connectionState
}

func NewDataStoreServer(ds DataStore, port int) *dataStoreServer {
	return &dataStoreServer{
		ds, port, nil, make(chan *connectionState, 16),
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

		chunks.Deserialize(reader, s.ds, nil)
		w.WriteHeader(http.StatusCreated)
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusBadRequest)
		return
	}
}

func (s *dataStoreServer) handleGetHasRefs(w http.ResponseWriter, req *http.Request) {
	err := d.Try(func() {
		d.Exp.Equal("POST", req.Method)

		req.ParseForm()
		refStrs := req.PostForm["ref"]
		d.Exp.True(len(refStrs) > 0)

		refs := make([]ref.Ref, 0)
		for _, refStr := range refStrs {
			refs = append(refs, ref.Parse(refStr))
		}

		w.Header().Add("Content-Type", "text/plain")
		writer := w.(io.Writer)
		if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Add("Content-Encoding", "gzip")
			gw := gzip.NewWriter(w)
			defer gw.Close()
			writer = gw
		}

		sz := chunks.NewSerializer(writer)
		for _, r := range refs {
			has := s.ds.Has(r)
			fmt.Fprintf(writer, "%s %t\n", r, has)
		}
		sz.Close()
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
	s.csChan <- &connectionState{c, cs}
}

// Blocks while the dataStoreServer is listening. Running on a separate go routine is supported.
func (s *dataStoreServer) Run() {
	fmt.Printf("Listening on port %d...\n", s.port)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	d.Chk.NoError(err)
	s.l = &l

	mux := http.NewServeMux()

	mux.HandleFunc(constants.RefPath, http.HandlerFunc(s.handleRef))
	mux.HandleFunc(constants.GetHasPath, http.HandlerFunc(s.handleGetHasRefs))
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

	go func() {
		m := map[net.Conn]http.ConnState{}
		for connState := range s.csChan {
			switch connState.cs {
			case http.StateNew, http.StateActive, http.StateIdle:
				m[connState.c] = connState.cs
			default:
				delete(m, connState.c)
			}
		}
		for c := range m {
			c.Close()
		}
	}()

	srv.Serve(l)
}

// Will cause the dataStoreServer to stop listening and an existing call to Run() to continue.
func (s *dataStoreServer) Stop() {
	(*s.l).Close()
	close(s.csChan)
}
