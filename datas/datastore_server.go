package datas

import (
	"fmt"
	"net"
	"net/http"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/constants"
	"github.com/attic-labs/noms/d"
	"github.com/julienschmidt/httprouter"
)

type connectionState struct {
	c  net.Conn
	cs http.ConnState
}

type remoteDataStoreServer struct {
	cs      chunks.ChunkStore
	port    int
	l       *net.Listener
	csChan  chan *connectionState
	closing bool
}

func NewRemoteDataStoreServer(cs chunks.ChunkStore, port int) *remoteDataStoreServer {
	return &remoteDataStoreServer{
		cs, port, nil, make(chan *connectionState, 16), false,
	}
}

// Run blocks while the remoteDataStoreServer is listening. Running on a separate go routine is supported.
func (s *remoteDataStoreServer) Run() {
	fmt.Printf("Listening on port %d...\n", s.port)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	d.Chk.NoError(err)
	s.l = &l

	router := httprouter.New()

	router.POST(constants.GetRefsPath, s.makeHandle(HandleGetRefs))
	router.GET(constants.RootPath, s.makeHandle(HandleRootGet))
	router.POST(constants.RootPath, s.makeHandle(HandleRootPost))
	router.POST(constants.PostRefsPath, s.makeHandle(HandlePostRefs))
	router.POST(constants.WriteValuePath, s.makeHandle(HandleWriteValue))

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Add("Access-Control-Allow-Origin", "*")
			router.ServeHTTP(w, req)
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

func (s *remoteDataStoreServer) makeHandle(hndlr Handler) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		hndlr(w, req, ps, s.cs)
	}
}

func (s *remoteDataStoreServer) connState(c net.Conn, cs http.ConnState) {
	if s.closing {
		d.Chk.Equal(cs, http.StateClosed)
		return
	}
	s.csChan <- &connectionState{c, cs}
}

// Will cause the remoteDataStoreServer to stop listening and an existing call to Run() to continue.
func (s *remoteDataStoreServer) Stop() {
	s.closing = true
	(*s.l).Close()
	(s.cs).Close()
	close(s.csChan)
}
