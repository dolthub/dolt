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

type remoteDatabaseServer struct {
	csf     chunks.Factory
	port    int
	l       *net.Listener
	csChan  chan *connectionState
	closing bool
}

func NewRemoteDatabaseServer(csf chunks.Factory, port int) *remoteDatabaseServer {
	return &remoteDatabaseServer{
		csf, port, nil, make(chan *connectionState, 16), false,
	}
}

// Run blocks while the remoteDatabaseServer is listening. Running on a separate go routine is supported.
func (s *remoteDatabaseServer) Run() {
	fmt.Printf("Listening on port %d...\n", s.port)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	d.Chk.NoError(err)
	s.l = &l

	router := httprouter.New()

	dsKey := "database"
	router.POST(fmt.Sprintf("/:%s%s", dsKey, constants.GetRefsPath), s.makeHandle(HandleGetRefs))
	router.GET(fmt.Sprintf("/:%s%s", dsKey, constants.RootPath), s.makeHandle(HandleRootGet))
	router.POST(fmt.Sprintf("/:%s%s", dsKey, constants.RootPath), s.makeHandle(HandleRootPost))
	router.POST(fmt.Sprintf("/:%s%s", dsKey, constants.PostRefsPath), s.makeHandle(HandlePostRefs))
	router.POST(fmt.Sprintf("/:%s%s", dsKey, constants.WriteValuePath), s.makeHandle(HandleWriteValue))

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

func (s *remoteDatabaseServer) makeHandle(hndlr Handler) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		cs := expectStoreCreate(s.csf, ps)
		defer cs.Close()
		hndlr(w, req, ps, cs)
	}
}

func expectStoreCreate(f chunks.Factory, ps httprouter.Params) (cs chunks.ChunkStore) {
	cs = f.CreateStore(ps.ByName("database"))
	d.Exp.NotNil(cs, "Failed to create database named %s", ps.ByName("database"))
	return
}

func (s *remoteDatabaseServer) connState(c net.Conn, cs http.ConnState) {
	if s.closing {
		d.Chk.Equal(cs, http.StateClosed)
		return
	}
	s.csChan <- &connectionState{c, cs}
}

// Will cause the remoteDatabaseServer to stop listening and an existing call to Run() to continue.
func (s *remoteDatabaseServer) Stop() {
	s.closing = true
	(*s.l).Close()
	close(s.csChan)
}
