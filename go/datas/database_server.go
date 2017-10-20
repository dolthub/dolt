// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/d"
	"github.com/julienschmidt/httprouter"
)

type connectionState struct {
	c  net.Conn
	cs http.ConnState
}

type RemoteDatabaseServer struct {
	cs      chunks.ChunkStore
	port    int
	l       *net.Listener
	csChan  chan *connectionState
	closing bool
	// Called just before the server is started.
	Ready func()
}

func NewRemoteDatabaseServer(cs chunks.ChunkStore, port int) *RemoteDatabaseServer {
	dataVersion := cs.Version()
	if constants.NomsVersion != dataVersion {
		d.Panic("SDK version %s is incompatible with data of version %s", constants.NomsVersion, dataVersion)
	}
	return &RemoteDatabaseServer{
		cs, port, nil, make(chan *connectionState, 16), false, func() {},
	}
}

// Port is the actual port used. This may be different than the port passed in to NewRemoteDatabaseServer.
func (s *RemoteDatabaseServer) Port() int {
	return s.port
}

// Run blocks while the RemoteDatabaseServer is listening. Running on a separate go routine is supported.
func (s *RemoteDatabaseServer) Run() {

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	d.Chk.NoError(err)
	s.l = &l
	_, port, err := net.SplitHostPort(l.Addr().String())
	d.Chk.NoError(err)
	s.port, err = strconv.Atoi(port)
	d.Chk.NoError(err)
	log.Printf("Listening on port %d...\n", s.port)

	router := httprouter.New()

	router.POST(constants.GetRefsPath, s.corsHandle(s.makeHandle(HandleGetRefs)))
	router.GET(constants.GetBlobPath, s.corsHandle(s.makeHandle(HandleGetBlob)))
	router.OPTIONS(constants.GetRefsPath, s.corsHandle(noopHandle))
	router.POST(constants.HasRefsPath, s.corsHandle(s.makeHandle(HandleHasRefs)))
	router.OPTIONS(constants.HasRefsPath, s.corsHandle(noopHandle))
	router.GET(constants.RootPath, s.corsHandle(s.makeHandle(HandleRootGet)))
	router.POST(constants.RootPath, s.corsHandle(s.makeHandle(HandleRootPost)))
	router.OPTIONS(constants.RootPath, s.corsHandle(noopHandle))
	router.POST(constants.WriteValuePath, s.corsHandle(s.makeHandle(HandleWriteValue)))
	router.OPTIONS(constants.WriteValuePath, s.corsHandle(noopHandle))
	router.GET(constants.BasePath, s.corsHandle(s.makeHandle(HandleBaseGet)))

	router.GET(constants.GraphQLPath, s.corsHandle(s.makeHandle(HandleGraphQL)))
	router.POST(constants.GraphQLPath, s.corsHandle(s.makeHandle(HandleGraphQL)))
	router.OPTIONS(constants.GraphQLPath, s.corsHandle(noopHandle))

	router.GET(constants.StatsPath, s.corsHandle(s.makeHandle(HandleStats)))
	router.OPTIONS(constants.StatsPath, s.corsHandle(noopHandle))

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
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

	go s.Ready()
	srv.Serve(l)
}

func (s *RemoteDatabaseServer) makeHandle(hndlr Handler) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		hndlr(w, req, ps, s.cs)
	}
}

func noopHandle(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
}

func (s *RemoteDatabaseServer) corsHandle(f httprouter.Handle) httprouter.Handle {
	// TODO: Implement full pre-flighting?
	// See: http://www.html5rocks.com/static/images/cors_server_flowchart.png
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		// Can't use * when clients are using cookies.
		w.Header().Add("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		w.Header().Add("Access-Control-Allow-Methods", "GET, POST")
		w.Header().Add("Access-Control-Allow-Headers", NomsVersionHeader)
		w.Header().Add("Access-Control-Expose-Headers", NomsVersionHeader)
		w.Header().Add(NomsVersionHeader, constants.NomsVersion)
		f(w, r, ps)
	}
}

func (s *RemoteDatabaseServer) connState(c net.Conn, cs http.ConnState) {
	if s.closing {
		d.PanicIfFalse(cs == http.StateClosed)
		return
	}
	s.csChan <- &connectionState{c, cs}
}

// Will cause the RemoteDatabaseServer to stop listening and an existing call to Run() to continue.
func (s *RemoteDatabaseServer) Stop() {
	s.closing = true
	(*s.l).Close()
	(s.cs).Close()
	close(s.csChan)
}
