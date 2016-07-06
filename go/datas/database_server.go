// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"fmt"
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

type remoteDatabaseServer struct {
	cs      chunks.ChunkStore
	port    int
	l       *net.Listener
	csChan  chan *connectionState
	closing bool
}

func NewRemoteDatabaseServer(cs chunks.ChunkStore, port int) *remoteDatabaseServer {
	dataVersion := cs.Version()
	d.PanicIfTrue(constants.NomsVersion != dataVersion, "SDK version %s is incompatible with data of version %s", constants.NomsVersion, dataVersion)
	return &remoteDatabaseServer{
		cs, port, nil, make(chan *connectionState, 16), false,
	}
}

// Run blocks while the remoteDatabaseServer is listening. Running on a separate go routine is supported.
func (s *remoteDatabaseServer) Run() {

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	d.Chk.NoError(err)
	s.l = &l
	_, port, err := net.SplitHostPort(l.Addr().String())
	d.Chk.NoError(err)
	s.port, err = strconv.Atoi(port)
	d.Chk.NoError(err)
	fmt.Printf("Listening on port %d...\n", s.port)

	router := httprouter.New()

	router.POST(constants.GetRefsPath, s.makeHandle(HandleGetRefs))
	router.POST(constants.HasRefsPath, s.makeHandle(HandleHasRefs))
	router.GET(constants.RootPath, s.makeHandle(HandleRootGet))
	router.POST(constants.RootPath, s.makeHandle(HandleRootPost))
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

func (s *remoteDatabaseServer) makeHandle(hndlr Handler) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		hndlr(w, req, ps, s.cs)
	}
}

func (s *remoteDatabaseServer) connState(c net.Conn, cs http.ConnState) {
	if s.closing {
		d.Chk.True(cs == http.StateClosed)
		return
	}
	s.csChan <- &connectionState{c, cs}
}

// Will cause the remoteDatabaseServer to stop listening and an existing call to Run() to continue.
func (s *remoteDatabaseServer) Stop() {
	s.closing = true
	(*s.l).Close()
	(s.cs).Close()
	close(s.csChan)
}
