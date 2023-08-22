// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remotesrv

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type Server struct {
	wg       sync.WaitGroup
	stopChan chan struct{}

	grpcListenAddr string
	httpListenAddr string

	grpcSrv *grpc.Server
	httpSrv http.Server

	tlsConfig *tls.Config
}

func (s *Server) GracefulStop() {
	close(s.stopChan)
	s.wg.Wait()
}

type ServerArgs struct {
	Logger   *logrus.Entry
	HttpHost string

	HttpListenAddr string
	GrpcListenAddr string

	FS       filesys.Filesys
	DBCache  DBCache
	ReadOnly bool
	Options  []grpc.ServerOption

	HttpInterceptor func(http.Handler) http.Handler

	// If supplied, the listener(s) returned from Listeners() will be TLS
	// listeners. The scheme used in the URLs returned from the gRPC server
	// will be https.
	TLSConfig *tls.Config
}

func NewServer(args ServerArgs) (*Server, error) {
	if args.Logger == nil {
		args.Logger = logrus.NewEntry(logrus.StandardLogger())
	}

	s := new(Server)
	s.stopChan = make(chan struct{})

	sealer, err := NewSingleSymmetricKeySealer()
	if err != nil {
		return nil, err
	}

	scheme := "http"
	if args.TLSConfig != nil {
		scheme = "https"
	}
	s.tlsConfig = args.TLSConfig

	s.wg.Add(2)
	s.grpcListenAddr = args.GrpcListenAddr
	s.grpcSrv = grpc.NewServer(append([]grpc.ServerOption{grpc.MaxRecvMsgSize(128 * 1024 * 1024)}, args.Options...)...)
	var chnkSt remotesapi.ChunkStoreServiceServer = NewHttpFSBackedChunkStore(args.Logger, args.HttpHost, args.DBCache, args.FS, scheme, sealer)
	if args.ReadOnly {
		chnkSt = ReadOnlyChunkStore{chnkSt}
	}
	remotesapi.RegisterChunkStoreServiceServer(s.grpcSrv, chnkSt)

	var handler http.Handler = newFileHandler(args.Logger, args.DBCache, args.FS, args.ReadOnly, sealer)
	if args.HttpInterceptor != nil {
		handler = args.HttpInterceptor(handler)
	}
	if args.HttpListenAddr == args.GrpcListenAddr {
		handler = grpcMultiplexHandler(s.grpcSrv, handler)
	} else {
		s.wg.Add(2)
	}

	s.httpListenAddr = args.HttpListenAddr
	s.httpSrv = http.Server{
		Addr:    args.HttpListenAddr,
		Handler: handler,
	}

	return s, nil
}

func grpcMultiplexHandler(grpcSrv *grpc.Server, handler http.Handler) http.Handler {
	h2s := &http2.Server{}
	newHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcSrv.ServeHTTP(w, r)
		} else {
			handler.ServeHTTP(w, r)
		}
	})
	return h2c.NewHandler(newHandler, h2s)
}

type Listeners struct {
	http net.Listener
	grpc net.Listener
}

func (s *Server) Listeners() (Listeners, error) {
	var httpListener net.Listener
	var grpcListener net.Listener
	var err error
	if s.tlsConfig != nil {
		httpListener, err = tls.Listen("tcp", s.httpListenAddr, s.tlsConfig)
	} else {
		httpListener, err = net.Listen("tcp", s.httpListenAddr)
	}
	if err != nil {
		return Listeners{}, err
	}
	if s.httpListenAddr == s.grpcListenAddr {
		return Listeners{http: httpListener}, nil
	}
	if s.tlsConfig != nil {
		grpcListener, err = tls.Listen("tcp", s.grpcListenAddr, s.tlsConfig)
	} else {
		grpcListener, err = net.Listen("tcp", s.grpcListenAddr)
	}
	if err != nil {
		httpListener.Close()
		return Listeners{}, err
	}
	return Listeners{http: httpListener, grpc: grpcListener}, nil
}

// Can be used to register more services on the server.
// Should only be accessed before `Serve` is called.
func (s *Server) GrpcServer() *grpc.Server {
	return s.grpcSrv
}

func (s *Server) Serve(listeners Listeners) {
	if listeners.grpc != nil {
		go func() {
			defer s.wg.Done()
			logrus.Println("Starting grpc server on", s.grpcListenAddr)
			err := s.grpcSrv.Serve(listeners.grpc)
			logrus.Println("grpc server exited. error:", err)
		}()
		go func() {
			defer s.wg.Done()
			<-s.stopChan
			s.grpcSrv.GracefulStop()
		}()
	}

	go func() {
		defer s.wg.Done()
		logrus.Println("Starting http server on", s.httpListenAddr)
		err := s.httpSrv.Serve(listeners.http)
		logrus.Println("http server exited. exit error:", err)
	}()
	go func() {
		defer s.wg.Done()
		<-s.stopChan
		s.httpSrv.Shutdown(context.Background())
	}()

	s.wg.Wait()
}
