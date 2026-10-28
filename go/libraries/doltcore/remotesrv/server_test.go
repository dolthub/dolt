// Copyright 2026 Dolthub, Inc.
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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/types/known/emptypb"
)

// slowConnEndHandler keeps multiplexed grpcSrv.ServeHTTP calls from
// slowAddr in flight for a while after their transport is closed, so
// that shutdown has to wait on them while other requests keep
// arriving.
type slowConnEndHandler struct {
	slowAddr *atomic.Pointer[string]
}

type remoteAddrKey struct{}

func (slowConnEndHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (slowConnEndHandler) HandleRPC(context.Context, stats.RPCStats) {}

func (slowConnEndHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, remoteAddrKey{}, info.RemoteAddr.String())
}

func (h slowConnEndHandler) HandleConn(ctx context.Context, s stats.ConnStats) {
	if _, ok := s.(*stats.ConnEnd); ok {
		if slow := h.slowAddr.Load(); slow != nil && ctx.Value(remoteAddrKey{}) == *slow {
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// When HTTP and gRPC are multiplexed on one listener, httpSrv keeps
// accepting gRPC requests while GracefulStop stops grpcSrv and waits
// for in-flight gRPC requests to finish. Those new requests must
// neither keep grpcSrv.Stop() from returning nor race the wait.
func TestMultiplexedGracefulStopWithConcurrentGRPCRequests(t *testing.T) {
	for i := 0; i < 32; i++ {
		var slowAddr atomic.Pointer[string]
		srv, err := NewServer(ServerArgs{
			HttpListenAddr: "127.0.0.1:0",
			GrpcListenAddr: "127.0.0.1:0",
			Options: []grpc.ServerOption{
				grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
					var m emptypb.Empty
					if err := stream.RecvMsg(&m); err != nil {
						return err
					}
					return stream.SendMsg(&m)
				}),
				grpc.StatsHandler(slowConnEndHandler{&slowAddr}),
			},
		})
		require.NoError(t, err)
		listeners, err := srv.Listeners()
		require.NoError(t, err)
		require.Nil(t, listeners.grpc)
		addr := listeners.http.Addr().String()

		served := make(chan struct{})
		go func() {
			defer close(served)
			srv.Serve(listeners)
		}()

		slowConn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				c, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
				if err == nil {
					local := c.LocalAddr().String()
					slowAddr.Store(&local)
				}
				return c, err
			}))
		require.NoError(t, err)
		fastConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		invoke := func(conn *grpc.ClientConn) error {
			// Any method works; UnknownServiceHandler echoes.
			return conn.Invoke(ctx, "/test.Service/Method", &emptypb.Empty{}, &emptypb.Empty{})
		}
		// Make sure the connections are up before we start stopping.
		require.NoError(t, invoke(slowConn))
		require.NoError(t, invoke(fastConn))
		var wg sync.WaitGroup
		for j := 0; j < 16; j++ {
			conn := fastConn
			if j%4 == 0 {
				conn = slowConn
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ctx.Err() == nil {
					_ = invoke(conn)
				}
			}()
		}
		time.Sleep(10 * time.Millisecond)

		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			srv.GracefulStop()
		}()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Fatal("GracefulStop did not return while gRPC requests kept arriving")
		}
		<-served
		cancel()
		wg.Wait()
		require.NoError(t, slowConn.Close())
		require.NoError(t, fastConn.Close())
	}
}
