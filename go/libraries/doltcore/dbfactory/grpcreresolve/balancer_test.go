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

package grpcreresolve

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeCC is a minimal balancer.ClientConn that only records ResolveNow calls.
type fakeCC struct {
	balancer.ClientConn
	resolveNow atomic.Int32
}

func (f *fakeCC) ResolveNow(resolver.ResolveNowOptions) { f.resolveNow.Add(1) }

func TestOnRPCDoneThresholdAndReset(t *testing.T) {
	cc := &fakeCC{}
	b := &reResolveBalancer{cc: cc}
	b.threshold.Store(3)

	unavail := func() { b.onRPCDone(status.Error(codes.Unavailable, "down")) }

	// Below the threshold: no re-resolve.
	unavail()
	unavail()
	require.Equal(t, int32(0), cc.resolveNow.Load())

	// Hitting the threshold fires exactly one ResolveNow and resets the counter.
	unavail()
	require.Equal(t, int32(1), cc.resolveNow.Load())

	unavail()
	unavail()
	require.Equal(t, int32(1), cc.resolveNow.Load())
	unavail()
	require.Equal(t, int32(2), cc.resolveNow.Load())

	// A success resets the consecutive-failure streak.
	unavail()
	unavail()
	b.onRPCDone(nil)
	unavail()
	require.Equal(t, int32(2), cc.resolveNow.Load(), "success should have reset the streak")
}

func TestOnRPCDoneIgnoresApplicationCodes(t *testing.T) {
	cc := &fakeCC{}
	b := &reResolveBalancer{cc: cc}
	b.threshold.Store(2)

	// Application-level codes must never trigger re-resolution, no matter how many
	// of them arrive.
	b.onRPCDone(status.Error(codes.NotFound, "missing"))
	b.onRPCDone(status.Error(codes.PermissionDenied, "no"))
	b.onRPCDone(status.Error(codes.Canceled, "ctx"))
	b.onRPCDone(status.Error(codes.InvalidArgument, "bad"))
	require.Equal(t, int32(0), cc.resolveNow.Load())
}

func TestOnRPCDoneCountsConnectivityFailures(t *testing.T) {
	// Both codes.Unavailable and codes.DeadlineExceeded are connectivity failures:
	// behind a mesh a stale endpoint first trips the deadline, then turns into
	// Unavailable, and we want recovery to begin in the first phase.
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			cc := &fakeCC{}
			b := &reResolveBalancer{cc: cc}
			b.threshold.Store(2)

			b.onRPCDone(status.Error(code, "fail"))
			require.Equal(t, int32(0), cc.resolveNow.Load())
			b.onRPCDone(status.Error(code, "fail"))
			require.Equal(t, int32(1), cc.resolveNow.Load())
		})
	}
}

func TestOnRPCDoneMixedConnectivityFailures(t *testing.T) {
	// A run of connectivity failures counts even when the codes alternate, and an
	// interleaved application error neither counts nor resets the streak.
	cc := &fakeCC{}
	b := &reResolveBalancer{cc: cc}
	b.threshold.Store(3)

	b.onRPCDone(status.Error(codes.DeadlineExceeded, "blackhole"))
	b.onRPCDone(status.Error(codes.NotFound, "app error, ignored"))
	b.onRPCDone(status.Error(codes.Unavailable, "upstream request timeout"))
	require.Equal(t, int32(0), cc.resolveNow.Load())
	b.onRPCDone(status.Error(codes.Unavailable, "upstream request timeout"))
	require.Equal(t, int32(1), cc.resolveNow.Load())
}

func TestParseConfig(t *testing.T) {
	cfg, err := builder{}.ParseConfig([]byte(`{}`))
	require.NoError(t, err)
	require.Equal(t, uint32(0), cfg.(*lbConfig).FailureThreshold)

	cfg, err = builder{}.ParseConfig([]byte(`{"failureThreshold":5}`))
	require.NoError(t, err)
	require.Equal(t, uint32(5), cfg.(*lbConfig).FailureThreshold)

	_, err = builder{}.ParseConfig([]byte(`{bad`))
	require.Error(t, err)
}

// recordingBalancer captures the ClientConnState handed to the child.
type recordingBalancer struct {
	balancer.Balancer
	last balancer.ClientConnState
}

func (r *recordingBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	r.last = s
	return nil
}

func TestUpdateClientConnStateStripsConfigAndSetsThreshold(t *testing.T) {
	child := &recordingBalancer{}
	b := &reResolveBalancer{cc: &fakeCC{}}
	b.Balancer = child

	// A config with a threshold override is applied to us and stripped before
	// reaching the child pick_first (which rejects configs it doesn't recognize).
	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{BalancerConfig: &lbConfig{FailureThreshold: 7}}))
	require.Nil(t, child.last.BalancerConfig)
	require.Equal(t, uint32(7), b.threshold.Load())

	// With no config we fall back to the default threshold.
	b.threshold.Store(99)
	require.NoError(t, b.UpdateClientConnState(balancer.ClientConnState{}))
	require.Equal(t, uint32(defaultFailureThreshold), b.threshold.Load())
}

type unavailableHealth struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (unavailableHealth) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	// Connectable server whose RPCs fail at L7 — mimics an Envoy sidecar that
	// stays up while the real upstream is gone. The subchannel stays READY, so
	// stock gRPC never re-resolves on its own.
	return nil, status.Error(codes.Unavailable, "upstream unavailable")
}

type servingHealth struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (servingHealth) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func serveHealth(t *testing.T, impl grpc_health_v1.HealthServer) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis
}

// TestReResolveRecoversFromMovedEndpoint reproduces the mesh mechanism end to
// end: server A is reachable (subchannel READY) but every RPC returns
// Unavailable, so stock gRPC would never re-resolve. Our policy observes the
// failures and calls ResolveNow; the resolver then returns server B and
// pick_first switches to it.
func TestReResolveRecoversFromMovedEndpoint(t *testing.T) {
	const addrA, addrB = "server-a", "server-b"

	aLis := serveHealth(t, unavailableHealth{})
	bLis := serveHealth(t, servingHealth{})

	dialer := func(ctx context.Context, s string) (net.Conn, error) {
		switch s {
		case addrA:
			return aLis.DialContext(ctx)
		case addrB:
			return bLis.DialContext(ctx)
		}
		return nil, fmt.Errorf("unknown address %q", s)
	}

	r := manual.NewBuilderWithScheme("grpcreresolvetest")
	r.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: addrA}}})

	var allowSwitch atomic.Bool
	var switched atomic.Bool
	var resolveNowCount atomic.Int32
	r.ResolveNowCallback = func(resolver.ResolveNowOptions) {
		resolveNowCount.Add(1)
		if allowSwitch.Load() && switched.CompareAndSwap(false, true) {
			// Mirror the dns resolver: deliver the new state asynchronously.
			go r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: addrB}}})
		}
	}

	cc, err := grpc.NewClient(
		r.Scheme()+":///ignored",
		grpc.WithResolvers(r),
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(ServiceConfigJSON),
	)
	require.NoError(t, err)
	defer cc.Close()

	client := grpc_health_v1.NewHealthClient(cc)
	check := func() (*grpc_health_v1.HealthCheckResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	}

	// Phase 1: only A exists and switching is disabled. Confirm RPCs actually
	// reach A and fail Unavailable, proving the subchannel is READY (so stock
	// gRPC is not re-resolving) before we test our trigger.
	sawUnavailable := false
	for i := 0; i < 50 && !sawUnavailable; i++ {
		if _, err := check(); status.Code(err) == codes.Unavailable {
			sawUnavailable = true
		}
	}
	require.True(t, sawUnavailable, "expected RPCs to reach server A and return Unavailable")

	// Phase 2: allow the endpoint to move. Our policy must drive ResolveNow,
	// the resolver returns B, and pick_first switches so RPCs succeed.
	allowSwitch.Store(true)
	var lastErr error
	served := false
	for i := 0; i < 100 && !served; i++ {
		resp, err := check()
		lastErr = err
		if err == nil && resp.Status == grpc_health_v1.HealthCheckResponse_SERVING {
			served = true
		}
	}
	require.True(t, served, "expected recovery to server B; last err: %v", lastErr)
	require.Greater(t, resolveNowCount.Load(), int32(0), "our policy should have triggered ResolveNow")
}
