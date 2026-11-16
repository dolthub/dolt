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

// Package grpcreresolve provides a gRPC load-balancing policy that
// asks gRPC to re-resolve the endpoint on Unavailable and
// DeadlineExceeded responses. This fixes a potential sticky-fault
// scenario when behind an L7 proxy or service mesh (e.g. Istio in
// K8s), where gRPC's default resolver only re-resolves on transport
// level failures and the service mesh side car does not terminate the
// connection when delivering the no-upstream response. When an
// upstream moves or dies, a gRPC ClientConn can be left with a
// SubChannel indefinitely resolved to the stale IP address.
//
// Implemented as a thin wrapper over pick_first which uses the Done
// signal to call ClientConn.ResolveNow when we see failures. dns
// resolver ResolveNow already de-bounces resolution requests.
package grpcreresolve

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/pickfirst"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/status"
)

// Name is the name under which this load-balancing policy is registered. Select
// it on a ClientConn with grpc.WithDefaultServiceConfig(ServiceConfigJSON).
const Name = "dolt_reresolve_pick_first"

// defaultFailureThreshold is the number of consecutive connectivity
// failures (see isConnectivityFailure) that triggers a
// re-resolution. See isConnectivityFailure.  Because the resolver
// already debounces ResolveNow requests, it is better to be
// responsive here rather than conservative.
const defaultFailureThreshold = 1

// ServiceConfigJSON selects this policy. Pass it to
// grpc.WithDefaultServiceConfig when dialing.
const ServiceConfigJSON = `{"loadBalancingConfig":[{"` + Name + `":{}}]}`

func init() {
	balancer.Register(builder{})
}

type builder struct{}

func (builder) Name() string { return Name }

func (builder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	b := &reResolveBalancer{cc: cc}
	b.threshold.Store(defaultFailureThreshold)
	// The child is a stock pick_first. We hand it a wrapped ClientConn so we can
	// intercept the pickers it publishes; everything else passes through.
	wrapped := &ccWrapper{ClientConn: cc, b: b}
	b.Balancer = balancer.Get(pickfirst.Name).Build(wrapped, opts)
	return b
}

// lbConfig is the parsed form of this policy's service-config entry. The only
// knob is an optional failureThreshold override.
type lbConfig struct {
	serviceconfig.LoadBalancingConfig `json:"-"`
	FailureThreshold                  uint32 `json:"failureThreshold,omitempty"`
}

func (builder) ParseConfig(j json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	cfg := &lbConfig{}
	if err := json.Unmarshal(j, cfg); err != nil {
		return nil, fmt.Errorf("%s: invalid load balancing config %q: %w", Name, string(j), err)
	}
	return cfg, nil
}

// reResolveBalancer embeds a child pick_first balancer and adds failure-driven
// re-resolution. All balancer.Balancer methods except UpdateClientConnState are
// satisfied by the embedded child.
type reResolveBalancer struct {
	balancer.Balancer

	cc        balancer.ClientConn
	threshold atomic.Uint32
	failures  atomic.Uint32
}

func (b *reResolveBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	threshold := uint32(defaultFailureThreshold)
	if cfg, ok := s.BalancerConfig.(*lbConfig); ok && cfg.FailureThreshold > 0 {
		threshold = cfg.FailureThreshold
	}
	b.threshold.Store(threshold)

	// pick_first rejects any non-nil BalancerConfig it does not
	// recognize.  Let it run with default behavior.
	s.BalancerConfig = nil
	return b.Balancer.UpdateClientConnState(s)
}

// onRPCDone is called for every completed RPC picked through this balancer's
// picker. It counts consecutive connectivity failures and asks gRPC to
// re-resolve once the threshold is reached.
func (b *reResolveBalancer) onRPCDone(err error) {
	if err == nil {
		b.failures.Store(0)
		return
	}
	if !isConnectivityFailure(err) {
		// A non-connectivity outcome (an application-level code) does not signal a
		// moved endpoint, but neither does it signal health: leave the consecutive
		// streak untouched so interleaved application errors don't mask a genuine
		// run of connectivity failures.
		return
	}
	if b.failures.Add(1) >= b.threshold.Load() {
		b.failures.Store(0)
		b.cc.ResolveNow(resolver.ResolveNowOptions{})
	}
}

// isConnectivityFailure reports whether err is the kind of failure that, behind
// an L7 proxy / service mesh, indicates the endpoint may have moved and a
// re-resolution is warranted.
//
// Use trigger on both Unavailable and DeadlineExceeded because a mesh can
// surface a stale endpoint in two phases: an RPC to a dead ORIGINAL_DST first
// blackholes and trips the caller's deadline (DeadlineExceeded), and only once
// the sidecar's endpoint discovery drops the gone upstream does it return
// 503 / "upstream request timeout" (Unavailable).
func isConnectivityFailure(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// ccWrapper intercepts the pickers published by the child balancer so we can
// observe RPC outcomes. Every other ClientConn method is inherited unchanged
// from the embedded real ClientConn.
type ccWrapper struct {
	balancer.ClientConn
	b *reResolveBalancer
}

func (w *ccWrapper) UpdateState(s balancer.State) {
	if s.Picker != nil {
		s.Picker = &picker{Picker: s.Picker, b: w.b}
	}
	w.ClientConn.UpdateState(s)
}

// picker delegates selection to the child picker and wraps the result's Done
// callback to feed RPC outcomes back to the balancer.
type picker struct {
	balancer.Picker
	b *reResolveBalancer
}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	res, err := p.Picker.Pick(info)
	if err != nil {
		return res, err
	}
	orig := res.Done
	res.Done = func(di balancer.DoneInfo) {
		if orig != nil {
			orig(di)
		}
		p.b.onRPCDone(di.Err)
	}
	return res, nil
}
