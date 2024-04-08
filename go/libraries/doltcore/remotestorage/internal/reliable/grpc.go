// Copyright 2024 Dolthub, Inc.
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

package reliable

import (
	"context"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type ClientStream[Req, Resp any] interface {
	Send(Req) error
	Recv() (Resp, error)
	CloseSend() error
}

type OpenStreamFunc[Req, Resp any] func(context.Context, ...grpc.CallOption) (ClientStream[Req, Resp], error)

type CallOptions[Req, Resp any] struct {
	Open     OpenStreamFunc[Req, Resp]
	GrpcOpts []grpc.CallOption
	ErrF     func(error) error
	BackOffF func(context.Context) backoff.BackOff

	ReadRequestTimeout time.Duration
	DeliverRespTimeout time.Duration
}

func MakeCall[Req, Resp any](ctx context.Context, opts CallOptions[Req, Resp]) (ClientStream[Req, Resp], error) {
	eg, ctx := errgroup.WithContext(ctx)
	ret := &reliableCall[Req, Resp]{
		eg:     eg,
		ctx:    ctx,
		reqCh:  make(chan Req),
		respCh: make(chan Resp),
		opts:   opts,
	}
	eg.Go(func() error {
		return ret.thread()
	})
	return ret, nil
}

type reliableCall[Req, Resp any] struct {
	eg  *errgroup.Group
	ctx context.Context

	reqCh  chan Req
	respCh chan Resp

	opts CallOptions[Req, Resp]
}

func (c *reliableCall[Req, Resp]) thread() error {
	bo := c.opts.BackOffF(c.ctx)

	requests := NewChan(c.reqCh)
	defer requests.Close()

	sm := &reliableCallStateMachine[Req, Resp]{
		call: c,
		requests: requests,
		bo: bo,
	}

	return sm.run(c.ctx)
}

func (c *reliableCall[Req, Resp]) Send(r Req) error {
	select {
	case c.reqCh <- r:
		return nil
	case <-c.ctx.Done():
		return c.eg.Wait()
	}
}

func (c *reliableCall[Req, Resp]) CloseSend() error {
	close(c.reqCh)
	return nil
}

func (c *reliableCall[Req, Resp]) Recv() (Resp, error) {
	var r Resp
	var ok bool
	select {
	case r, ok = <-c.respCh:
		if !ok {
			return r, io.EOF
		}
		return r, nil
	case <-c.ctx.Done():
		return r, c.eg.Wait()
	}
}
