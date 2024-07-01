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

type ReqClientStream[Req, Resp any] interface {
	ClientStream[Req, Resp]

	// After a successful |Recv| call, calling |AssociatedReq| returns the
	// request which was associated with the last returned response. This
	// is only safe to do from the same goroutine which is calling |Recv|.
	AssociatedReq() Req
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

type reqResp[Req, Resp any] struct {
	Req  Req
	Resp Resp
}

func MakeCall[Req, Resp any](ctx context.Context, opts CallOptions[Req, Resp]) (ReqClientStream[Req, Resp], error) {
	eg, ctx := errgroup.WithContext(ctx)
	ret := &reliableCall[Req, Resp]{
		eg:     eg,
		ctx:    ctx,
		reqCh:  make(chan Req),
		respCh: make(chan reqResp[Req, Resp]),
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
	respCh chan reqResp[Req, Resp]

	opts CallOptions[Req, Resp]

	associatedReq Req
}

func (c *reliableCall[Req, Resp]) thread() error {
	bo := c.opts.BackOffF(c.ctx)

	requests := NewChan(c.reqCh)
	defer requests.Close()

	sm := &reliableCallStateMachine[Req, Resp]{
		call:     c,
		requests: requests,
		bo:       bo,
	}

	return sm.run(c.ctx)
}

func (c *reliableCall[Req, Resp]) Send(r Req) error {
	select {
	case c.reqCh <- r:
		return nil
	case <-c.ctx.Done():
		// Always non-nil. We never closed reqCh, so the only way
		// sm.run() should complete is with a non-nil error.
		return c.eg.Wait()
	}
}

func (c *reliableCall[Req, Resp]) CloseSend() error {
	close(c.reqCh)
	return nil
}

func (c *reliableCall[Req, Resp]) Recv() (Resp, error) {
	var r reqResp[Req, Resp]
	var ok bool
	select {
	case r, ok = <-c.respCh:
		if !ok {
			// Always |nil|. If respCh is closed, run() should
			// always return nil error.
			_ = c.eg.Wait()
			return r.Resp, io.EOF
		}
		c.associatedReq = r.Req
		return r.Resp, nil
	case <-c.ctx.Done():
		err := c.eg.Wait()
		if err == nil {
			// The parent context can be canceled while the state
			// machine is actually finished successfully and we
			// have not read the close of c.respCh yet. If err is
			// nil here, then we technically know that we are at
			// io.EOF.
			//
			// It would also be correct to return
			// context.Cause(ctx) here, since any behavior relying
			// on observing one or the other is racey. But it is
			// *not* correct to return a default-value Resp and a
			// |nil| error here, which is what you get with just
			// |return r.Resp, c.eg.Wait()|.
			return r.Resp, io.EOF
		}
		return r.Resp, err
	}
}

func (c *reliableCall[Req, Resp]) AssociatedReq() Req {
	return c.associatedReq
}
