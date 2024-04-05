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
	"errors"
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

type CtxStateFunc func(context.Context) (CtxStateFunc, error)

type ErrWantBlockForDeliverResp[Resp any] struct {
	Resp Resp
}

func (ErrWantBlockForDeliverResp[Resp]) Error() string {
	return "ErrWantBlockForDeliverResp"
}

func (c *reliableCall[Req, Resp]) thread() error {
	var state_Initial CtxStateFunc
	var state_Open CtxStateFunc
	var state_Backoff func(time.Duration) CtxStateFunc
	var state_BlockForDeliverResp func(Resp) CtxStateFunc

	bo := c.opts.BackOffF(c.ctx)

	requests := NewChan(c.reqCh)
	defer requests.Close()

	stateForError := func(err error) (CtxStateFunc, error) {
		err = c.opts.ErrF(err)
		pe := new(backoff.PermanentError)
		if errors.As(err, &pe) {
			return nil, pe.Err
		}
		duration := bo.NextBackOff()
		if duration == backoff.Stop {
			return nil, err
		}
		return state_Backoff(duration), nil
	}

	state_BlockForDeliverResp = func(resp Resp) CtxStateFunc {
		return func(ctx context.Context) (CtxStateFunc, error) {
			select {
			case c.respCh <- resp:
				requests.Ack()
				requests.Reset()
				return state_Initial, nil
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			}
		}
	}

	state_Backoff = func(duration time.Duration) CtxStateFunc {
		return func(ctx context.Context) (CtxStateFunc, error) {
			select {
			case <-time.After(duration):
				return state_Initial, nil
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			}
		}
	}

	state_Initial = func(ctx context.Context) (CtxStateFunc, error) {
		select {
		case _, ok := <-requests.Recv():
			if !ok {
				return nil, nil
			}
			// The easiest way to drive the just-received request
			// in the Open state is to just reset the reliable
			// channel here.
			requests.Reset()
			return state_Open, nil
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}

	state_Open = func(ctx context.Context) (CtxStateFunc, error) {
		eg, sCtx := errgroup.WithContext(ctx)
		stream, err := c.opts.Open(sCtx, c.opts.GrpcOpts...)
		if err != nil {
			return stateForError(err)
		}
		var nextState CtxStateFunc
		// Handle requests
		eg.Go(func() error {
			timeout := time.NewTimer(c.opts.ReadRequestTimeout)
			for {
				if !timeout.Stop() {
					<-timeout.C
				}
				timeout.Reset(c.opts.ReadRequestTimeout)
				select {
				case req, ok := <-requests.Recv():
					if !ok {
						return stream.CloseSend()
					}
					err := stream.Send(req)
					if err != nil {
						return err
					}
				case <-sCtx.Done():
					return context.Cause(sCtx)
				case <-timeout.C:
					nextState = state_Initial
					return stream.CloseSend()
				}
			}
		})
		// Handle responses
		eg.Go(func() error {
			timeout := time.NewTimer(c.opts.DeliverRespTimeout)
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if !timeout.Stop() {
					<-timeout.C
				}
				timeout.Reset(c.opts.DeliverRespTimeout)
				select {
				case <-sCtx.Done():
					return context.Cause(sCtx)
				case c.respCh <- resp:
					requests.Ack()
				case <-timeout.C:
					// We signal this next state with an error, since we need the
					// (possibly-blocked-on-network-sending) Send thread to see
					// failure by having its context canceled.
					return ErrWantBlockForDeliverResp[Resp]{Resp: resp}
				}
			}
		})
		var dre ErrWantBlockForDeliverResp[Resp]
		err = eg.Wait()
		if err == nil {
			requests.Reset()
			return nextState, nil
		} else if errors.As(err, &dre) {
			// We do not reset the reliable Chan of requsets here.
			// Once this response is delivered, it will be Ackd and
			// the channel will be reset by the next state.
			return state_BlockForDeliverResp(dre.Resp), nil
		} else {
			requests.Reset()
			return stateForError(err)
		}
	}

	var currentState = state_Initial

	for {
		nextState, err := currentState(c.ctx)
		if err != nil {
			return err
		}
		if nextState == nil {
			close(c.respCh)
			return nil
		}
		currentState = nextState
	}
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
