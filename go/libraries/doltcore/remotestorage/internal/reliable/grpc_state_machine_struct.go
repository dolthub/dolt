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
	"fmt"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"
)

type CtxStateFunc func(context.Context) (CtxStateFunc, error)

type ErrWantBlockForDeliverResp[Resp any] struct {
	Resp Resp
}

func (ErrWantBlockForDeliverResp[Resp]) Error() string {
	return "ErrWantBlockForDeliverResp"
}

type reliableCallStateMachine[Req, Resp any] struct {
	call     *reliableCall[Req, Resp]
	requests *Chan[Req]
	bo       backoff.BackOff

	// For backoff state.
	duration time.Duration

	// For block for delivery state
	resp Resp
}

func (s *reliableCallStateMachine[Req, Resp]) run(ctx context.Context) error {
	var curState CtxStateFunc = s.initial
	for {
		nextState, err := curState(ctx)
		if err != nil {
			return err
		}
		if nextState == nil {
			close(s.call.respCh)
			return nil
		}
		curState = nextState
	}

}

func (s *reliableCallStateMachine[Req, Resp]) initial(ctx context.Context) (CtxStateFunc, error) {
	select {
	case _, ok := <-s.requests.Recv():
		if !ok {
			return nil, nil
		}
		// The easiest way to drive the just-received request
		// in the Open state is to just reset the reliable
		// channel here.
		s.requests.Reset()
		return s.open, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (s *reliableCallStateMachine[Req, Resp]) stateForError(err error) (CtxStateFunc, error) {
	err = s.call.opts.ErrF(err)
	pe := new(backoff.PermanentError)
	if errors.As(err, &pe) {
		return nil, pe.Err
	}
	s.duration = s.bo.NextBackOff()
	if s.duration == backoff.Stop {
		return nil, err
	}
	return s.backoff, nil
}

func (s *reliableCallStateMachine[Req, Resp]) open(ctx context.Context) (CtxStateFunc, error) {
	eg, sCtx := errgroup.WithContext(ctx)
	stream, err := s.call.opts.Open(sCtx, s.call.opts.GrpcOpts...)
	if err != nil {
		return s.stateForError(err)
	}
	var nextState CtxStateFunc
	// Handle requests
	eg.Go(func() error {
		timeout := time.NewTimer(s.call.opts.ReadRequestTimeout)
		for {
			if !timeout.Stop() {
				<-timeout.C
			}
			timeout.Reset(s.call.opts.ReadRequestTimeout)
			select {
			case req, ok := <-s.requests.Recv():
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
				nextState = s.initial
				return stream.CloseSend()
			}
		}
	})
	// Handle responses
	eg.Go(func() error {
		timeout := time.NewTimer(s.call.opts.DeliverRespTimeout)
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
			req, ok := s.requests.Front()
			if !ok {
				return errors.New("unexpected requests closed")
			}
			timeout.Reset(s.call.opts.DeliverRespTimeout)
			select {
			case <-sCtx.Done():
				return context.Cause(sCtx)
			case s.call.respCh <- reqResp[Req, Resp]{Req: req, Resp: resp}:
				s.requests.Ack()
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
		s.requests.Reset()
		return nextState, nil
	} else if errors.As(err, &dre) {
		// We do not reset the reliable Chan of requsets here.
		// Once this response is delivered, it will be Ackd and
		// the channel will be reset by the next state.
		s.resp = dre.Resp
		return s.blockForDeliverResp, nil
	} else {
		s.requests.Reset()
		return s.stateForError(err)
	}
}

func (s *reliableCallStateMachine[Req, Resp]) blockForDeliverResp(ctx context.Context) (CtxStateFunc, error) {
	req, ok := s.requests.Front()
	if !ok {
		return nil, errors.New("unexpected requests closed")
	}
	select {
	case s.call.respCh <- reqResp[Req, Resp]{Req: req, Resp: s.resp}:
		s.requests.Ack()
		s.requests.Reset()
		return s.initial, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (s *reliableCallStateMachine[Req, Resp]) backoff(ctx context.Context) (CtxStateFunc, error) {
	select {
	case <-time.After(s.duration):
		return s.initial, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}
