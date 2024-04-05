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
)

type state interface {
	Run(ctx context.Context) (state, error)
}

type stateVars[Req, Resp any] struct {
	call     *reliableCall[Req, Resp]
	requests *Chan[Req]
	bo       backoff.BackOff
}

type stateInitial[Req, Resp any] struct {
	stateVars[Req, Resp]
}

func (s *stateInitial[Req, Resp]) Run(ctx context.Context) (state, error) {
	select {
	case _, ok := <-s.requests.Recv():
		if !ok {
			return nil, nil
		}
		// The easiest way to drive the just-received request
		// in the Open state is to just reset the reliable
		// channel here.
		s.requests.Reset()
		return &stateOpen[Req, Resp]{stateVars: s.stateVars}, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

type stateOpen[Req, Resp any] struct {
	stateVars[Req, Resp]
}

func (s *stateOpen[Req, Resp]) Run(ctx context.Context) (state, error) {
	eg, sCtx := errgroup.WithContext(ctx)
	stream, err := s.call.opts.Open(sCtx, s.call.opts.GrpcOpts...)
	if err != nil {
		return s.call.stateForError(err, s.stateVars)
	}
	var nextState state
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
				nextState = &stateInitial[Req, Resp]{stateVars: s.stateVars}
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
			timeout.Reset(s.call.opts.DeliverRespTimeout)
			select {
			case <-sCtx.Done():
				return context.Cause(sCtx)
			case s.call.respCh <- resp:
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
		return &stateBlockForDeliverResp[Req, Resp]{stateVars: s.stateVars, resp: dre.Resp}, nil
	} else {
		s.requests.Reset()
		return s.call.stateForError(err, s.stateVars)
	}
}

type stateBlockForDeliverResp[Req, Resp any] struct {
	stateVars[Req, Resp]
	resp Resp
}

func (s *stateBlockForDeliverResp[Req, Resp]) Run(ctx context.Context) (state, error) {
	select {
	case s.call.respCh <- s.resp:
		s.requests.Ack()
		s.requests.Reset()
		return &stateInitial[Req, Resp]{stateVars: s.stateVars}, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (c *reliableCall[Req, Resp]) stateForError(err error, s stateVars[Req, Resp]) (state, error) {
	err = c.opts.ErrF(err)
	pe := new(backoff.PermanentError)
	if errors.As(err, &pe) {
		return nil, pe.Err
	}
	duration := s.bo.NextBackOff()
	if duration == backoff.Stop {
		return nil, err
	}
	return &stateBackoff[Req, Resp]{stateVars: s, duration: duration}, nil
}

type stateBackoff[Req, Resp any] struct {
	stateVars[Req, Resp]
	duration time.Duration
}

func (s *stateBackoff[Req, Resp]) Run(ctx context.Context) (state, error) {
	select {
	case <-time.After(s.duration):
		return &stateInitial[Req, Resp]{stateVars: s.stateVars}, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}
