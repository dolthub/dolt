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

// In |initial|, we wait for a first request to come in from the application.
// Sometimes this will be an already queued request, and sometimes it will be a
// new request which we want to drive and get a response for. When we get our
// first request to send, we transition to |open|.
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

// This should be configurable.
var rpcOneShotTimeout = 15 * time.Second

// Open is the normal state of reading requests from the application, sending
// them to the RPC, and reading thier responses from the RPC, and sending the
// responses to the application.
func (s *reliableCallStateMachine[Req, Resp]) open(ctx context.Context) (CtxStateFunc, error) {
	var nextState CtxStateFunc
	eg, sCtx := errgroup.WithContext(ctx)
	tc := NewTimeoutController()
	eg.Go(func() error {
		return tc.Run()
	})
	eg.Go(func() error {
		tc.SetTimeout(sCtx, rpcOneShotTimeout)
		stream, err := s.call.opts.Open(sCtx, s.call.opts.GrpcOpts...)
		if err != nil {
			tc.Close()
			return err
		}
		tc.SetTimeout(sCtx, 0)
		// Handle requests
		eg.Go(func() error {
			defer tc.Close()
			// If we don't get new requests to send from the application
			// for a certain amount of time, then we can close the remote
			// call and transition to a state where we wait for a new
			// request before opening the connection again.
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
					tc.SetTimeout(sCtx, rpcOneShotTimeout)
					err := stream.Send(req)
					if err != nil {
						return err
					}
					tc.SetTimeout(sCtx, 0)
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
				// If we can't deliver our response to the application
				// for a certain amount of time, then we want to shut
				// down the streaming connection and wait until the
				// application accepts this response. Then we will open
				// the connection again and get more responses by
				// sending our queued requests.
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
		return nil
	})
	var dre ErrWantBlockForDeliverResp[Resp]
	err := eg.Wait()
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

// We transition here if the application is slow to accept our responses. We
// wait until they accept our response, and then we go back to |initial|, where
// we will open a new RPC if we have requests to send.
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

// We transition here if we see retriable errors from our RPC. We backoff a
// certain amount of time and then transition to |initial|.
func (s *reliableCallStateMachine[Req, Resp]) backoff(ctx context.Context) (CtxStateFunc, error) {
	select {
	case <-time.After(s.duration):
		return s.initial, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}
