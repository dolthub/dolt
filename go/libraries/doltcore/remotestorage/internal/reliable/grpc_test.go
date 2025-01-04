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
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestMakeCall(t *testing.T) {
	t.Run("ImmediateClose", func(t *testing.T) {
		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(context.Context, ...grpc.CallOption) (ClientStream[int, int], error) {
				return nil, nil
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return &backoff.StopBackOff{}
			},
		})
		assert.NoError(t, err)
		assert.NoError(t, stream.CloseSend())
		_, err = stream.Recv()
		assert.ErrorIs(t, err, io.EOF)
	})
	t.Run("OpenErrorStopBackOff_Recv", func(t *testing.T) {
		thisErr := errors.New("error opening stream")
		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(context.Context, ...grpc.CallOption) (ClientStream[int, int], error) {
				return nil, thisErr
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return &backoff.StopBackOff{}
			},
		})
		assert.NoError(t, err)
		err = stream.Send(0)
		assert.NoError(t, err)
		_, err = stream.Recv()
		assert.EqualError(t, err, thisErr.Error())
	})
	t.Run("OpenErrorStopBackOff_Send", func(t *testing.T) {
		thisErr := errors.New("error opening stream")
		opened := make(chan struct{})
		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(context.Context, ...grpc.CallOption) (ClientStream[int, int], error) {
				close(opened)
				return nil, thisErr
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return &backoff.StopBackOff{}
			},
		})
		assert.NoError(t, err)
		err = stream.Send(0)
		assert.NoError(t, err)
		<-opened
		err = stream.Send(1)
		assert.EqualError(t, err, thisErr.Error())
	})
	t.Run("OpenErrorPermanentErr_Recv", func(t *testing.T) {
		thisErr := errors.New("error opening stream")
		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF: func(err error) error {
				return &backoff.PermanentError{Err: err}
			},
			Open: func(context.Context, ...grpc.CallOption) (ClientStream[int, int], error) {
				return nil, thisErr
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return backoff.NewConstantBackOff(5 * time.Second)
			},
		})
		assert.NoError(t, err)
		err = stream.Send(0)
		assert.NoError(t, err)
		_, err = stream.Recv()
		assert.EqualError(t, err, thisErr.Error())
	})
	t.Run("OpenErrorPermanentErr_Send", func(t *testing.T) {
		thisErr := errors.New("error opening stream")
		opened := make(chan struct{})
		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF: func(err error) error {
				return &backoff.PermanentError{Err: err}
			},
			Open: func(context.Context, ...grpc.CallOption) (ClientStream[int, int], error) {
				close(opened)
				return nil, thisErr
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return backoff.NewConstantBackOff(5 * time.Second)
			},
		})
		assert.NoError(t, err)
		err = stream.Send(0)
		assert.NoError(t, err)
		<-opened
		err = stream.Send(1)
		assert.EqualError(t, err, thisErr.Error())
	})
	t.Run("SingleStream", func(t *testing.T) {
		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
				return newTestStream[int](ctx, 1), nil
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return &backoff.StopBackOff{}
			},
			ReadRequestTimeout: 5 * time.Second,
			DeliverRespTimeout: 5 * time.Second,
		})
		assert.NoError(t, err)
		for i := 0; i < 8; i++ {
			err = stream.Send(i)
			assert.NoError(t, err)
			j, err := stream.Recv()
			assert.NoError(t, err)
			assert.Equal(t, i, j)
		}
		err = stream.CloseSend()
		assert.NoError(t, err)
		_, err = stream.Recv()
		assert.ErrorIs(t, err, io.EOF)
	})
	t.Run("SingleStreamBackOffOnOpen", func(t *testing.T) {
		tries := 0
		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
				if tries >= 3 {
					return newTestStream[int](ctx, 1), nil
				} else {
					tries += 1
					return nil, errors.New("attempt")
				}
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return backoff.NewConstantBackOff(0)
			},
			ReadRequestTimeout: 5 * time.Second,
			DeliverRespTimeout: 5 * time.Second,
		})
		assert.NoError(t, err)
		for i := 0; i < 8; i++ {
			err = stream.Send(i)
			assert.NoError(t, err)
			j, err := stream.Recv()
			assert.NoError(t, err)
			assert.Equal(t, i, j)
		}
		err = stream.CloseSend()
		assert.NoError(t, err)
		_, err = stream.Recv()
		assert.ErrorIs(t, err, io.EOF)
	})
	t.Run("CtxCancelInInitialState", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := MakeCall(ctx, CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
				return newTestStream[int](ctx, 1), nil
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return backoff.NewConstantBackOff(0)
			},
			ReadRequestTimeout: 5 * time.Second,
			DeliverRespTimeout: 5 * time.Second,
		})
		assert.NoError(t, err)
		cancel()
		_, err = stream.Recv()
		assert.ErrorIs(t, err, context.Canceled)
	})
	t.Run("CtxCancelInBackOff_Recv", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		opened := make(chan struct{})
		stream, err := MakeCall(ctx, CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(context.Context, ...grpc.CallOption) (ClientStream[int, int], error) {
				close(opened)
				return nil, errors.New("got error")
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return backoff.NewConstantBackOff(5 * time.Second)
			},
			ReadRequestTimeout: 5 * time.Second,
			DeliverRespTimeout: 5 * time.Second,
		})
		assert.NoError(t, err)
		err = stream.Send(0)
		assert.NoError(t, err)
		<-opened
		cancel()
		_, err = stream.Recv()
		assert.ErrorIs(t, err, context.Canceled)
	})
	t.Run("CtxCancelInBackOff_Send", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		opened := make(chan struct{})
		stream, err := MakeCall(ctx, CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(context.Context, ...grpc.CallOption) (ClientStream[int, int], error) {
				close(opened)
				return nil, errors.New("got error")
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return backoff.NewConstantBackOff(5 * time.Second)
			},
			ReadRequestTimeout: 5 * time.Second,
			DeliverRespTimeout: 5 * time.Second,
		})
		assert.NoError(t, err)
		err = stream.Send(0)
		assert.NoError(t, err)
		<-opened
		cancel()
		err = stream.Send(1)
		assert.ErrorIs(t, err, context.Canceled)
	})
	t.Run("RedrivesOnStreamErrors", func(t *testing.T) {
		parity := false
		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
				parity = !parity
				if parity {
					return &errAfterRecvsStream[int]{
						stream: newTestStream[int](ctx, 1),
						recvs:  2,
						err:    errors.New("an error after recving"),
					}, nil
				} else {
					return &errAfterSendsStream[int]{
						stream: newTestStream[int](ctx, 1),
						sends:  2,
						err:    errors.New("an error after sending"),
					}, nil
				}
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return backoff.NewConstantBackOff(0)
			},
			ReadRequestTimeout: 5 * time.Second,
			DeliverRespTimeout: 5 * time.Second,
		})
		assert.NoError(t, err)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 16; i++ {
				err := stream.Send(i)
				assert.NoError(t, err)
			}
			assert.NoError(t, stream.CloseSend())
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 16; i++ {
				j, err := stream.Recv()
				assert.NoError(t, err)
				assert.Equal(t, i, j)
			}
			_, err := stream.Recv()
			assert.ErrorIs(t, err, io.EOF)
		}()
		wg.Wait()
	})
	t.Run("NestErrorStreams", func(t *testing.T) {
		// This is a somewhat pathological end-to-end test nesting
		// multiple reliable streams on top of each other, where some
		// of the streams throw periodic errors.

		errF := func(err error) error {
			return err
		}
		backOffF := func(ctx context.Context) backoff.BackOff {
			return backoff.NewConstantBackOff(time.Millisecond * time.Duration(rand.IntN(32)))
		}
		base := func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
			return MakeCall(ctx, CallOptions[int, int]{
				ErrF:     errF,
				BackOffF: backOffF,
				Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
					return newTestStream[int](ctx, rand.IntN(8)), nil
				},
				ReadRequestTimeout: 5 * time.Second,
				DeliverRespTimeout: 5 * time.Second,
			})
		}
		recvError := func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
			return MakeCall(ctx, CallOptions[int, int]{
				ErrF:     errF,
				BackOffF: backOffF,
				Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
					s, err := base(ctx, opts...)
					if err != nil {
						return nil, err
					}
					return &errAfterRecvsStream[int]{
						stream: s,
						recvs:  rand.IntN(8),
						err:    errors.New("an error after recving"),
					}, nil
				},
				ReadRequestTimeout: 5 * time.Second,
				DeliverRespTimeout: 5 * time.Second,
			})
		}
		sendError := func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
			return MakeCall(ctx, CallOptions[int, int]{
				ErrF:     errF,
				BackOffF: backOffF,
				Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
					s, err := recvError(ctx, opts...)
					if err != nil {
						return nil, err
					}
					return &errAfterSendsStream[int]{
						stream: s,
						sends:  rand.IntN(8),
						err:    errors.New("an error after recving"),
					}, nil
				},
				ReadRequestTimeout: 5 * time.Second,
				DeliverRespTimeout: 5 * time.Second,
			})
		}

		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF:               errF,
			BackOffF:           backOffF,
			Open:               sendError,
			ReadRequestTimeout: 5 * time.Second,
			DeliverRespTimeout: 5 * time.Second,
		})
		assert.NotNil(t, stream)
		assert.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 128; i++ {
				assert.NoError(t, stream.Send(i))
			}
			assert.NoError(t, stream.CloseSend())
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 128; i++ {
				j, err := stream.Recv()
				assert.NoError(t, err)
				assert.Equal(t, i, j)
			}
			_, err := stream.Recv()
			assert.ErrorIs(t, err, io.EOF)
		}()
		wg.Wait()
	})
	t.Run("StreamReadTimeout", func(t *testing.T) {
		closeNotify := make(chan struct{})
		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
				ret := newTestStream[int](ctx, 1)
				ret.closeNotify = closeNotify
				return ret, nil
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return backoff.NewConstantBackOff(0)
			},
			ReadRequestTimeout: 50 * time.Millisecond,
			DeliverRespTimeout: 5 * time.Second,
		})
		assert.NoError(t, err)
		err = stream.Send(0)
		assert.NoError(t, err)
		i, err := stream.Recv()
		assert.Equal(t, i, 0)
		assert.NoError(t, err)
		<-closeNotify
		err = stream.Send(1)
		assert.NoError(t, err)
		i, err = stream.Recv()
		assert.Equal(t, i, 1)
		assert.NoError(t, err)
		<-closeNotify
		err = stream.CloseSend()
		assert.NoError(t, err)
		_, err = stream.Recv()
		assert.ErrorIs(t, err, io.EOF)
	})
	t.Run("StreamDeliverTimeout", func(t *testing.T) {
		ctxNotify := make(chan struct{})
		stream, err := MakeCall(context.Background(), CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
				ret := newTestStream[int](ctx, 1)
				go func() {
					<-ctx.Done()
					ctxNotify <- struct{}{}
				}()
				return ret, nil
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return backoff.NewConstantBackOff(0)
			},
			ReadRequestTimeout: 5 * time.Second,
			DeliverRespTimeout: 50 * time.Millisecond,
		})
		assert.NoError(t, err)
		err = stream.Send(0)
		assert.NoError(t, err)
		<-ctxNotify
		i, err := stream.Recv()
		assert.Equal(t, i, 0)
		assert.NoError(t, err)
		err = stream.Send(1)
		assert.NoError(t, err)
		<-ctxNotify
		i, err = stream.Recv()
		assert.Equal(t, i, 1)
		err = stream.CloseSend()
		assert.NoError(t, err)
		_, err = stream.Recv()
		assert.ErrorIs(t, err, io.EOF)
	})
	t.Run("CtxCancelInStreamDeliverTimeout_Recv", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ctxNotify := make(chan struct{})
		stream, err := MakeCall(ctx, CallOptions[int, int]{
			ErrF: func(err error) error {
				return err
			},
			Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[int, int], error) {
				ret := newTestStream[int](ctx, 1)
				go func() {
					<-ctx.Done()
					ctxNotify <- struct{}{}
				}()
				return ret, nil
			},
			BackOffF: func(ctx context.Context) backoff.BackOff {
				return backoff.NewConstantBackOff(0)
			},
			ReadRequestTimeout: 5 * time.Second,
			DeliverRespTimeout: 50 * time.Millisecond,
		})
		assert.NoError(t, err)
		err = stream.Send(0)
		assert.NoError(t, err)
		<-ctxNotify
		cancel()
		_, err = stream.Recv()
		assert.ErrorIs(t, err, context.Canceled)
	})
	t.Run("ErrWantBlockForDeliverResp", func(t *testing.T) {
		assert.EqualError(t, ErrWantBlockForDeliverResp[int]{}, "ErrWantBlockForDeliverResp")
	})
	t.Run("RecvOnCompletedCallAndCanceledParentCtxNeverReturnsError", func(t *testing.T) {
		for i := 0; i < 128; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			stream, err := MakeCall(ctx, CallOptions[*int, *int]{
				ErrF: func(err error) error {
					return err
				},
				Open: func(ctx context.Context, opts ...grpc.CallOption) (ClientStream[*int, *int], error) {
					ret := newTestStream[*int](ctx, 1)
					return ret, nil
				},
				BackOffF: func(ctx context.Context) backoff.BackOff {
					return backoff.NewConstantBackOff(0)
				},
				ReadRequestTimeout: 5 * time.Second,
				DeliverRespTimeout: 50 * time.Millisecond,
			})
			assert.NoError(t, stream.CloseSend())
			cancel()
			runtime.Gosched()
			_, err = stream.Recv()
			assert.Error(t, err)
		}
	})
}

type testStream[T any] struct {
	ctx context.Context
	ch  chan T

	closeNotify chan struct{}
}

func newTestStream[T any](ctx context.Context, sz int) *testStream[T] {
	return &testStream[T]{
		ctx: ctx,
		ch:  make(chan T, sz),
	}
}

func (s *testStream[T]) Send(t T) error {
	select {
	case s.ch <- t:
		return nil
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	}
}

func (s *testStream[T]) CloseSend() error {
	close(s.ch)
	if s.closeNotify != nil {
		s.closeNotify <- struct{}{}
	}
	return nil
}

func (s *testStream[T]) Recv() (T, error) {
	select {
	case t, ok := <-s.ch:
		if !ok {
			return t, io.EOF
		}
		return t, nil
	case <-s.ctx.Done():
		var t T
		return t, context.Cause(s.ctx)
	}
}

type errAfterRecvsStream[T any] struct {
	stream ClientStream[T, T]
	recvs  int
	err    error
}

func (s *errAfterRecvsStream[T]) Send(t T) error {
	return s.stream.Send(t)
}

func (s *errAfterRecvsStream[T]) CloseSend() error {
	return s.stream.CloseSend()
}

func (s *errAfterRecvsStream[T]) Recv() (T, error) {
	s.recvs -= 1
	if s.recvs == 0 {
		var t T
		return t, s.err
	}
	return s.stream.Recv()
}

type errAfterSendsStream[T any] struct {
	stream ClientStream[T, T]
	sends  int
	err    error
}

func (s *errAfterSendsStream[T]) Send(t T) error {
	s.sends -= 1
	if s.sends == 0 {
		return s.err
	}
	return s.stream.Send(t)
}

func (s *errAfterSendsStream[T]) CloseSend() error {
	return s.stream.CloseSend()
}

func (s *errAfterSendsStream[T]) Recv() (T, error) {
	return s.stream.Recv()
}
