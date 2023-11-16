// Copyright 2023 Dolthub, Inc.
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

package svcs

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestController(t *testing.T) {
	t.Run("NewController", func(t *testing.T) {
		c := NewController()
		require.NotNil(t, c)
	})
	t.Run("Stop", func(t *testing.T) {
		t.Run("CalledBeforeStart", func(t *testing.T) {
			c := NewController()
			c.Stop()
			require.Error(t, c.Start(context.Background()))
			require.NoError(t, c.WaitForStart())
			require.NoError(t, c.WaitForStop())
		})
		t.Run("ReturnsFirstError", func(t *testing.T) {
			c := NewController()
			ctx := context.Background()
			err := errors.New("first")
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error { return nil },
				RunF:  func(context.Context) {},
				StopF: func() error { return errors.New("second") },
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error { return nil },
				RunF:  func(context.Context) {},
				StopF: func() error { return err },
			}))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(t, c.WaitForStart())
				c.Stop()
			}()
			require.ErrorIs(t, c.Start(ctx), err)
			require.ErrorIs(t, c.WaitForStop(), err)
			wg.Wait()
		})
	})
	t.Run("EmptyServices", func(t *testing.T) {
		c := NewController()
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, c.WaitForStart())
			c.Stop()
		}()
		require.NoError(t, c.Start(ctx))
		require.NoError(t, c.WaitForStop())
		wg.Wait()
	})
	t.Run("Register", func(t *testing.T) {
		t.Run("AfterStartCalled", func(t *testing.T) {
			c := NewController()
			ctx := context.Background()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(t, c.WaitForStart())
				require.Error(t, c.Register(&AnonService{
					InitF: func(context.Context) error { return nil },
					RunF:  func(context.Context) {},
					StopF: func() error { return nil },
				}))
				c.Stop()
			}()
			require.NoError(t, c.Start(ctx))
			require.NoError(t, c.WaitForStop())
			wg.Wait()
		})
	})
	t.Run("Start", func(t *testing.T) {
		t.Run("CallsInitInOrder", func(t *testing.T) {
			c := NewController()
			var inited []int
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					inited = append(inited, 0)
					return nil
				},
				RunF:  func(context.Context) {},
				StopF: func() error { return nil },
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					inited = append(inited, 1)
					return nil
				},
				RunF:  func(context.Context) {},
				StopF: func() error { return nil },
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					inited = append(inited, 2)
					return nil
				},
				RunF:  func(context.Context) {},
				StopF: func() error { return nil },
			}))
			ctx := context.Background()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(t, c.WaitForStart())
				c.Stop()
			}()
			require.NoError(t, c.Start(ctx))
			require.NoError(t, c.WaitForStop())
			require.Equal(t, inited, []int{0, 1, 2})
			wg.Wait()
		})
		t.Run("StopsCallingInitOnFirstError", func(t *testing.T) {
			err := errors.New("first error")
			c := NewController()
			var inited []int
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					inited = append(inited, 0)
					return nil
				},
				RunF:  func(context.Context) {},
				StopF: func() error { return nil },
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					inited = append(inited, 1)
					return nil
				},
				RunF:  func(context.Context) {},
				StopF: func() error { return nil },
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					return err
				},
				RunF:  func(context.Context) {},
				StopF: func() error { return nil },
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					inited = append(inited, 2)
					return nil
				},
				RunF:  func(context.Context) {},
				StopF: func() error { return nil },
			}))
			ctx := context.Background()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.ErrorIs(t, c.WaitForStart(), err)
				c.Stop()
			}()
			require.ErrorIs(t, c.Start(ctx), err)
			require.ErrorIs(t, c.WaitForStop(), err)
			require.Equal(t, inited, []int{0, 1})
			wg.Wait()
		})
		t.Run("CallsStopWhenInitErrors", func(t *testing.T) {
			err := errors.New("first error")
			c := NewController()
			var stopped []int
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					return nil
				},
				RunF: func(context.Context) {},
				StopF: func() error {
					stopped = append(stopped, 0)
					return nil
				},
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					return nil
				},
				RunF: func(context.Context) {},
				StopF: func() error {
					stopped = append(stopped, 1)
					return nil
				},
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					return err
				},
				RunF: func(context.Context) {},
				StopF: func() error {
					stopped = append(stopped, 2)
					return nil
				},
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error {
					return nil
				},
				RunF: func(context.Context) {},
				StopF: func() error {
					stopped = append(stopped, 3)
					return nil
				},
			}))
			ctx := context.Background()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.ErrorIs(t, c.WaitForStart(), err)
				c.Stop()
			}()
			require.ErrorIs(t, c.Start(ctx), err)
			require.ErrorIs(t, c.WaitForStop(), err)
			require.Equal(t, stopped, []int{1, 0})
			wg.Wait()
		})
		t.Run("RunsServices", func(t *testing.T) {
			c := NewController()
			var wg sync.WaitGroup
			wg.Add(2)
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error { return nil },
				RunF:  func(context.Context) { wg.Done() },
				StopF: func() error { return nil },
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error { return nil },
				RunF:  func(context.Context) { wg.Done() },
				StopF: func() error { return nil },
			}))
			ctx := context.Background()
			var cwg sync.WaitGroup
			cwg.Add(1)
			go func() {
				defer cwg.Done()
				require.NoError(t, c.WaitForStart())
				c.Stop()
			}()
			require.NoError(t, c.Start(ctx))
			require.NoError(t, c.WaitForStop())
			wg.Wait()
			cwg.Wait()
		})
		t.Run("StopsAllServices", func(t *testing.T) {
			c := NewController()
			var wg sync.WaitGroup
			err := errors.New("first error")
			wg.Add(2)
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error { return nil },
				RunF:  func(context.Context) {},
				StopF: func() error {
					wg.Done()
					return errors.New("second error")
				},
			}))
			require.NoError(t, c.Register(&AnonService{
				InitF: func(context.Context) error { return nil },
				RunF:  func(context.Context) {},
				StopF: func() error {
					wg.Done()
					return err
				},
			}))
			ctx := context.Background()
			var cwg sync.WaitGroup
			cwg.Add(1)
			go func() {
				defer cwg.Done()
				require.NoError(t, c.WaitForStart())
				c.Stop()
			}()
			require.ErrorIs(t, c.Start(ctx), err)
			require.ErrorIs(t, c.WaitForStop(), err)
			wg.Wait()
			cwg.Wait()
		})
	})
	t.Run("RunStopsControllerExample", func(t *testing.T) {
		// |Run| has no way to return an error, but it *can* use the
		// controller itself to coordinate a shutdown of all the
		// services and to ensure that an error is returned from its
		// Service's |Close| method.
		c := NewController()
		ctx := context.Background()

		expectedErr := errors.New("error set from run")
		errCh := make(chan error)
		var runErr error
		var runWg sync.WaitGroup
		runWg.Add(1)
		failingService := &AnonService{
			RunF: func(context.Context) {
				runErr = <-errCh
				// Do this in the background, since it will block on StopF down below being completed.
				go c.Stop()
				runWg.Done()
			},
			StopF: func() error {
				runWg.Wait()
				return runErr
			},
		}
		c.Register(failingService)

		// See how we do not call |Stop| on the controller here. The
		// "failing" Run method of the failingService will call it.
		var cwg sync.WaitGroup
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			require.ErrorIs(t, c.Start(ctx), expectedErr)
		}()
		require.NoError(t, c.WaitForStart())
		errCh <- expectedErr
		require.ErrorIs(t, c.WaitForStop(), expectedErr)
		cwg.Wait()
	})
}
