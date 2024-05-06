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

package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDynamic(t *testing.T) {
	t.Run("Size=0", func(t *testing.T) {
		assert.Panics(t, func() {
			NewDynamic(context.Background(), func(context.Context, <-chan struct{}) error { return nil }, 0)
		})
	})
	t.Run("F=nil", func(t *testing.T) {
		assert.Panics(t, func() {
			NewDynamic(context.Background(), nil, 16)
		})
	})
	t.Run("StaticSize", func(t *testing.T) {
		arrives := make(chan struct{})
		f := func(ctx context.Context, shutdownCh <-chan struct{}) error {
			arrives <- struct{}{}
			<-shutdownCh
			return nil
		}
		p := NewDynamic(context.Background(), f, 16)
		select {
		case <-arrives:
			assert.FailNow(t, "should not spawn threads with Run()")
		default:
		}

		var err error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = p.Run()
		}()

		for i := 0; i < 16; i++ {
			<-arrives
		}

		p.Close()
		wg.Wait()
		assert.NoError(t, err)
	})
	t.Run("ScaleUpAndDown", func(t *testing.T) {
		arrives := make(chan struct{})
		var exits atomic.Int32
		f := func(ctx context.Context, shutdownCh <-chan struct{}) error {
			defer exits.Add(1)
			arrives <- struct{}{}
			<-shutdownCh
			return nil
		}
		p := NewDynamic(context.Background(), f, 16)

		var err error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = p.Run()
		}()

		for i := 0; i < 16; i++ {
			<-arrives
		}

		p.SetSize(8)
		assert.Equal(t, int32(8), exits.Load())
		exits.Store(0)
		p.SetSize(16)
		for i := 0; i < 8; i++ {
			<-arrives
		}
		p.SetSize(1)
		assert.Equal(t, int32(15), exits.Load())

		p.Close()
		wg.Wait()
		assert.NoError(t, err)
	})
	t.Run("Error", func(t *testing.T) {
		var starts atomic.Int32
		var exits atomic.Int32
		ferr := errors.New("error encountered")
		f := func(ctx context.Context, shutdownCh <-chan struct{}) error {
			defer exits.Add(1)
			if starts.Add(1) == 32 {
				return ferr
			}
			select {
			case <-shutdownCh:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		p := NewDynamic(context.Background(), f, 64)
		var err error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = p.Run()
		}()
		wg.Wait()
		assert.ErrorIs(t, err, ferr)
		p.Close()
	})
}
