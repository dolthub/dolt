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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestTimeoutController(t *testing.T) {
	t.Run("ClosedRun", func(t *testing.T) {
		tc := NewTimeoutController()
		tc.Close()
		assert.NoError(t, tc.Run())

		tc = NewTimeoutController()
		var eg errgroup.Group
		eg.Go(tc.Run)
		tc.SetTimeout(context.Background(), 0)
		tc.Close()
		assert.NoError(t, eg.Wait())
	})
	t.Run("DeliversTimeout", func(t *testing.T) {
		tc := NewTimeoutController()
		var eg errgroup.Group
		eg.Go(tc.Run)
		tc.SetTimeout(context.Background(), 1*time.Millisecond)
		assert.Error(t, eg.Wait())
		tc.Close()
	})
	t.Run("AdjustsTimeout", func(t *testing.T) {
		tc := NewTimeoutController()
		var eg errgroup.Group
		eg.Go(tc.Run)
		tc.SetTimeout(context.Background(), 1*time.Hour)
		tc.SetTimeout(context.Background(), 1*time.Millisecond)
		assert.Error(t, eg.Wait())
		tc.Close()
	})
	t.Run("ClosesWithTimeoutSet", func(t *testing.T) {
		tc := NewTimeoutController()
		var eg errgroup.Group
		eg.Go(tc.Run)
		tc.SetTimeout(context.Background(), 1*time.Hour)
		tc.Close()
		assert.NoError(t, eg.Wait())
	})
	t.Run("SetTimeoutRespectsContext", func(t *testing.T) {
		t.Run("BeforeRun", func(t *testing.T) {
			tc := NewTimeoutController()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tc.SetTimeout(ctx, 1*time.Hour)
		})
		t.Run("AfterRun", func(t *testing.T) {
			tc := NewTimeoutController()
			tc.Close()
			assert.NoError(t, tc.Run())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tc.SetTimeout(ctx, 1*time.Hour)
		})
	})
}
