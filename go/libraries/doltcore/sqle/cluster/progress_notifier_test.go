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

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProgressNotifier(t *testing.T) {
	t.Run("WaitBeforeBeginAttempt", func(t *testing.T) {
		p := new(ProgressNotifier)
		f := p.Wait()
		a := p.BeginAttempt()
		p.RecordSuccess(a)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)
		assert.NoError(t, f(ctx))
	})

	t.Run("WaitAfterBeginAttempt", func(t *testing.T) {
		p := new(ProgressNotifier)
		a := p.BeginAttempt()
		f := p.Wait()
		p.RecordSuccess(a)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)
		assert.ErrorIs(t, f(ctx), context.DeadlineExceeded)

		a = p.BeginAttempt()
		p.RecordSuccess(a)
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)
		assert.NoError(t, f(ctx))
	})

	t.Run("WaitBeforeAttemptFailure", func(t *testing.T) {
		p := new(ProgressNotifier)
		f := p.Wait()
		a := p.BeginAttempt()
		p.RecordFailure(a)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)
		assert.ErrorIs(t, f(ctx), context.DeadlineExceeded)

		a = p.BeginAttempt()
		p.RecordSuccess(a)
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)
		assert.NoError(t, f(ctx))
	})
}
