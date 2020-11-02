// Copyright 2020 Dolthub, Inc.
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

package async

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestGoWithCancel(t *testing.T) {
	t.Run("NilError", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		_ = GoWithCancel(ctx, eg, func(ctx context.Context) error {
			return nil
		})
		assert.NoError(t, eg.Wait())
	})
	t.Run("NonNilError", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		_ = GoWithCancel(ctx, eg, func(ctx context.Context) error {
			return errors.New("there was an error")
		})
		assert.Error(t, eg.Wait())
	})
	t.Run("CancelNoError", func(t *testing.T) {
		eg, ctx := errgroup.WithContext(context.Background())
		cancel := GoWithCancel(ctx, eg, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		cancel()
		assert.NoError(t, eg.Wait())
	})
	t.Run("CancelParentError", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		eg, ctx := errgroup.WithContext(parent)
		_ = GoWithCancel(ctx, eg, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		cancel()
		assert.Error(t, eg.Wait())
	})
}
