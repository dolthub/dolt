// Copyright 2020 Liquidata, Inc.
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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActionExecutorOrdered(t *testing.T) {
	expectedStr := "abcdefghijklmnopqrstuvwxyz"
	outStr := ""
	actionExecutor := NewActionExecutor(context.Background(), func(ctx context.Context, val interface{}) error {
		str := val.(string)
		outStr += str
		return nil
	}, 1, 0)
	for _, char := range expectedStr {
		actionExecutor.Execute(string(char))
	}
	err := actionExecutor.WaitForEmpty()
	require.NoError(t, err)
	assert.Equal(t, expectedStr, outStr)
}

func TestActionExecutorOrderedBuffered(t *testing.T) {
	expectedStr := "abcdefghijklmnopqrstuvwxyz"
	outStr := ""
	actionExecutor := NewActionExecutor(context.Background(), func(ctx context.Context, val interface{}) error {
		str := val.(string)
		outStr += str
		return nil
	}, 1, 3)
	for _, char := range expectedStr {
		actionExecutor.Execute(string(char))
	}
	err := actionExecutor.WaitForEmpty()
	require.NoError(t, err)
	assert.Equal(t, expectedStr, outStr)
}

func TestActionExecutorUnordered(t *testing.T) {
	expectedValue := int64(50005000)
	outValue := int64(0)
	actionExecutor := NewActionExecutor(context.Background(), func(ctx context.Context, val interface{}) error {
		atomic.AddInt64(&outValue, val.(int64))
		return nil
	}, 5, 0)
	for i := int64(1); i <= 10000; i++ {
		actionExecutor.Execute(i)
	}
	err := actionExecutor.WaitForEmpty()
	require.NoError(t, err)
	assert.Equal(t, expectedValue, outValue)
}

func TestActionExecutorUnorderedBuffered(t *testing.T) {
	expectedValue := int64(50005000)
	outValue := int64(0)
	actionExecutor := NewActionExecutor(context.Background(), func(ctx context.Context, val interface{}) error {
		atomic.AddInt64(&outValue, val.(int64))
		return nil
	}, 5, 10)
	for i := int64(1); i <= 10000; i++ {
		actionExecutor.Execute(i)
	}
	err := actionExecutor.WaitForEmpty()
	require.NoError(t, err)
	assert.Equal(t, expectedValue, outValue)
}

func TestActionExecutorUnnecessaryWaits(t *testing.T) {
	outValue := int64(0)
	actionExecutor := NewActionExecutor(context.Background(), func(ctx context.Context, val interface{}) error {
		atomic.AddInt64(&outValue, val.(int64))
		return nil
	}, 5, 10)
	for i := int64(1); i <= 10000; i++ {
		actionExecutor.Execute(i)
	}
	for i := 0; i < 10; i++ {
		err := actionExecutor.WaitForEmpty()
		assert.NoError(t, err)
	}
}

func TestActionExecutorError(t *testing.T) {
	for _, conBuf := range []struct {
		concurrency uint32
		maxBuffer   uint64
	}{
		{1, 0},
		{5, 0},
		{10, 0},
		{1, 1},
		{5, 1},
		{10, 1},
		{1, 5},
		{5, 5},
		{10, 5},
		{1, 10},
		{5, 10},
		{10, 10},
	} {
		actionExecutor := NewActionExecutor(context.Background(), func(ctx context.Context, val interface{}) error {
			if val.(int64) == 11 {
				return errors.New("hey there")
			}
			return nil
		}, conBuf.concurrency, conBuf.maxBuffer)
		for i := int64(1); i <= 100; i++ {
			actionExecutor.Execute(i)
		}
		err := actionExecutor.WaitForEmpty()
		assert.Error(t, err)
		err = actionExecutor.WaitForEmpty()
		assert.NoError(t, err)
	}
}

func TestActionExecutorPanicRecovery(t *testing.T) {
	for _, conBuf := range []struct {
		concurrency uint32
		maxBuffer   uint64
	}{
		{1, 0},
		{5, 0},
		{10, 0},
		{1, 1},
		{5, 1},
		{10, 1},
		{1, 5},
		{5, 5},
		{10, 5},
		{1, 10},
		{5, 10},
		{10, 10},
	} {
		actionExecutor := NewActionExecutor(context.Background(), func(ctx context.Context, val interface{}) error {
			if val.(int64) == 22 {
				panic("hey there")
			}
			return nil
		}, conBuf.concurrency, conBuf.maxBuffer)
		for i := int64(1); i <= 100; i++ {
			actionExecutor.Execute(i)
		}
		err := actionExecutor.WaitForEmpty()
		require.Error(t, err)
	}
}
