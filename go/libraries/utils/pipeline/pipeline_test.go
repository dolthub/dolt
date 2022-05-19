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

package pipeline

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intSliceToInterfaceSlice(ints []int) []interface{} {
	interfaces := make([]interface{}, len(ints))
	for i, n := range ints {
		interfaces[i] = n
	}

	return interfaces
}

func stringSliceToInterfaceSlice(strs []string) []interface{} {
	interfaces := make([]interface{}, len(strs))
	for i, str := range strs {
		interfaces[i] = str
	}

	return interfaces
}

func sliceAsInputFunc(in []interface{}, batchSize int) StageFunc {
	var i int
	return func(ctx context.Context, _ []ItemWithProps) ([]ItemWithProps, error) {
		var batch []ItemWithProps
		for j := 0; j < batchSize && i < len(in); i, j = i+1, j+1 {
			batch = append(batch, NewItemWithNoProps(in[i]))
		}

		if len(batch) == 0 {
			return nil, io.EOF
		}

		return batch, nil
	}
}

func replicateString(str string, numCopies int) []string {
	strs := make([]string, numCopies)
	for i := 0; i < numCopies; i++ {
		strs[i] = str
	}

	return strs
}

func generateIntSlice(genFunc func() int, numItems int) []int {
	ints := make([]int, numItems)
	for i := 0; i < numItems; i++ {
		ints[i] = genFunc()
	}

	return ints
}

type outputCapturer struct {
	results []ItemWithProps
}

func (oc *outputCapturer) captureOutput(_ context.Context, items []ItemWithProps) ([]ItemWithProps, error) {
	oc.results = append(oc.results, items...)
	return nil, nil
}

func TestPipeline(t *testing.T) {
	const numInputItems = 16384
	const outBatchSize = 32

	i := 0
	ints := generateIntSlice(func() int {
		n := i
		i += 1
		return n
	}, numInputItems)

	oc := &outputCapturer{}
	p := NewPipeline(
		NewStage("input", nil, sliceAsInputFunc(intSliceToInterfaceSlice(ints), outBatchSize*4), 1, -1, -1),
		NewStage("output", nil, oc.captureOutput, 1, 32, outBatchSize),
	)

	p.Start(context.Background())
	err := p.Wait()
	require.NoError(t, err)
	require.Equal(t, numInputItems, len(oc.results))

	for i := 0; i < numInputItems; i++ {
		item := oc.results[i].GetItem()
		assert.Equal(t, i, item)
	}
}

func TestParallelProcessingPipeline(t *testing.T) {
	const (
		numInputItems            = 16384
		processBatchSize         = 16
		outBatchSize             = 32
		processParallelism       = 4
		localStoreRoutineIndexID = 0
		routineIndexProperty     = "routine_index"
	)

	init := func(ctx context.Context, routineIndex int) error {
		GetLocalStorage(ctx).Put(localStoreRoutineIndexID, routineIndex)
		return nil
	}

	transform := func(ctx context.Context, in []ItemWithProps) ([]ItemWithProps, error) {
		if in == nil {
			return nil, nil
		}

		routineIdex, ok := GetLocalStorage(ctx).Get(localStoreRoutineIndexID)

		if !ok {
			return nil, errors.New("missing routine index in local storage")
		}

		out := make([]ItemWithProps, len(in))
		for i, item := range in {
			str := item.GetItem().(string)
			out[i] = NewItemWithProps(str+" processed", NewImmutableProps(map[string]interface{}{routineIndexProperty: routineIdex}))
		}

		return out, nil
	}

	oc := &outputCapturer{}
	p := NewPipeline(
		NewStage("input", nil, sliceAsInputFunc(stringSliceToInterfaceSlice(replicateString("test", numInputItems)), processBatchSize*4), 1, -1, -1),
		NewStage("process", init, transform, processParallelism, 32, processBatchSize),
		NewStage("output", nil, oc.captureOutput, 1, 32, outBatchSize),
	)

	p.Start(context.Background())
	err := p.Wait()
	require.NoError(t, err)
	require.Equal(t, numInputItems, len(oc.results))

	counts := make(map[int]int)
	for i := 0; i < numInputItems; i++ {
		item := oc.results[i].GetItem()
		assert.Equal(t, "test processed", item)

		routineIndex, ok := oc.results[i].GetProperties().Get(routineIndexProperty)
		require.True(t, ok)
		counts[routineIndex.(int)] += 1
	}

	require.Len(t, counts, processParallelism)

	for i := 0; i < processParallelism; i++ {
		assert.True(t, counts[i] != 0)
	}
}

func TestPipelineError(t *testing.T) {
	const (
		numInputItems      = 16384
		processBatchSize   = 16
		outBatchSize       = 32
		errorOnBatchNum    = 4
		batchNumStorageIdx = 0
	)

	errTest := errors.New("test error")
	transform := func(ctx context.Context, in []ItemWithProps) ([]ItemWithProps, error) {
		if in == nil {
			return nil, io.EOF
		}

		var batchNum int
		ls := GetLocalStorage(ctx)
		val, ok := ls.Get(batchNumStorageIdx)
		if ok {
			batchNum = val.(int)
		}

		ls.Put(batchNumStorageIdx, batchNum+1)
		if batchNum == errorOnBatchNum {
			return nil, errTest
		}

		return in, nil
	}

	oc := &outputCapturer{}
	p := NewPipeline(
		NewStage("input", nil, sliceAsInputFunc(stringSliceToInterfaceSlice(replicateString("test", numInputItems)), processBatchSize*4), 1, -1, -1),
		NewStage("process", nil, transform, 1, 32, processBatchSize),
		NewStage("output", nil, oc.captureOutput, 1, 32, outBatchSize),
	)

	p.Start(context.Background())
	err := p.Wait()
	require.Equal(t, err, errTest)
}

func TestAbortPipeline(t *testing.T) {
	const parallelism = 50

	infiniteInput := func(_ context.Context, in []ItemWithProps) ([]ItemWithProps, error) {
		return []ItemWithProps{NewItemWithNoProps(0)}, nil
	}

	transform := func(_ context.Context, in []ItemWithProps) ([]ItemWithProps, error) {
		if in == nil {
			return nil, io.EOF
		}

		return []ItemWithProps{NewItemWithNoProps(in[0].GetItem().(int) + 1)}, nil
	}

	oc := &outputCapturer{}
	p := NewPipeline(
		NewStage("input", nil, infiniteInput, parallelism, -1, -1),
		NewStage("transform0to1", nil, transform, parallelism, 2048, 1),
		NewStage("transform1to2", nil, transform, parallelism, 2048, 1),
		NewStage("transform2to3", nil, transform, parallelism, 2048, 1),
		NewStage("transform3to4", nil, transform, parallelism, 2048, 1),
		NewStage("output", nil, oc.captureOutput, 1, 2048, 1),
	)

	p.Start(context.Background())

	time.Sleep(100 * time.Millisecond)
	p.Abort()
	err := p.Wait()
	require.Equal(t, ErrPipelineAborted, err)
}

func TestMassParallelism(t *testing.T) {
	const parallelism = 50
	const numItems = 128 * 1024
	const maxInBatchSize = 128

	seed := time.Now().Unix()
	t.Run("seed_"+strconv.FormatInt(seed, 10), func(t *testing.T) {
		r := rand.New(rand.NewSource(seed))
		inCh := make(chan []ItemWithProps, parallelism*2)
		go func() {
			defer close(inCh)
			for i := 0; i < numItems; {
				batchSize := r.Intn(maxInBatchSize-1) + 1

				if batchSize > numItems-i {
					batchSize = numItems - i
				}

				batch := make([]ItemWithProps, batchSize)
				for j := 0; j < batchSize; j++ {
					batch[j] = NewItemWithNoProps(0)
				}

				i += batchSize
				inCh <- batch
			}
		}()

		infiniteInput := func(_ context.Context, in []ItemWithProps) ([]ItemWithProps, error) {
			batch, ok := <-inCh

			if !ok {
				return nil, io.EOF
			}

			return batch, nil
		}

		transform := func(_ context.Context, in []ItemWithProps) ([]ItemWithProps, error) {
			if in == nil {
				return nil, nil
			}

			out := make([]ItemWithProps, len(in))
			for i, curr := range in {
				out[i] = NewItemWithNoProps(curr.GetItem().(int) + 1)
			}

			return out, nil
		}

		oc := &outputCapturer{}
		p := NewPipeline(
			NewStage("input", nil, infiniteInput, parallelism, -1, -1),
			NewStage("transform0to1", nil, transform, parallelism, 2048, 32),
			NewStage("transform1to2", nil, transform, parallelism, 2048, 32),
			NewStage("transform2to3", nil, transform, parallelism, 2048, 32),
			NewStage("transform3to4", nil, transform, parallelism, 2048, 32),
			NewStage("output", nil, oc.captureOutput, 1, 2048, 32),
		)

		inputToOutputStage, err := p.GetInputChannel("output")
		require.NoError(t, err)

		testInjectionItem := NewItemWithNoProps("Test Inject")
		inputToOutputStage <- []ItemWithProps{testInjectionItem}

		p.Start(context.Background())
		err = p.Wait()
		require.NoError(t, err)
		require.Equal(t, numItems+1, len(oc.results))

		assert.Equal(t, testInjectionItem, oc.results[0])

		for i := 1; i < len(oc.results); i++ {
			assert.Equal(t, 4, oc.results[i].GetItem().(int))
		}
	})
}
