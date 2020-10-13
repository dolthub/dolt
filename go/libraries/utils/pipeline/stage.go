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

package pipeline

import (
	"context"
	"io"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// StageInitFunc is an initialization call made by each go routine executing the stage of the pipeline
type StageInitFunc func(ctx context.Context, stageRoutineIndex int) error

// StageFunc takes a batch of items, and returns a new batch of items that have been transformed.  The first StageFunc
// in a pipeline will receive nil input batches, and should produce batches to be processed by the pipeline.  Other stages
// in the pipeline will only receive a nil input batch as a signal to flush any items that it is bufferring internally.
type StageFunc func(ctx context.Context, inBatch []ItemWithProps) ([]ItemWithProps, error)

// Stage is a parallelizable portion of a pipeline which reads data from an input channel, transforms it, and then
// writes it to an output channel. The first stage of a pipeline will not read from it's input channel, and the last
// stage of a pipeline should only write ErrorItems
type Stage struct {
	name         string
	initFunc     StageInitFunc
	stageFunc    StageFunc
	parallelism  int
	inBufferSize int
	inBatchSize  int
	outBatchSize int
	inCh         chan []ItemWithProps
	outCh        chan []ItemWithProps
	p            *Pipeline
}

// NewStage creates a new pipeline
func NewStage(name string, initFunc StageInitFunc, stageFunc StageFunc, parallelism, inBufferSize, inBatchSize int) *Stage {
	return &Stage{
		name:         name,
		initFunc:     initFunc,
		stageFunc:    stageFunc,
		parallelism:  parallelism,
		inBufferSize: inBufferSize,
		inBatchSize:  inBatchSize,
	}
}

// init sets up the stages internal state so it can run within the given pipeline
func (s *Stage) init(outBatchSize int, out chan []ItemWithProps, p *Pipeline) chan []ItemWithProps {
	var in chan []ItemWithProps

	if s.inBufferSize > 0 {
		in = make(chan []ItemWithProps, s.inBufferSize)
	}

	s.outCh = out
	s.inCh = in
	s.outBatchSize = outBatchSize
	s.p = p

	return in
}

// start kicks off N go routines equivalent to the parallelism of the stage.
func (s *Stage) start(eg *errgroup.Group, ctx context.Context) {
	parallelism := 1
	if s.parallelism > 1 {
		parallelism = s.parallelism
	}

	stageWorkers := int32(parallelism)
	for i := 0; i < parallelism; i++ {
		routineIndex := i
		eg.Go(func() error {
			defer func() {
				if atomic.AddInt32(&stageWorkers, -1) == 0 {
					close(s.outCh)
				}
			}()

			ctx = context.WithValue(ctx, localStorageKey, &LocalStorage{})

			if s.initFunc != nil {
				err := s.initFunc(ctx, routineIndex)

				if err != nil {
					return err
				}
			}

			if s.inCh == nil {
				return s.runFirstStageInPipeline(ctx)
			} else {
				return s.runPipelineStage(ctx)
			}
		})
	}
}

// runFirstStageInPipeline calls the stageFunc on the first stage in the pipeline in order to produce batches
// which move through the pipeline.
func (s *Stage) runFirstStageInPipeline(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		iwp, err := s.stageFunc(ctx, nil)

		if err != nil {
			if err != io.EOF {
				return err
			}

			return nil
		}

		select {
		case <-ctx.Done(): // prevents potential write deadlock
			return nil
		case s.outCh <- iwp:
		}
	}
}

// runPipelineStage calls the stageFunc on batches that it reads from it's input channel
func (s *Stage) runPipelineStage(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		case inBatch, ok := <-s.inCh:
			if !ok {
				return nil
			}

			err := s.transformBatch(ctx, inBatch)

			if err != nil {
				return err
			}
		}
	}
}

func (s *Stage) transformBatch(ctx context.Context, inBatch []ItemWithProps) error {
	outBatch, err := s.stageFunc(ctx, inBatch)

	if err != nil {
		return err
	}

	for i := 0; i < len(outBatch); i++ {
		currBatch := outBatch[i : i+s.outBatchSize]

		select {
		case <-ctx.Done():
			return nil
		case s.outCh <- currBatch:
		}
	}

	return nil
}
