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
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// ErrUnknownStageName is the error returned when an unknown stage name is referenced
var ErrUnknownStageName = errors.New("unknown stage name")

// ErrPipelidneAborted is the error returned from Pipeline.Wait when Pipeline.Abort is called.
var ErrPipelineAborted = errors.New("pipeline aborted")

// Pipeline is a batch processor which takes data in batches and transforms it in stages
type Pipeline struct {
	nameToStage map[string]*Stage
	stages      []*Stage
	waitCh      <-chan []ItemWithProps
	eg          *errgroup.Group
	ctx         context.Context
}

// NewPipeline creates a new Pipeline from an ordered slice of stages. The first stage in the pipeline must produce data
// and each stage will pass data on to the next stage.
func NewPipeline(stages ...*Stage) *Pipeline {
	var nextInStage chan []ItemWithProps

	outBatchSize := -1
	nameToStage := make(map[string]*Stage)

	p := &Pipeline{waitCh: nextInStage}
	for i := len(stages) - 1; i >= 0; i-- {
		nextInStage = stages[i].init(outBatchSize, nextInStage, p)
		outBatchSize = stages[i].inBatchSize

		nameToStage[stages[i].name] = stages[i]
	}

	p.eg = nil
	p.stages = stages
	p.nameToStage = nameToStage

	return p
}

// Start the pipeline
func (p *Pipeline) Start(ctx context.Context) {
	if p.eg != nil {
		panic("started multiple times")
	}

	p.eg, p.ctx = errgroup.WithContext(ctx)
	for _, stage := range p.stages {
		stage.start(p.eg, p.ctx)
	}
}

// Wait waits for the pipeline to finish
func (p *Pipeline) Wait() error {
	return p.eg.Wait()
}

// Abort aborts the pipeline.  After abort is called the pipeline will continue running closing asynchronously
// Use Wait() if you want to wait for the pipeline to finish closing before continuing.
func (p *Pipeline) Abort() {
	p.eg.Go(func() error {
		return ErrPipelineAborted
	})
}

// GetInputChannel gets the input channel for a pipeline stage
func (p *Pipeline) GetInputChannel(stageName string) (chan []ItemWithProps, error) {
	stage, ok := p.nameToStage[stageName]

	if !ok {
		return nil, fmt.Errorf("%s: %w", stageName, ErrUnknownStageName)
	}

	return stage.inCh, nil
}
