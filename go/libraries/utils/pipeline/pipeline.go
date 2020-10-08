package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

const LocalStorageKey = "ls"

var ErrUnknownStageName = errors.New("unknown stage name")

type LocalStorage map[int]interface{}

func (ls LocalStorage) Get(id int) (interface{}, bool) {
	val, ok := ls[id]
	return val, ok
}

func (ls LocalStorage) Put(id int, val interface{}) {
	ls[id] = val
}

// ErrInvalidItemWrittenAtFinalStageInPipeline is indicative of a bug.  Only error items should be written to the output
// channel of the final stage in a pipeline
var ErrInvalidItemWrittenAtFinalStageInPipeline = errors.New("invalid item written at last stage in pipeline")

type StageInitFunc func(ctx context.Context, stageRoutineIndex int)

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
func (s *Stage) start(ctx context.Context, wg *sync.WaitGroup) {
	parallelism := 1
	if s.parallelism > 1 {
		parallelism = s.parallelism
	}

	wg.Add(parallelism)
	stageWorkers := int32(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer wg.Done()
			defer func() {
				if atomic.AddInt32(&stageWorkers, -1) == 0 {
					if s.inCh != nil {
						// flush buffered items
						s.transformBatch(ctx, nil)
					}

					close(s.outCh)
				}
			}()

			ctx = context.WithValue(ctx, LocalStorageKey, &LocalStorage{})

			if s.initFunc != nil {
				s.initFunc(ctx, i)
			}

			if s.inCh == nil {
				s.runFirstStageInPipeline(ctx)
			} else {
				s.runPipelineStage(ctx)
			}
		}()
	}
}

// runFirstStageInPipeline calls the stageFunc on the first stage in the pipeline in order to produce batches
// which move through the pipeline.
func (s *Stage) runFirstStageInPipeline(ctx context.Context) {
	for atomic.LoadInt32(&s.p.aborted) == 0 {
		iwp, err := s.stageFunc(ctx, nil)

		if err != nil {
			if err != io.EOF {
				s.outCh <- []ItemWithProps{NewErrorItem(err, &s.p.errNum)}
			}

			return
		}

		s.outCh <- iwp
	}
	return
}

// runPipelineStage calls the stageFunc on batches that it reads from it's input channel
func (s *Stage) runPipelineStage(ctx context.Context) {
	for {
		inBatch, ok := <-s.inCh

		if !ok {
			return
		}

		if len(inBatch) == 1 {
			// check to see if this is an error item
			if errItem, isErr := inBatch[0].(ErrorItem); isErr {
				// filter out everything except the first error
				if errItem.ErrNumber == 1 {
					// send the error on to the next stage
					s.outCh <- inBatch
				}
				return
			}
		}

		err := s.transformBatch(ctx, inBatch)

		if err != nil {
			// if an error occurs write it as a special item to the pipeline and abort the pipeline.
			s.outCh <- []ItemWithProps{NewErrorItem(err, &s.p.errNum)}
			atomic.StoreInt32(&s.p.aborted, 1)
			return
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
		s.outCh <- currBatch
	}

	return nil
}

// Pipeline is a batch processor which takes data in batches and transforms it in stages
type Pipeline struct {
	wg          *sync.WaitGroup
	nameToStage map[string]*Stage
	stages      []*Stage
	waitCh      <-chan []ItemWithProps
	aborted     int32
	errNum      uint32
}

// NewPipeline creates a new Pipeline from an ordered slice of stages. The first stage in the pipeline must produce data
// and each stage will pass data on to the next stage.
func NewPipeline(stages []*Stage) *Pipeline {
	nextInStage := make(chan []ItemWithProps)

	outBatchSize := -1
	nameToStage := make(map[string]*Stage)

	p := &Pipeline{waitCh: nextInStage}
	for i := len(stages) - 1; i >= 0; i-- {
		nextInStage = stages[i].init(outBatchSize, nextInStage, p)
		outBatchSize = stages[i].inBatchSize

		nameToStage[stages[i].name] = stages[i]
	}

	p.wg = &sync.WaitGroup{}
	p.stages = stages
	p.nameToStage = nameToStage

	return p
}

// Start the pipeline
func (p *Pipeline) Start(ctx context.Context) {
	for _, stage := range p.stages {
		stage.start(ctx, p.wg)
	}
}

// Wait waits for the pipeline to finish
func (p *Pipeline) Wait() error {
	p.wg.Wait()
	items, ok := <-p.waitCh

	if !ok || len(items) == 0 {
		return nil
	}

	if len(items) != 1 {
		return ErrInvalidItemWrittenAtFinalStageInPipeline
	}

	if errItem, ok := items[0].(ErrorItem); !ok {
		return ErrInvalidItemWrittenAtFinalStageInPipeline
	} else {
		return errItem
	}
}

// Abort aborts the pipeline
func (p *Pipeline) Abort() {
	atomic.StoreInt32(&p.aborted, 1)
}

// GetInputChannel gets the input channel for a pipeline stage
func (p *Pipeline) GetInputChannel(stageName string) (chan []ItemWithProps, error) {
	stage, ok := p.nameToStage[stageName]

	if !ok {
		return nil, fmt.Errorf("%s: %w", stageName, ErrUnknownStageName)
	}

	return stage.inCh, nil
}
