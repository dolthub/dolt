package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"sync"
	"sync/atomic"
)

// Buffer size of processing channels created by the pipeline
const channelSize = 1024

// InFunc is a pipeline input function that reads row data from a source and puts it in a channel.
type InFunc func(p *Pipeline, ch chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, noMoreChan <-chan struct{})

// OutFunc is a pipeline output function that takes the data the pipeline has processed off of the channel.
type OutFunc func(p *Pipeline, ch <-chan RowWithProps, badRowChan chan<- *TransformRowFailure)

// BadRowCallback is a callback function that is called when a bad row is encountered.  returning true from this
// function when called will quit the entire pipeline
type BadRowCallback func(*TransformRowFailure) (quit bool)

// Pipeline is a struct that manages the operation of a row processing pipeline, where data is read from some source
// and written to a channel by the InFunc. An optional series of transformation functions read from this output as their
// input, passing output to the next stage, ultimately to the OutFunc. Each transform has a name, and is referred to as
// a stage in the pipeline.
//
// Pipelines can be constructed in phases, with different call sites adding transformations or even redirecting output
// as required. Once a pipeline is started with Start(), all configuration methods will panic.
//
// Pipelines must be cleaned up by a call to either Wait, Abort, or StopWithError, all of which run any deferred
// functions registered with the pipeline via calls to RunAfter (e.g. closing readers and writers).
//
// Ironically, not even a little thread safe.
type Pipeline struct {
	// A wait group that will block until the pipeline is done.
	wg *sync.WaitGroup
	// A channel that will receive a message when the pipeline stops.
	stopChan chan struct{}
	// A channel for consumers to write to when there are no more input rows to process.
	noMoreChan chan struct{}
	// A channel for consumers to read from to handle bad rows.
	badRowChan chan *TransformRowFailure
	// A function to run on rows that cannot be transformed.
	badRowCB BadRowCallback
	// An error in the pipeline's operation, accessible after it finishes.
	atomicErr atomic.Value
	// The input function for the pipeline.
	inFunc InFunc
	// The output function for the pipeline.
	outFunc OutFunc
	// The series of transformations to apply, each of which has a name called the "stage" of the pipeline
	stages *TransformCollection
	// A map of stage name to input channel.
	inputChansByStageName map[string]chan RowWithProps
	// A collection of synthetic rows to insert into the pipeline at a particular stage, before any other pipelined
	// input arrives to that stage.
	syntheticRowsByStageName map[string][]RowWithProps
	// A set of functions to run when the pipeline finishes.
	runAfterFuncs []func()
	// Whether the pipeline is currently running
	isRunning bool
}

//NewAsyncPipeline creates a Pipeline from a given InFunc, OutFunc, TransformCollection, and a BadRowCallback.
func NewAsyncPipeline(inFunc InFunc, outFunc OutFunc, stages *TransformCollection, badRowCB BadRowCallback) *Pipeline {
	var wg sync.WaitGroup

	return &Pipeline{
		wg:                       &wg,
		inFunc:                   inFunc,
		outFunc:                  outFunc,
		stages:                   stages,
		badRowCB:                 badRowCB,
		badRowChan:               make(chan *TransformRowFailure, channelSize),
		stopChan:                 make(chan struct{}),
		noMoreChan:               make(chan struct{}),
		inputChansByStageName:    make(map[string]chan RowWithProps),
		syntheticRowsByStageName: make(map[string][]RowWithProps),
	}
}

// NewPartialPipeline creates a pipeline stub that doesn't have an output func set on it yet. An OutFunc must be
// applied via a call to SetOutput before calling Start().
func NewPartialPipeline(inFunc InFunc) *Pipeline {
	return NewAsyncPipeline(inFunc, nil, &TransformCollection{}, nil)
}

// AddStage adds a new named transform to the set of stages
func (p *Pipeline) AddStage(stage NamedTransform) {
	if p.isRunning {
		panic("cannot add stages to a running pipeline")
	}

	p.stages.AppendTransforms(stage)
}

// SetOutput sets the output function to the function given
func (p *Pipeline) SetOutput(outFunc OutFunc) {
	if p.isRunning {
		panic("cannot set output on a running pipeline")
	}

	p.outFunc = outFunc
}

// SetBadRowCallback sets the callback to run when a bad row is encountered to the callback given
func (p *Pipeline) SetBadRowCallback(callback BadRowCallback) {
	if p.isRunning {
		panic("cannot set bad row callback on a running pipeline")
	}

	p.badRowCB = callback
}

// InjectRow injects a row at a particular stage in the pipeline. The row will be processed before other pipeline input
// arrives.
func (p *Pipeline) InjectRow(stageName string, r row.Row) {
	p.InjectRowWithProps(stageName, r, nil)
}

func (p *Pipeline) InjectRowWithProps(stageName string, r row.Row, props map[string]interface{}) {
	if p.isRunning {
		panic("cannot inject rows into a running pipeline")
	}

	var validStageName bool
	for _, stage := range p.stages.Transforms {
		if stage.Name == stageName {
			validStageName = true
			break
		}
	}
	if !validStageName {
		panic("unknown stage name " + stageName)
	}

	_, ok := p.syntheticRowsByStageName[stageName]
	if !ok {
		p.syntheticRowsByStageName[stageName] = make([]RowWithProps, 0, 1)
	}

	rowWithProps := NewRowWithProps(r, props)
	p.syntheticRowsByStageName[stageName] = append(p.syntheticRowsByStageName[stageName], rowWithProps)
}

// Schedules the given function to run after the pipeline completes.
func (p *Pipeline) RunAfter(f func()) {
	if p.isRunning {
		panic("cannot add a RunAfter function to a running pipeline")
	}

	p.runAfterFuncs = append(p.runAfterFuncs, f)
}

// NoMore signals that the pipeline has no more input to process. Must be called exactly once by the consumer when there
// are no more input rows to process.
func (p *Pipeline) NoMore() {
	defer func() {
		// TODO zachmu: there is a bug in pipeline execution where a limit of 1 causes NoMore to be called more than
		//  once. This should be an error we don't recover from.
		recover()
	}()

	close(p.noMoreChan)
}

// Starts the pipeline processing. Panics if the pipeline hasn't been set up completely yet.
func (p *Pipeline) Start() {
	if p.isRunning {
		panic("pipeline already started")
	}

	if p.inFunc == nil || p.outFunc == nil {
		panic("pipeline started without input or output func")
	}

	in := make(chan RowWithProps, channelSize)
	p.stopChan = make(chan struct{})

	// Start all the transform stages, chaining the output of one to the input of the next.
	curr := in
	if p.stages != nil {
		for i := 0; i < p.stages.NumTransforms(); i++ {
			stage := p.stages.TransformAt(i)
			p.inputChansByStageName[stage.Name] = curr
			curr = transformAsync(stage.Func, p.wg, curr, p.badRowChan, p.stopChan)
		}
	}

	// Inject all synthetic rows requested into their appropriate input channels.
	for stageName, injectedRows := range p.syntheticRowsByStageName {
		ch := p.inputChansByStageName[stageName]
		for _, rowWithProps := range injectedRows {
			ch <- rowWithProps
		}
	}

	// Start all the async processing: the sink, the error handlers, then the source.
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.processBadRows()
	}()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.outFunc(p, curr, p.badRowChan)
	}()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.inFunc(p, in, p.badRowChan, p.noMoreChan)
	}()

	p.isRunning = true
}

// Wait waits for the pipeline to complete and return any error that occurred during its execution.
func (p *Pipeline) Wait() error {
	if !p.isRunning {
		panic("cannot Wait() a pipeline before a call to Start()")
	}

	for _, f := range p.runAfterFuncs {
		defer f()
	}

	p.wg.Wait()
	p.isRunning = false

	atomicErr := p.atomicErr.Load()

	if atomicErr != nil {
		return atomicErr.(error)
	}

	return nil
}

// Abort signals the pipeline to stop processing.
func (p *Pipeline) Abort() {
	defer func() {
		p.isRunning = false
	}()

	defer func() {
		recover() // ignore multiple calls to close channels
	}()

	for _, f := range p.runAfterFuncs {
		defer f()
	}

	close(p.stopChan)
}

// StopWithErr provides a method by the pipeline can be stopped when an error is encountered.  This would typically be
// done in InFuncs and OutFuncs
func (p Pipeline) StopWithErr(err error) {
	p.atomicErr.Store(err)
	p.Abort()
}

// IsStopping returns true if the pipeline is currently stopping
func (p Pipeline) IsStopping() bool {
	// exit if stop
	select {
	case <-p.stopChan:
		return true

	default:
	}

	return false
}

// Processes all the errors that occur during the pipeline
func (p *Pipeline) processBadRows() {
	if p.badRowCB != nil {
		for {
			select {
			case bRow, ok := <-p.badRowChan:
				if ok {
					quit := p.badRowCB(bRow)

					if quit {
						p.Abort()
						return
					}
				} else {
					return
				}

			case <-p.stopChan:
				return
			}
		}
	}
}

// Runs the ansync transform function given with the input channel given and returns its output channel.
func transformAsync(transformer TransformFunc, wg *sync.WaitGroup, inChan <-chan RowWithProps, badRowChan chan<- *TransformRowFailure, stopChan <-chan struct{}) chan RowWithProps {
	wg.Add(1)
	outChan := make(chan RowWithProps, channelSize)

	go func() {
		defer wg.Done()
		defer close(outChan)

		transformer(inChan, outChan, badRowChan, stopChan)
	}()

	return outChan
}
