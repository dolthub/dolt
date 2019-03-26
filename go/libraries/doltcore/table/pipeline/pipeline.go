package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"sync"
	"sync/atomic"
)

// InFunc is a pipeline input function that reads row data from a source and puts it in a channel.
type InFunc func(p *Pipeline, ch chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, noMoreChan <-chan struct{})

// OutFUnc is a pipeline output function that takes the data the pipeline has processed off of the channel.
type OutFunc func(p *Pipeline, ch <-chan RowWithProps, badRowChan chan<- *TransformRowFailure)

// BadRowCallback is a callback function that is called when a bad row is encountered.  returning true from this
// function when called will quit the entire pipeline
type BadRowCallback func(*TransformRowFailure) (quit bool)

// Pipeline is a struct that manages the operation of a row processing pipeline, where data is read from some source
// and written to a channel by the InFunc. An optional series of transformation functions read from this output as their
// input, passing output to the next stage, ultimately to the OutFunc. Each transform has a name, and is referred to as
// a stage in the pipeline.
//
// Pipelines must be cleaned up by a call to either Wait, Abort, or StopWithError, all of which run any deferred
// functions registered with the pipeline via calls to RunAfter (e.g. closing readers and writers).
type Pipeline struct {
	// A wait group that will block until the pipeline is done.
	wg *sync.WaitGroup
	// A channel that will receive a message when the pipeline stops.
	stopChan chan struct{}
	// A channel for consumers to write to when there are no more input rows to process.
	noMoreChan chan struct{}
	// An error in the pipeline's operation, accessible after it finishes.
	atomicErr atomic.Value
	// A map of stage name to input channel.
	stageInChan map[string]chan RowWithProps
	// A set of functions to run when the pipeline finishes.
	runAfterFuncs []func()
	// Start is a function used to start pipeline processing.  The Pipeline will be created in an unstarted state.
	Start func()
}

// InsertRow will insert a row at a particular stage in the pipeline
func (p *Pipeline) InsertRow(stageName string, r row.Row) bool {
	ch, ok := p.stageInChan[stageName]

	if !ok {
		return false
	}

	ch <- RowWithProps{r, NoProps}
	return true
}

// Abort signals the pipeline to stop processing.
func (p *Pipeline) Abort() {
	defer func() {
		recover() // ignore multiple calls to close channels
	}()

	for _, f := range p.runAfterFuncs {
		defer f()
	}

	close(p.stopChan)
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

// Schedules the given function to run after the pipeline completes.
func (p *Pipeline) RunAfter(f func()) {
	p.runAfterFuncs = append(p.runAfterFuncs, f)
}

// Wait will wait for the pipeline to complete and return any erorr that occurred during its execution.
func (p *Pipeline) Wait() error {
	for _, f := range p.runAfterFuncs {
		defer f()
	}

	p.wg.Wait()

	atomicErr := p.atomicErr.Load()

	if atomicErr != nil {
		return atomicErr.(error)
	}

	return nil
}

//NewAsyncPipeline creates a Pipeline from a given InFunc, OutFunc, TransformCollection, and a BadRowCallback
func NewAsyncPipeline(processInputs InFunc, processOutputs OutFunc, stages *TransformCollection, badRowCB BadRowCallback) (pipeline *Pipeline) {
	var wg sync.WaitGroup

	in := make(chan RowWithProps, 1024)
	badRowChan := make(chan *TransformRowFailure, 1024)
	stopChan := make(chan struct{})
	noMoreChan := make(chan struct{})
	stageInChan := make(map[string]chan RowWithProps)

	curr := in
	if stages != nil {
		for i := 0; i < stages.NumTransforms(); i++ {
			stage := stages.TransformAt(i)
			stageInChan[stage.Name] = curr
			curr = transformAsync(stage.Func, &wg, curr, badRowChan, stopChan)
		}
	}

	p := &Pipeline{wg: &wg, stopChan: stopChan, noMoreChan: noMoreChan, stageInChan: stageInChan}

	wg.Add(2)
	go func() {
		defer wg.Done()
		p.processBadRows(badRowCB, badRowChan)
	}()
	go func() {
		defer wg.Done()
		processOutputs(p, curr, badRowChan)
	}()

	p.Start = func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processInputs(p, in, badRowChan, noMoreChan)
		}()
	}

	return p
}

// Runs the ansync transform function given with the input channel given and returns its output channel.
func transformAsync(transformer TransformFunc, wg *sync.WaitGroup, inChan chan RowWithProps, badRowChan chan<- *TransformRowFailure, stopChan <-chan struct{}) chan RowWithProps {
	wg.Add(1)
	outChan := make(chan RowWithProps, 1024)

	go func() {
		defer wg.Done()
		defer close(outChan)

		transformer(inChan, outChan, badRowChan, stopChan)
	}()

	return outChan
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

func (p *Pipeline) processBadRows(badRowCB BadRowCallback, badRowChan <-chan *TransformRowFailure) {
	if badRowCB != nil {
		for {
			select {
			case bRow, ok := <-badRowChan:
				if ok {
					quit := badRowCB(bRow)

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
