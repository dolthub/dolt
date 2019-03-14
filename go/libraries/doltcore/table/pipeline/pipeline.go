package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"sync"
	"sync/atomic"
)

// InFunc is a pipeline input function that reads row data from a source and puts it in a channel.
type InFunc func(p *Pipeline, ch chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, noMoreChan <-chan NoValue)

// OutFUnc is a pipeline output function that takes the data the pipeline has processed off of the channel.
type OutFunc func(p *Pipeline, ch <-chan RowWithProps, badRowChan chan<- *TransformRowFailure)

// BadRowCallback is a callback function that is called when a bad row is encountered.  returning true from this
// function when called will quit the entire pipeline
type BadRowCallback func(*TransformRowFailure) (quit bool)

// NoValue is a signal type to indicate that channels of this type should never expect to receive values, only control flow (close())
type NoValue struct{}

// Pipeline is a struct that manages the operation of a row processing pipeline, where data is read from some source
// and written to a channel by the InFunc, a transform would then read the data off of their input channel, and write
// the transformed data to their output channel.  A series of transforms can be applied until the transformed data
// reaches the OutFunc which takes the data off the channel and would typically store the result somewhere.
type Pipeline struct {
	wg         *sync.WaitGroup
	stopChan   chan NoValue
	noMoreChan chan NoValue

	atomicErr atomic.Value
	transInCh map[string]chan RowWithProps

	// Start is a function used to start pipeline processing.  The Pipeline will be created in an unstarted state.
	Start func()
}

// InsertRow will insert a row at a particular stage in the pipeline
func (p *Pipeline) InsertRow(name string, r row.Row) bool {
	ch, ok := p.transInCh[name]

	if !ok {
		return false
	}

	ch <- RowWithProps{r, NoProps}
	return true
}

// Abort signals the pipeline to stop processing.
func (p *Pipeline) Abort() {
	defer func() {
		recover() // ignore multiple calls to close channel
	}()

	close(p.stopChan)
}

// NoMore signals that the pipeline has no more input to process. Must be called exactly once when there are no more
// input rows to process.
func (p *Pipeline) NoMore() {
	close(p.noMoreChan)
}

// Wait will wait for the pipeline to complete
func (p *Pipeline) Wait() error {
	p.wg.Wait()

	atomicErr := p.atomicErr.Load()

	if atomicErr != nil {
		return atomicErr.(error)
	}

	return nil
}

//NewAsyncPipeline creates a Pipeline from a given InFunc, OutFunc, TransformCollection, and a BadRowCallback
func NewAsyncPipeline(processInputs InFunc, processOutputs OutFunc, transforms *TransformCollection, badRowCB BadRowCallback) (pipeline *Pipeline) {
	var wg sync.WaitGroup

	in := make(chan RowWithProps, 1024)
	badRowChan := make(chan *TransformRowFailure, 1024)
	stopChan := make(chan NoValue)
	noMoreChan := make(chan NoValue)
	transInCh := make(map[string]chan RowWithProps)

	curr := in
	if transforms != nil {
		for i := 0; i < transforms.NumTransforms(); i++ {
			nt := transforms.TransformAt(i)
			transInCh[nt.Name] = curr
			curr = transformAsync(nt.Func, &wg, curr, badRowChan, stopChan)
		}
	}

	p := &Pipeline{&wg, stopChan, noMoreChan, atomic.Value{}, transInCh, nil}

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

func transformAsync(transformer TransformFunc, wg *sync.WaitGroup, inChan chan RowWithProps, badRowChan chan<- *TransformRowFailure, stopChan <-chan NoValue) chan RowWithProps {
	wg.Add(1)
	outChan := make(chan RowWithProps, 256)

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
