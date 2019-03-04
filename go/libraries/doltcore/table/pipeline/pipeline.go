package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"sync"
	"sync/atomic"
)

type ProcFunc func(p *Pipeline, ch chan RowWithProps, badRowChan chan<- *TransformRowFailure)
type BadRowCallback func(*TransformRowFailure) (quit bool)

type Pipeline struct {
	wg        *sync.WaitGroup
	stopChan  chan bool
	atomicErr atomic.Value
	transInCh map[string]chan RowWithProps

	Start func()
}

func (p *Pipeline) InsertRow(name string, r row.Row) bool {
	ch, ok := p.transInCh[name]

	if !ok {
		return false
	}

	ch <- RowWithProps{r, NoProps}
	return true
}

func (p *Pipeline) Abort() {
	defer func() {
		recover()
	}()

	close(p.stopChan)
}

func (p *Pipeline) Wait() error {
	p.wg.Wait()

	atomicErr := p.atomicErr.Load()

	if atomicErr != nil {
		return atomicErr.(error)
	}

	return nil
}

func NewAsyncPipeline(processInputs, processOutputs ProcFunc, transforms *TransformCollection, badRowCB BadRowCallback) (pipeline *Pipeline) {
	var wg sync.WaitGroup

	in := make(chan RowWithProps, 1024)
	badRowChan := make(chan *TransformRowFailure, 1024)
	stopChan := make(chan bool)
	transInCh := make(map[string]chan RowWithProps)

	curr := in
	if transforms != nil {
		for i := 0; i < transforms.NumTransforms(); i++ {
			nt := transforms.TransformAt(i)
			transInCh[nt.Name] = curr
			curr = transformAsync(nt.Func, &wg, curr, badRowChan, stopChan)
		}
	}

	p := &Pipeline{&wg, stopChan, atomic.Value{}, transInCh, nil}

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
			processInputs(p, in, badRowChan)
		}()
	}

	return p
}

func transformAsync(transformer TransformFunc, wg *sync.WaitGroup, inChan chan RowWithProps, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool) chan RowWithProps {
	wg.Add(1)
	outChan := make(chan RowWithProps, 256)

	go func() {
		defer wg.Done()
		defer close(outChan)

		transformer(inChan, outChan, badRowChan, stopChan)
	}()

	return outChan
}

func (p Pipeline) StopWithErr(err error) {
	p.atomicErr.Store(err)
	close(p.stopChan)
}

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
						close(p.stopChan)
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
