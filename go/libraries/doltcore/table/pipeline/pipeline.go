package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"io"
	"sync"
	"sync/atomic"
)

type BadRowCallback func(*TransformRowFailure) (quit bool)

type Pipeline struct {
	wg        *sync.WaitGroup
	stopChan  chan bool
	atomicErr atomic.Value
	transInCh map[string]chan *table.Row
}

func (p *Pipeline) InsertRow(name string, row *table.Row) bool {
	ch, ok := p.transInCh[name]

	if !ok {
		return false
	}

	ch <- row
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

func NewAsyncPipeline(rd table.TableReader, transforms *TransformCollection, wr table.TableWriter, badRowCB BadRowCallback) (pipeline *Pipeline, start func()) {
	var wg sync.WaitGroup

	in := make(chan *table.Row, 1024)
	badRowChan := make(chan *TransformRowFailure, 1024)
	stopChan := make(chan bool)
	transInCh := make(map[string]chan *table.Row)

	curr := in
	if transforms != nil {
		for i := 0; i < transforms.NumTransforms(); i++ {
			nt := transforms.TransformAt(i)
			transInCh[nt.Name] = curr
			curr = transformAsync(nt.Func, &wg, curr, badRowChan, stopChan)
		}
	}

	p := &Pipeline{&wg, stopChan, atomic.Value{}, transInCh}

	wg.Add(3)
	go p.processBadRows(badRowCB, badRowChan)
	go p.processOutputs(wr, curr, badRowChan)

	return p, func() {
		go p.processInputs(rd, in, badRowChan)
	}
}

func transformAsync(transformer TransformFunc, wg *sync.WaitGroup, inChan chan *table.Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool) chan *table.Row {
	wg.Add(1)
	outChan := make(chan *table.Row, 256)

	go func() {
		defer wg.Done()
		defer close(outChan)

		transformer(inChan, outChan, badRowChan, stopChan)
	}()

	return outChan
}

func (p *Pipeline) processInputs(rd table.TableReader, in chan<- *table.Row, badRowChan chan<- *TransformRowFailure) {
	defer close(in)
	defer p.wg.Done()

	for {
		// read row
		row, err := rd.ReadRow()

		// process read errors
		if err != nil {
			if err == io.EOF {
				if row == nil {
					return
				}
			} else if table.IsBadRow(err) {
				badRowChan <- &TransformRowFailure{table.GetBadRowRow(err), "reader", err.Error()}
			} else {
				p.atomicErr.Store(err)
				close(p.stopChan)
				return
			}
		} else if row == nil {
			panic("Readers should not be returning nil without error.  io.EOF should be used when done.")
		}

		// exit if stop
		select {
		case <-p.stopChan:
			return

		default:
		}

		if row != nil {
			in <- row
		}
	}
}

func (p *Pipeline) processOutputs(wr table.TableWriter, out <-chan *table.Row, badRowChan chan<- *TransformRowFailure) {
	defer close(badRowChan)
	defer p.wg.Done()

	for {
		select {
		case row, ok := <-out:
			if ok {
				err := wr.WriteRow(row)

				if err != nil {
					if table.IsBadRow(err) {
						badRowChan <- &TransformRowFailure{row, "writer", err.Error()}
					} else {
						p.atomicErr.Store(err)
						close(p.stopChan)
						return
					}
				}
			} else {
				return
			}

		case <-p.stopChan:
			return
		}
	}
}

func (p *Pipeline) processBadRows(badRowCB BadRowCallback, badRowChan <-chan *TransformRowFailure) {
	defer p.wg.Done()

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
