package table

import (
	"io"
	"sync"
	"sync/atomic"
)

type TransFailCallback func(row *Row, errDetails string)
type TransformFunc func(inChan <-chan *Row, outChan chan<- *Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool)
type BulkTransformFunc func(rows []*Row, outChan chan<- *Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool)

type TransformedRowResult struct {
	RowData    *RowData
	Properties map[string]interface{}
}
type TransformRowFunc func(inRow *Row) (rowData []*TransformedRowResult, badRowDetails string)

func NewBulkTransformer(bulkTransFunc BulkTransformFunc) TransformFunc {
	return func(inChan <-chan *Row, outChan chan<- *Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool) {
		var rows []*Row
	RowLoop:
		for {
			select {
			case <-stopChan:
				return
			default:
			}

			select {
			case row, ok := <-inChan:
				if ok {
					rows = append(rows, row)
					break RowLoop
				} else {
					return
				}

			case <-stopChan:
				return
			}
		}

		bulkTransFunc(rows, outChan, badRowChan, stopChan)
	}
}

func NewRowTransformer(name string, transRowFunc TransformRowFunc) TransformFunc {
	return func(inChan <-chan *Row, outChan chan<- *Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool) {
		for {
			select {
			case <-stopChan:
				return
			default:
			}

			select {
			case row, ok := <-inChan:
				if ok {
					outRowData, badRowDetails := transRowFunc(row)
					outSize := len(outRowData)

					for i := 0; i < outSize; i++ {
						props := row.ClonedMergedProperties(outRowData[i].Properties)
						outRow := NewRowWithProperties(outRowData[i].RowData, props)
						outChan <- outRow
					}

					if badRowDetails != "" {
						badRowChan <- &TransformRowFailure{row, name, badRowDetails}
					}
				} else {
					return
				}

			case <-stopChan:
				return
			}
		}
	}
}

type BadRowCallback func(transfName string, row *Row, errDetails string) (quit bool)

type TransformRowFailure struct {
	BadRow        *Row
	TransformName string
	BadRowDetails string
}

type Pipeline struct {
	wg        *sync.WaitGroup
	stopChan  chan bool
	atomicErr atomic.Value
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

func StartAsyncPipeline(rd TableReader, transforms []TransformFunc, wr TableWriter, badRowCB BadRowCallback) *Pipeline {
	var wg sync.WaitGroup

	in := make(chan *Row, 1024)
	badRowChan := make(chan *TransformRowFailure, 1024)
	stopChan := make(chan bool)

	var curr <-chan *Row = in
	for _, transformer := range transforms {
		curr = transformAsync(transformer, &wg, curr, badRowChan, stopChan)
	}

	p := &Pipeline{&wg, stopChan, atomic.Value{}}

	wg.Add(3)
	go p.processBadRows(badRowCB, badRowChan)
	go p.processOutputs(wr, curr, badRowChan)
	go p.processInputs(rd, in, badRowChan)

	return p
}

func transformAsync(transformer TransformFunc, wg *sync.WaitGroup, inChan <-chan *Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool) <-chan *Row {
	wg.Add(1)
	outChan := make(chan *Row, 256)

	go func() {
		defer wg.Done()
		defer close(outChan)

		transformer(inChan, outChan, badRowChan, stopChan)
	}()

	return outChan
}

func (p *Pipeline) processInputs(rd TableReader, in chan<- *Row, badRowChan chan<- *TransformRowFailure) {
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
			} else if err == ErrBadRow {
				badRowChan <- &TransformRowFailure{row, "reader", err.Error()}
			} else {
				p.atomicErr.Store(err)
				close(p.stopChan)
				return
			}
		}

		// exit if stop
		select {
		case <-p.stopChan:
			return

		default:
		}

		in <- row
	}
}

func (p *Pipeline) processOutputs(wr TableWriter, out <-chan *Row, badRowChan chan<- *TransformRowFailure) {
	defer close(badRowChan)
	defer p.wg.Done()

	for {
		select {
		case row, ok := <-out:
			if ok {
				err := wr.WriteRow(row)

				if err != nil {
					if err == ErrBadRow {
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
					quit := badRowCB(bRow.TransformName, bRow.BadRow, bRow.BadRowDetails)

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
