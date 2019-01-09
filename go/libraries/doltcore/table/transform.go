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

type BadRowCallback func(*TransformRowFailure) (quit bool)

type TransformRowFailure struct {
	Row           *Row
	TransformName string
	Details       string
}

func (trf *TransformRowFailure) Error() string {
	return trf.TransformName + " failed processing"
}

func IsTransformFailure(err error) bool {
	_, ok := err.(*TransformRowFailure)
	return ok
}

func GetTransFailureTransName(err error) string {
	trf, ok := err.(*TransformRowFailure)

	if !ok {
		panic("Verify error using IsTransformFailure before calling this.")
	}

	return trf.TransformName
}

func GetTransFailureRow(err error) *Row {
	trf, ok := err.(*TransformRowFailure)

	if !ok {
		panic("Verify error using IsTransformFailure before calling this.")
	}

	return trf.Row

}
func GetTransFailureDetails(err error) string {
	trf, ok := err.(*TransformRowFailure)

	if !ok {
		panic("Verify error using IsTransformFailure before calling this.")
	}

	return trf.Details
}

type NamedTransform struct {
	Name string
	Func TransformFunc
}

type TransformCollection struct {
	Transforms []NamedTransform
}

func NewTransformCollection(namedTransforms ...NamedTransform) *TransformCollection {
	return &TransformCollection{namedTransforms}
}

func (tc *TransformCollection) AppendTransforms(nt NamedTransform) {
	tc.Transforms = append(tc.Transforms, nt)
}

func (tc *TransformCollection) NumTransforms() int {
	return len(tc.Transforms)
}

func (tc *TransformCollection) TransformAt(idx int) NamedTransform {
	return tc.Transforms[idx]
}

type Pipeline struct {
	wg        *sync.WaitGroup
	stopChan  chan bool
	atomicErr atomic.Value
	transInCh map[string]chan *Row
}

func (p *Pipeline) GetInChForTransf(name string) (chan *Row, bool) {
	ch, ok := p.transInCh[name]
	return ch, ok
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

func NewAsyncPipeline(rd TableReader, transforms *TransformCollection, wr TableWriter, badRowCB BadRowCallback) (pipeline *Pipeline, start func()) {
	var wg sync.WaitGroup

	in := make(chan *Row, 1024)
	badRowChan := make(chan *TransformRowFailure, 1024)
	stopChan := make(chan bool)
	transInCh := make(map[string]chan *Row)

	curr := in
	for i := 0; i < transforms.NumTransforms(); i++ {
		nt := transforms.TransformAt(i)
		transInCh[nt.Name] = curr
		curr = transformAsync(nt.Func, &wg, curr, badRowChan, stopChan)
	}

	p := &Pipeline{&wg, stopChan, atomic.Value{}, transInCh}

	wg.Add(3)
	go p.processBadRows(badRowCB, badRowChan)
	go p.processOutputs(wr, curr, badRowChan)

	return p, func() {
		go p.processInputs(rd, in, badRowChan)
	}
}

func transformAsync(transformer TransformFunc, wg *sync.WaitGroup, inChan chan *Row, badRowChan chan<- *TransformRowFailure, stopChan <-chan bool) chan *Row {
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
			} else if IsBadRow(err) {
				badRowChan <- &TransformRowFailure{GetBadRowRow(err), "reader", err.Error()}
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

func (p *Pipeline) processOutputs(wr TableWriter, out <-chan *Row, badRowChan chan<- *TransformRowFailure) {
	defer close(badRowChan)
	defer p.wg.Done()

	for {
		select {
		case row, ok := <-out:
			if ok {
				err := wr.WriteRow(row)

				if err != nil {
					if IsBadRow(err) {
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
