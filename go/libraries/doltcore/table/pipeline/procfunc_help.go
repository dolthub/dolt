package pipeline

import (
	"context"
	"io"
	"time"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
)

// SourceFunc is a function that will return a new row for each successive call until all it's rows are exhausted, at
// which point io.EOF should be returned
type SourceFunc func() (row.Row, ImmutableProperties, error)

// ProcFuncForSourceFunc is a helper method that creates an InFunc for a given SourceFunc.  It takes care of channel
// processing, stop conditions, and error handling.
func ProcFuncForSourceFunc(sourceFunc SourceFunc) InFunc {
	return func(p *Pipeline, ch chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, noMoreChan <-chan struct{}) {
		defer close(ch)

		for {
			select {
			case <-noMoreChan:
				return
			default:
				break
			}

			r, props, err := sourceFunc()

			// process read errors
			if err != nil {
				if err == io.EOF {
					if r == nil {
						return
					}
				} else if table.IsBadRow(err) {
					badRowChan <- &TransformRowFailure{table.GetBadRowRow(err), "reader", err.Error()}
				} else {
					p.StopWithErr(err)
					return
				}
			} else if r == nil {
				panic("Readers should not be returning nil without error.  io.EOF should be used when done.")
			}

			if p.IsStopping() {
				return
			}

			if r != nil {
				ch <- RowWithProps{r, props}
			}
		}
	}
}

// ProcFuncForReader adapts a standard TableReader to work as an InFunc for a pipeline
func ProcFuncForReader(rd table.TableReader) InFunc {
	return ProcFuncForSourceFunc(func() (row.Row, ImmutableProperties, error) {
		r, err := rd.ReadRow(context.TODO())

		return r, NoProps, err
	})
}

// SinkFunc is a function that will process the final transformed rows from a pipeline.  This function will be called
// once for every row that makes it through the pipeline
type SinkFunc func(row.Row, ReadableMap) error

// ProcFuncForSinkFunc is a helper method that creates an OutFunc for a given SinkFunc.  It takes care of channel
// processing, stop conditions, and error handling.
func ProcFuncForSinkFunc(sinkFunc SinkFunc) OutFunc {
	return func(p *Pipeline, ch <-chan RowWithProps, badRowChan chan<- *TransformRowFailure) {
		defer close(badRowChan)

		for {
			if p.IsStopping() {
				return
			}

			select {
			case r, ok := <-ch:
				if ok {
					err := sinkFunc(r.Row, r.Props)

					if err != nil {
						if table.IsBadRow(err) {
							badRowChan <- &TransformRowFailure{r.Row, "writer", err.Error()}
						} else {
							p.StopWithErr(err)
							return
						}
					}
				} else {
					return
				}

			case <-time.After(100 * time.Millisecond):
				// wake up and check stop condition
			}
		}
	}
}

// ProcFuncForWriter adapts a standard TableWriter to work as an InFunc for a pipeline
func ProcFuncForWriter(wr table.TableWriter) OutFunc {
	return ProcFuncForSinkFunc(func(r row.Row, props ReadableMap) error {
		return wr.WriteRow(context.TODO(), r)
	})
}
