package pipeline

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"io"
	"time"
)

type SourceFunc func() (row.Row, ImmutableProperties, error)

func ProcFuncForSourceFunc(sourceFunc SourceFunc) ProcFunc {
	return func(p *Pipeline, ch chan RowWithProps, badRowChan chan<- *TransformRowFailure) {
		defer close(ch)

		for {
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

func ProcFuncForReader(rd table.TableReader) ProcFunc {
	return ProcFuncForSourceFunc(func() (row.Row, ImmutableProperties, error) {
		r, err := rd.ReadRow()

		return r, NoProps, err
	})
}

type SinkFunc func(row.Row, ReadableMap) error

func ProcFuncForSinkFunc(sinkFunc SinkFunc) ProcFunc {
	return func(p *Pipeline, ch chan RowWithProps, badRowChan chan<- *TransformRowFailure) {
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
				return
			}
		}
	}
}

func ProcFuncForWriter(wr table.TableWriter) ProcFunc {
	return ProcFuncForSinkFunc(func(r row.Row, props ReadableMap) error {
		return wr.WriteRow(r)
	})
}
