// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipeline

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// SourceFunc is a function that will return a new row for each successive call until all it's rows are exhausted, at
// which point io.EOF should be returned
type SourceFunc func() (row.Row, ImmutableProperties, error)

// ProcFuncForSourceFunc is a helper method that creates an InFunc for a given SourceFunc.  It takes care of channel
// processing, stop conditions, and error handling.
func ProcFuncForSourceFunc(sourceFunc SourceFunc) InFunc {
	return func(p *Pipeline, ch chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, noMoreChan <-chan struct{}) {
		defer close(ch)

		for !p.IsStopping() {
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
					badRowChan <- &TransformRowFailure{table.GetBadRowRow(err), nil, "reader", err.Error()}
				} else {
					p.StopWithErr(err)
					return
				}
			} else if r == nil {
				panic("Readers should not be returning nil without error.  io.EOF should be used when done.")
			}

			if r != nil {
				select {
				case ch <- RowWithProps{r, props}:
				case <-p.stopChan:
					return
				}
			}
		}
	}
}

// ProcFuncForReader adapts a standard TableReader to work as an InFunc for a pipeline
func ProcFuncForReader(ctx context.Context, rd table.TableReader) InFunc {
	return ProcFuncForSourceFunc(func() (row.Row, ImmutableProperties, error) {
		r, err := rd.ReadRow(ctx)

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
		for {
			if p.IsStopping() {
				return
			}

			select {
			case r, ok := <-ch:
				if ok {
					err := sinkFunc(r.Row, r.Props)

					if err != nil {
						if table.IsBadRow(err) ||
							sql.ErrPrimaryKeyViolation.Is(err) ||
							sql.ErrUniqueKeyViolation.Is(err) ||
							errors.Is(err, editor.ErrDuplicateKey) {
							badRowChan <- &TransformRowFailure{r.Row, nil, "writer", err.Error()}
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

// SourceFuncForRows returns a source func that yields the rows given in order. Suitable for very small result sets
// that are statically defined or otherwise fit easily into memory.
func SourceFuncForRows(rows []row.Row) SourceFunc {
	idx := 0
	return func() (row.Row, ImmutableProperties, error) {
		if idx >= len(rows) {
			return nil, NoProps, io.EOF
		}
		r := rows[idx]
		idx++
		return r, NoProps, nil
	}
}

// ProcFuncForWriter adapts a standard TableWriter to work as an OutFunc for a pipeline
func ProcFuncForWriter(ctx context.Context, wr table.TableWriter) OutFunc {
	return ProcFuncForSinkFunc(func(r row.Row, props ReadableMap) error {
		return wr.WriteRow(ctx, r)
	})
}

// InFuncForChannel returns an InFunc that reads off the channel given.
func InFuncForChannel(rowChan <-chan row.Row) InFunc {
	return func(p *Pipeline, ch chan<- RowWithProps, badRowChan chan<- *TransformRowFailure, noMoreChan <-chan struct{}) {
		defer close(ch)

		more := true
		for more {
			if p.IsStopping() {
				return
			}

			select {
			case <-noMoreChan:
				more = false
			case r, ok := <-rowChan:
				if ok {
					ch <- RowWithProps{Row: r, Props: NoProps}
				} else {
					return
				}
			}
		}

		// no more data will be written to rowChan, but still need to make sure what was written is drained.
		if !more {
			for {
				if p.IsStopping() {
					return
				}

				select {
				case r, ok := <-rowChan:
					if ok {
						ch <- RowWithProps{Row: r, Props: NoProps}
					} else {
						return
					}
				default:
					return
				}
			}
		}
	}
}
