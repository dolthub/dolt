// Copyright 2022 Dolthub, Inc.
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

package mvdata

import (
	"context"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/table"
)

// DataMoverPipeline is an errgroup based pipeline that reads rows from a reader and writes them to a destination with
// a writer.
type DataMoverPipeline struct {
	g   *errgroup.Group
	ctx context.Context
	rd  table.SqlRowReader
	wr  table.SqlRowWriter
}

func NewDataMoverPipeline(ctx context.Context, rd table.SqlRowReader, wr table.SqlRowWriter) *DataMoverPipeline {
	g, ctx := errgroup.WithContext(ctx)
	return &DataMoverPipeline{
		g:   g,
		ctx: ctx,
		rd:  rd,
		wr:  wr,
	}
}

func (e *DataMoverPipeline) Execute() error {
	parsedRowChan := make(chan sql.Row)

	e.g.Go(func() (err error) {
		defer func() {
			close(parsedRowChan)
			if cerr := e.rd.Close(e.ctx); cerr != nil {
				err = cerr
			}
		}()

		for {
			row, err := e.rd.ReadSqlRow(e.ctx)
			if err == io.EOF {
				return nil
			}

			if err != nil {
				return err
			}

			select {
			case <-e.ctx.Done():
				return e.ctx.Err()
			case parsedRowChan <- row:
			}
		}
	})

	e.g.Go(func() (err error) {
		defer func() {
			if cerr := e.wr.Close(e.ctx); cerr != nil {
				err = cerr
			}
		}()

		for r := range parsedRowChan {
			select {
			case <-e.ctx.Done():
				return e.ctx.Err()
			default:
				err := e.wr.WriteSqlRow(e.ctx, r)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})

	return e.g.Wait()
}
