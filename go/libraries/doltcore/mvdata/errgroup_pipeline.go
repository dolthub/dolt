// Copyright 2023 Dolthub, Inc.
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

type ErrgroupPipeline struct {
	g   *errgroup.Group
	ctx context.Context

	parsedRowChan chan sql.Row
	rd            table.SqlRowReader
	wr            table.SqlTableWriter
}

func NewErrGroupPipeline(ctx context.Context, rd table.SqlRowReader, wr table.SqlTableWriter) *ErrgroupPipeline {
	g, ctx := errgroup.WithContext(ctx)
	return &ErrgroupPipeline{
		g:   g,
		ctx: ctx,

		parsedRowChan: make(chan sql.Row),
		rd:            rd,
		wr:            wr,
	}
}

func (e *ErrgroupPipeline) Execute() error {
	e.g.Go(func() (err error) {
		defer func() {
			close(e.parsedRowChan)
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
			case e.parsedRowChan <- row:
			}
		}
	})

	e.g.Go(func() (err error) {
		defer func() {
			if cerr := e.wr.Close(e.ctx); cerr != nil {
				err = cerr
			}
		}()

		for r := range e.parsedRowChan {
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
