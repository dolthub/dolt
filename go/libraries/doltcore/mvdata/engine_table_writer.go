// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"io"
	"sync/atomic"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// tableWriterStatUpdateRate is the number of writes that will process before the updated stats are displayed.
	tableWriterStatUpdateRate = 64 * 1024
)

// type SqlEngineTableWriter is a utility for importing a set of rows through the sql engine.
type SqlEngineTableWriter struct {
	se     *engine.SqlEngine
	sqlCtx *sql.Context

	tableName string
	database  string
	contOnErr bool
	force     bool

	statsCB noms.StatsCB
	stats   types.AppliedEditStats
	statOps int32

	importOption       TableImportOp
	tableSchema        sql.PrimaryKeySchema
	rowOperationSchema sql.PrimaryKeySchema
}

func NewSqlEngineTableWriter(ctx context.Context, dEnv *env.DoltEnv, createTableSchema, rowOperationSchema schema.Schema, options *MoverOptions, statsCB noms.StatsCB) (*SqlEngineTableWriter, error) {
	mrEnv, err := env.DoltEnvAsMultiEnv(ctx, dEnv)
	if err != nil {
		return nil, err
	}

	// Choose the first DB as the current one. This will be the DB in the working dir if there was one there
	var dbName string
	mrEnv.Iter(func(name string, _ *env.DoltEnv) (stop bool, err error) {
		dbName = name
		return true, nil
	})

	se, err := engine.NewSqlEngine(ctx, mrEnv, engine.FormatCsv, dbName, false, nil, false)
	if err != nil {
		return nil, err
	}
	defer se.Close()

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}

	dsess.DSessFromSess(sqlCtx.Session).EnableBatchedMode()

	err = sqlCtx.Session.SetSessionVariable(sqlCtx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return nil, err
	}

	var doltCreateTableSchema sql.PrimaryKeySchema
	if options.Operation == CreateOp {
		doltCreateTableSchema, err = sqlutil.FromDoltSchema(options.TableToWriteTo, createTableSchema)
		if err != nil {
			return nil, err
		}
	}

	doltRowOperationSchema, err := sqlutil.FromDoltSchema(options.TableToWriteTo, rowOperationSchema)
	if err != nil {
		return nil, err
	}

	return &SqlEngineTableWriter{
		se:        se,
		sqlCtx:    sqlCtx,
		contOnErr: options.ContinueOnErr,
		force:     options.Force,

		database:  dbName,
		tableName: options.TableToWriteTo,

		statsCB: statsCB,

		importOption:       options.Operation,
		tableSchema:        doltCreateTableSchema,
		rowOperationSchema: doltRowOperationSchema,
	}, nil
}

// Used by Dolthub API
func NewSqlEngineTableWriterWithEngine(ctx *sql.Context, eng *sqle.Engine, db dsqle.Database, createTableSchema, rowOperationSchema schema.Schema, options *MoverOptions, statsCB noms.StatsCB) (*SqlEngineTableWriter, error) {
	dsess.DSessFromSess(ctx.Session).EnableBatchedMode()

	err := ctx.Session.SetSessionVariable(ctx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	var doltCreateTableSchema sql.PrimaryKeySchema
	if options.Operation == CreateOp {
		doltCreateTableSchema, err = sqlutil.FromDoltSchema(options.TableToWriteTo, createTableSchema)
		if err != nil {
			return nil, err
		}
	}

	doltRowOperationSchema, err := sqlutil.FromDoltSchema(options.TableToWriteTo, rowOperationSchema)
	if err != nil {
		return nil, err
	}

	return &SqlEngineTableWriter{
		se:        engine.NewRebasedSqlEngine(eng, map[string]dsqle.SqlDatabase{db.Name(): db}),
		sqlCtx:    ctx,
		contOnErr: options.ContinueOnErr,
		force:     options.Force,

		database:  db.Name(),
		tableName: options.TableToWriteTo,
		statsCB:   statsCB,

		importOption:       options.Operation,
		tableSchema:        doltCreateTableSchema,
		rowOperationSchema: doltRowOperationSchema,
	}, nil
}

func (s *SqlEngineTableWriter) WriteRows(ctx context.Context, inputChannel chan sql.Row, badRowCb func(*pipeline.TransformRowFailure) bool) (err error) {
	err = s.forceDropTableIfNeeded()
	if err != nil {
		return err
	}

	_, _, err = s.se.Query(s.sqlCtx, fmt.Sprintf("START TRANSACTION"))
	if err != nil {
		return err
	}

	err = s.createOrEmptyTableIfNeeded()
	if err != nil {
		return err
	}

	updateStats := func(row sql.Row) {
		if row == nil {
			return
		}

		// If the length of the row does not match the schema then we have an update operation.
		if len(row) != len(s.rowOperationSchema.Schema) {
			oldRow := row[:len(row)/2]
			newRow := row[len(row)/2:]

			if ok, err := oldRow.Equals(newRow, s.rowOperationSchema.Schema); err == nil {
				if ok {
					s.stats.SameVal++
				} else {
					s.stats.Modifications++
				}
			}
		} else {
			s.stats.Additions++
		}
	}

	insertOrUpdateOperation, err := s.getInsertNode(inputChannel)
	if err != nil {
		return err
	}

	iter, err := insertOrUpdateOperation.RowIter(s.sqlCtx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			iter.Close(s.sqlCtx) // save the error that should be propagated.
		} else {
			err = iter.Close(s.sqlCtx)
		}
	}()

	for {
		if s.statsCB != nil && atomic.LoadInt32(&s.statOps) >= tableWriterStatUpdateRate {
			atomic.StoreInt32(&s.statOps, 0)
			s.statsCB(s.stats)
		}

		row, err := iter.Next(s.sqlCtx)

		// All other errors are handled by the errorHandler
		if err == nil {
			_ = atomic.AddInt32(&s.statOps, 1)
			updateStats(row)
		} else if err == io.EOF {
			atomic.LoadInt32(&s.statOps)
			atomic.StoreInt32(&s.statOps, 0)
			if s.statsCB != nil {
				s.statsCB(s.stats)
			}

			return err
		} else {
			var offendingRow sql.Row
			switch n := err.(type) {
			case sql.WrappedInsertError:
				offendingRow = n.OffendingRow
			case sql.ErrInsertIgnore:
				offendingRow = n.OffendingRow
			}

			trf := &pipeline.TransformRowFailure{Row: nil, SqlRow: offendingRow, TransformName: "write", Details: err.Error()}
			quit := badRowCb(trf)
			if quit {
				return trf
			}
		}
	}
}

func (s *SqlEngineTableWriter) Commit(ctx context.Context) error {
	_, _, err := s.se.Query(s.sqlCtx, "COMMIT")
	return err
}

func (s *SqlEngineTableWriter) RowOperationSchema() sql.PrimaryKeySchema {
	return s.rowOperationSchema
}

func (s *SqlEngineTableWriter) TableSchema() sql.PrimaryKeySchema {
	return s.tableSchema
}

// forceDropTableIfNeeded drop the given table in case the -f parameter is passed.
func (s *SqlEngineTableWriter) forceDropTableIfNeeded() error {
	if s.force {
		_, _, err := s.se.Query(s.sqlCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", s.tableName))
		return err
	}

	return nil
}

// createOrEmptyTableIfNeeded either creates or truncates the table given a -c or -r parameter.
func (s *SqlEngineTableWriter) createOrEmptyTableIfNeeded() error {
	switch s.importOption {
	case CreateOp:
		return s.createTable()
	case ReplaceOp:
		_, _, err := s.se.Query(s.sqlCtx, fmt.Sprintf("TRUNCATE TABLE %s", s.tableName))
		return err
	default:
		return nil
	}
}

// createTable creates a table.
func (s *SqlEngineTableWriter) createTable() error {
	cr := plan.NewCreateTable(sql.UnresolvedDatabase(s.database), s.tableName, false, false, &plan.TableSpec{Schema: s.tableSchema})
	analyzed, err := s.se.Analyze(s.sqlCtx, cr)
	if err != nil {
		return err
	}

	analyzedQueryProcess := analyzer.StripPassthroughNodes(analyzed.(*plan.QueryProcess))

	ri, err := analyzedQueryProcess.RowIter(s.sqlCtx, nil)
	if err != nil {
		return err
	}

	for {
		_, err = ri.Next(s.sqlCtx)
		if err != nil {
			return ri.Close(s.sqlCtx)
		}
	}
}

// getInsertNode returns the sql.Node to be iterated on given the import option.
func (s *SqlEngineTableWriter) getInsertNode(inputChannel chan sql.Row) (sql.Node, error) {
	switch s.importOption {
	case CreateOp, ReplaceOp:
		return s.createInsertImportNode(inputChannel, s.contOnErr, false, nil) // contonerr translates to ignore
	case UpdateOp:
		return s.createInsertImportNode(inputChannel, s.contOnErr, false, generateOnDuplicateKeyExpressions(s.rowOperationSchema.Schema)) // contonerr translates to ignore
	default:
		return nil, fmt.Errorf("unsupported import type")
	}
}

// createInsertImportNode creates the relevant/analyzed insert node given the import option. This insert node is wrapped
// with an error handler.
func (s *SqlEngineTableWriter) createInsertImportNode(source chan sql.Row, ignore bool, replace bool, onDuplicateExpression []sql.Expression) (sql.Node, error) {
	src := NewChannelRowSource(s.rowOperationSchema.Schema, source)
	dest := plan.NewUnresolvedTable(s.tableName, s.database)

	colNames := make([]string, 0)
	for _, col := range s.rowOperationSchema.Schema {
		colNames = append(colNames, col.Name)
	}

	insert := plan.NewInsertInto(sql.UnresolvedDatabase(s.database), dest, src, replace, colNames, onDuplicateExpression, ignore)
	analyzed, err := s.se.Analyze(s.sqlCtx, insert)
	if err != nil {
		return nil, err
	}

	analyzed = analyzer.StripPassthroughNodes(analyzed)

	// Get the first insert (wrapped with the error handler)
	plan.Inspect(analyzed, func(node sql.Node) bool {
		switch n := node.(type) {
		case *plan.InsertInto:
			analyzed = n
			return false
		default:
			return true
		}
	})

	return analyzed, nil
}

// generateOnDuplicateKeyExpressions generates the duplicate key expressions needed for the update import option.
func generateOnDuplicateKeyExpressions(sch sql.Schema) []sql.Expression {
	ret := make([]sql.Expression, len(sch))
	for i, col := range sch {
		columnExpression := expression.NewUnresolvedColumn(col.Name)
		functionExpression := expression.NewUnresolvedFunction("values", false, nil, expression.NewUnresolvedColumn(col.Name))
		ret[i] = expression.NewSetField(columnExpression, functionExpression)
	}

	return ret
}
