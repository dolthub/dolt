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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// tableWriterStatUpdateRate is the number of writes that will process before the updated stats are displayed.
	tableWriterStatUpdateRate = 64 * 1024
)

// SqlEngineTableWriter is a utility for importing a set of rows through the sql engine.
type SqlEngineTableWriter struct {
	se     *engine.SqlEngine
	sqlCtx *sql.Context

	tableName  string
	database   string
	contOnErr  bool
	force      bool
	disableFks bool

	statsCB noms.StatsCB
	stats   types.AppliedEditStats
	statOps int32

	importOption       TableImportOp
	tableSchema        sql.PrimaryKeySchema
	rowOperationSchema sql.PrimaryKeySchema
}

func NewSqlEngineTableWriter(ctx context.Context, dEnv *env.DoltEnv, createTableSchema, rowOperationSchema schema.Schema, options *MoverOptions, statsCB noms.StatsCB) (*SqlEngineTableWriter, error) {
	// TODO: Assert that dEnv.DoltDB.AccessMode() != ReadOnly?

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	if err != nil {
		return nil, err
	}

	// Simplest path would have our import path be a layer over load data
	config := &engine.SqlEngineConfig{
		ServerUser: "root",
		Autocommit: false, // We set autocommit == false to ensure to improve performance. Bulk import should not commit on each row.
		Bulk:       true,
	}
	se, err := engine.NewSqlEngine(
		ctx,
		mrEnv,
		config,
	)
	if err != nil {
		return nil, err
	}
	defer se.Close()

	dbName := mrEnv.GetFirstDatabase()

	if se.GetUnderlyingEngine().IsReadOnly() {
		// SqlEngineTableWriter does not respect read only mode
		return nil, analyzererrors.ErrReadOnlyDatabase.New(dbName)
	}

	sqlCtx, err := se.NewLocalContext(ctx)
	if err != nil {
		return nil, err
	}
	sqlCtx.SetCurrentDatabase(dbName)

	doltCreateTableSchema, err := sqlutil.FromDoltSchema("", options.TableToWriteTo, createTableSchema)
	if err != nil {
		return nil, err
	}

	doltRowOperationSchema, err := sqlutil.FromDoltSchema("", options.TableToWriteTo, rowOperationSchema)
	if err != nil {
		return nil, err
	}

	return &SqlEngineTableWriter{
		se:         se,
		sqlCtx:     sqlCtx,
		contOnErr:  options.ContinueOnErr,
		force:      options.Force,
		disableFks: options.DisableFks,

		database:  dbName,
		tableName: options.TableToWriteTo,

		statsCB: statsCB,

		importOption:       options.Operation,
		tableSchema:        doltCreateTableSchema,
		rowOperationSchema: doltRowOperationSchema,
	}, nil
}

func (s *SqlEngineTableWriter) WriteRows(ctx context.Context, inputChannel chan sql.Row, badRowCb func(row sql.Row, rowSchema sql.PrimaryKeySchema, tableName string, lineNumber int, err error) bool) (err error) {
	err = s.forceDropTableIfNeeded()
	if err != nil {
		return err
	}

	_, _, err = s.se.Query(s.sqlCtx, "START TRANSACTION")
	if err != nil {
		return err
	}

	if s.disableFks {
		_, _, err = s.se.Query(s.sqlCtx, "SET FOREIGN_KEY_CHECKS = 0")
		if err != nil {
			return err
		}
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
		if len(row) != len(s.tableSchema.Schema) {
			oldRow := row[:len(row)/2]
			newRow := row[len(row)/2:]

			if ok, err := oldRow.Equals(newRow, s.tableSchema.Schema); err == nil {
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

	insertOrUpdateOperation, err := s.getInsertNode(inputChannel, false)
	if err != nil {
		return err
	}

	iter, err := rowexec.DefaultBuilder.Build(s.sqlCtx, insertOrUpdateOperation, nil)
	if err != nil {
		return err
	}

	defer func() {
		rerr := iter.Close(s.sqlCtx)
		if err == nil {
			err = rerr
		}
	}()

	line := 1

	for {
		if s.statsCB != nil && atomic.LoadInt32(&s.statOps) >= tableWriterStatUpdateRate {
			atomic.StoreInt32(&s.statOps, 0)
			s.statsCB(s.stats)
		}

		row, err := iter.Next(s.sqlCtx)
		line += 1

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
			case sql.IgnorableError:
				offendingRow = n.OffendingRow
			}

			quit := badRowCb(offendingRow, s.tableSchema, s.tableName, line, err)
			if quit {
				return err
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
		_, _, err := s.se.Query(s.sqlCtx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", s.tableName))
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
		_, _, err := s.se.Query(s.sqlCtx, fmt.Sprintf("TRUNCATE TABLE `%s`", s.tableName))
		return err
	default:
		return nil
	}
}

// createTable creates a table.
func (s *SqlEngineTableWriter) createTable() error {
	// TODO don't use internal interfaces to do this, we had to have a sql.Schema somewhere
	// upstream to make the dolt schema
	sqlCols := make([]string, len(s.tableSchema.Schema))
	for i, c := range s.tableSchema.Schema {
		sqlCols[i] = sql.GenerateCreateTableColumnDefinition(c, c.Default.String(), c.OnUpdate.String(), sql.Collation_Default)
	}
	var pks string
	var sep string
	for _, i := range s.tableSchema.PkOrdinals {
		pks += sep + sql.QuoteIdentifier(s.tableSchema.Schema[i].Name)
		sep = ", "
	}
	if len(sep) > 0 {
		sqlCols = append(sqlCols, fmt.Sprintf("PRIMARY KEY (%s)", pks))
	}

	createTable := sql.GenerateCreateTableStatement(s.tableName, sqlCols, "", sql.CharacterSet_utf8mb4.String(), sql.Collation_Default.String(), "")
	_, iter, err := s.se.Query(s.sqlCtx, createTable)
	if err != nil {
		return err
	}
	_, err = sql.RowIterToRows(s.sqlCtx, iter)
	return err
}

// createInsertImportNode creates the relevant/analyzed insert node given the import option. This insert node is wrapped
// with an error handler.
func (s *SqlEngineTableWriter) getInsertNode(inputChannel chan sql.Row, replace bool) (sql.Node, error) {
	update := s.importOption == UpdateOp
	colNames := ""
	values := ""
	duplicate := ""
	if update {
		duplicate += " ON DUPLICATE KEY UPDATE "
	}
	sep := ""
	for _, col := range s.rowOperationSchema.Schema {
		colNames += fmt.Sprintf("%s%s", sep, sql.QuoteIdentifier(col.Name))
		values += fmt.Sprintf("%s1", sep)
		if update {
			duplicate += fmt.Sprintf("%s`%s` = VALUES(`%s`)", sep, col.Name, col.Name)
		}
		sep = ", "
	}

	sqlEngine := s.se.GetUnderlyingEngine()
	binder := planbuilder.New(s.sqlCtx, sqlEngine.Analyzer.Catalog, sqlEngine.Parser)
	insert := fmt.Sprintf("insert into `%s` (%s) VALUES (%s)%s", s.tableName, colNames, values, duplicate)
	parsed, _, _, qFlags, err := binder.Parse(insert, false)
	if err != nil {
		return nil, fmt.Errorf("error constructing import query '%s': %w", insert, err)
	}
	parsedIns, ok := parsed.(*plan.InsertInto)
	if !ok {
		return nil, fmt.Errorf("import setup expected *plan.InsertInto root, found %T", parsed)
	}
	schema := make(sql.Schema, len(s.rowOperationSchema.Schema))
	for i, c := range s.rowOperationSchema.Schema {
		newC := c.Copy()
		newC.Source = planbuilder.OnDupValuesPrefix
		schema[i] = newC
	}

	switch n := parsedIns.Source.(type) {
	case *plan.Values:
		parsedIns.Source = NewChannelRowSource(schema, inputChannel)
	case *plan.Project:
		n.Child = NewChannelRowSource(schema, inputChannel)
	}

	parsedIns.Ignore = s.contOnErr
	parsedIns.IsReplace = replace
	analyzed, err := s.se.Analyze(s.sqlCtx, parsedIns, qFlags)
	if err != nil {
		return nil, err
	}

	analyzed = analyzer.StripPassthroughNodes(analyzed)

	// Get the first insert (wrapped with the error handler)
	transform.Inspect(analyzed, func(node sql.Node) bool {
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
