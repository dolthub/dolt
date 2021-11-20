package mvdata

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

type sqlEngineMover struct {
	se     *engine.SqlEngine
	sqlCtx *sql.Context

	tableName string
	database  string
	wrSch     sql.Schema
	contOnErr bool
	force     bool // TODO: Refactor all of these parameters

	statsCB noms.StatsCB
	stats   types.AppliedEditStats
	statOps int32

	importOption TableImportOp
}

func NewSqlEngineMover(ctx context.Context, dEnv *env.DoltEnv, writeSch schema.Schema, options *MoverOptions, statsCB noms.StatsCB) (*sqlEngineMover, error) {
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

	se, err := engine.NewSqlEngine(ctx, mrEnv, engine.FormatCsv, dbName, false)
	if err != nil {
		return nil, err
	}

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: Verify this is correct Enable batch mode
	dsess.DSessFromSess(sqlCtx.Session).EnableBatchedMode()

	err = sqlCtx.Session.SetSessionVariable(sqlCtx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	doltSchema, err := sqlutil.FromDoltSchema(options.TableToWriteTo, writeSch)
	if err != nil {
		return nil, err
	}

	return &sqlEngineMover{
		se:        se,
		contOnErr: options.ContinueOnErr,
		force:     options.Force,

		database:  dbName,
		tableName: options.TableToWriteTo,
		wrSch:     doltSchema,

		statsCB:      statsCB,
		importOption: options.Operation,
	}, nil
}

func (s *sqlEngineMover) StartWriting(ctx context.Context, inputChannel chan sql.Row, badRowChannel chan *pipeline.TransformRowFailure) error {
	var err error
	s.sqlCtx, err = s.se.NewContext(ctx)
	if err != nil {
		return err
	}

	err = s.forceDropTableIfNeeded()
	if err != nil {
		return err
	}

	err = s.createOrEmptyTableIfNeeded()
	if err != nil {
		return err
	}

	errorHandler := func(err error) {
		if err == io.EOF {
			return
		}

		var offendingRow sql.Row
		switch n := err.(type) {
		case sql.WrappedInsertError:
			offendingRow = n.OffendingRow
		case sql.ErrInsertIgnore:
			offendingRow = n.OffendingRow
		}

		select {
		case <-ctx.Done():
			return
		default:
			s.stats.Additions-- // Reduce the addition count
			badRowChannel <- &pipeline.TransformRowFailure{Row: nil, SqlRow: offendingRow, TransformName: "create", Details: err.Error()}
		}
	}

	insertOrUpdateOperation, err := s.getNodeOperation(inputChannel, errorHandler)
	if err != nil {
		return err
	}

	iter, err := insertOrUpdateOperation.RowIter(s.sqlCtx, nil)
	if err != nil {
		return err
	}
	defer iter.Close(s.sqlCtx)

	for {
		if s.statsCB != nil && atomic.LoadInt32(&s.statOps) >= tableWriterStatUpdateRate {
			atomic.StoreInt32(&s.statOps, 0)
			s.statsCB(s.stats)
		}

		_, err := iter.Next()

		// All other errors are handled by the errorHandler
		if err == nil {
			_ = atomic.AddInt32(&s.statOps, 1)
			s.stats.Additions++
		} else if err == io.EOF {
			atomic.LoadInt32(&s.statOps)
			atomic.StoreInt32(&s.statOps, 0)
			s.statsCB(s.stats)

			return err
		}
	}
}

func (s *sqlEngineMover) Commit(ctx context.Context) error {
	// TODO: Move this to the actual import code
	_, _, err := s.se.Query(s.sqlCtx, "COMMIT")
	return err
}

func (s *sqlEngineMover) forceDropTableIfNeeded() error {
	if s.force {
		_, _, err := s.se.Query(s.sqlCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", s.tableName))
		return err
	}

	return nil
}

func (s *sqlEngineMover) createOrEmptyTableIfNeeded() error {
	switch s.importOption {
	case CreateOp:
		return s.createTable()
	case ReplaceOp:
		_, _, err := s.se.Query(s.sqlCtx, fmt.Sprintf("TRUNCATE TABLE %s", s.tableName))
		return err
	default:
		_, _, err := s.se.Query(s.sqlCtx, fmt.Sprintf("START TRANSACTION"))
		return err
	}
}

func (s *sqlEngineMover) createTable() error {
	colStmts := make([]string, len(s.wrSch))
	var primaryKeyCols []string

	for i, col := range s.wrSch {
		stmt := fmt.Sprintf("  `%s` %s", col.Name, strings.ToLower(col.Type.String()))

		if !col.Nullable {
			stmt = fmt.Sprintf("%s NOT NULL", stmt)
		}

		if col.AutoIncrement {
			stmt = fmt.Sprintf("%s AUTO_INCREMENT", stmt)
		}

		// TODO: The columns that are rendered in defaults should be backticked
		if col.Default != nil {
			stmt = fmt.Sprintf("%s DEFAULT %s", stmt, col.Default.String())
		}

		if col.Comment != "" {
			stmt = fmt.Sprintf("%s COMMENT '%s'", stmt, col.Comment)
		}

		if col.PrimaryKey {
			primaryKeyCols = append(primaryKeyCols, col.Name)
		}

		colStmts[i] = stmt
	}

	if len(primaryKeyCols) > 0 {
		primaryKey := fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(quoteIdentifiers(primaryKeyCols), ","))
		colStmts = append(colStmts, primaryKey)
	}

	query := fmt.Sprintf(
		"CREATE TABLE `%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		s.tableName,
		strings.Join(colStmts, ",\n"),
	)

	_, _, err := s.se.Query(s.sqlCtx, query)
	return err
}

func (s *sqlEngineMover) getNodeOperation(inputChannel chan sql.Row, errorHandler func(err error)) (sql.Node, error) {
	switch s.importOption {
	case CreateOp, ReplaceOp:
		return createInsertImportNode(s.sqlCtx, s.se.GetAnalyzer(), s.database, s.tableName, inputChannel, s.wrSch, s.contOnErr, false, errorHandler) // contonerr translates to ignore
	case UpdateOp:
		return createInsertImportNode(s.sqlCtx, s.se.GetAnalyzer(), s.database, s.tableName, inputChannel, s.wrSch, false, true, errorHandler)
	default:
		return nil, fmt.Errorf("unsupported")
	}
}

func createInsertImportNode(ctx *sql.Context, analyzer *analyzer.Analyzer, dbname string, tableName string, source chan sql.Row, schema sql.Schema, ignore bool, replace bool, errorHandler plan.ErrorHandler) (sql.Node, error) {
	src := plan.NewRowIterSource(schema, source)
	dest := plan.NewUnresolvedTable(tableName, dbname)

	insert := plan.NewInsertInto(sql.UnresolvedDatabase(dbname), dest, src, replace, nil, nil, ignore)
	analyzed, err := analyzer.Analyze(ctx, insert, nil)
	if err != nil {
		return nil, err
	}

	analyzed, err = plan.TransformUp(analyzed, func(node sql.Node) (sql.Node, error) {
		switch n := node.(type) {
		case *plan.InsertInto:
			return plan.NewErrorHandlerNode(n, errorHandler), nil
		default:
			return n, nil
		}
	})

	if err != nil {
		return nil, err
	}

	analyzedQueryProcess, ok := analyzed.(*plan.QueryProcess)
	if !ok {
		return nil, fmt.Errorf("internal error: unknown analyzed result type `%T`", analyzed)
	}

	// We don't want the accumulator node wrapping the analyzed insert.
	accumulatorNode := analyzedQueryProcess.Child

	return accumulatorNode.(*plan.RowUpdateAccumulator).Child, nil
}

func quoteIdentifiers(ids []string) []string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = fmt.Sprintf("`%s`", id)
	}
	return quoted
}
