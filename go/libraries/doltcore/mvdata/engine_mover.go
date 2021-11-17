package mvdata

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cliengine"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
	"strings"
	"sync/atomic"
)

type SqlEngineMover struct {
	se *cliengine.SqlEngine
	sqlCtx *sql.Context

	tableName string
	database string
	wrSch sql.Schema

	ContOnErr  bool
	statsCB noms.StatsCB
	stats   types.AppliedEditStats
	statOps int32
}

func NewSqlEngineMover(ctx context.Context, dEnv *env.DoltEnv, writeSch schema.Schema, cont bool, tableName string, statsCB noms.StatsCB) (*SqlEngineMover, error) {
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

	se, err := cliengine.NewSqlEngine(ctx, mrEnv, cliengine.FormatCsv, dbName)
	if err != nil {
		return nil, err
	}

	se.SetBatchMode()

	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: Move this to factory
	err = sqlCtx.Session.SetSessionVariable(sqlCtx, sql.AutoCommitSessionVar, false)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	doltSchema, err := sqlutil.FromDoltSchema(tableName, writeSch)
	if err != nil {
		return nil, err
	}

	return &SqlEngineMover{
		se: se,
		sqlCtx: sqlCtx,
		ContOnErr: cont,

		database: dbName,
		tableName: tableName,
		wrSch: doltSchema,

		statsCB: statsCB,

	}, nil
}

func (s *SqlEngineMover) StartWriting(ctx context.Context, inputChannel chan sql.Row, badRowChannel chan sql.Row) error {
	err := s.createTable(s.sqlCtx, s.wrSch)
	if err != nil {
		return err
	}

	specialInsert, err := sqle.CreateSpecialInsertNode(s.sqlCtx, s.se.GetAnalyzer(), s.database, s.tableName, inputChannel, s.wrSch)
	if err != nil {
		return err
	}

	iter, err := specialInsert.RowIter(s.sqlCtx, nil)
	if err != nil {
		return err
	}

	// TODO: badRow support
	for {
		if s.statsCB != nil && atomic.LoadInt32(&s.statOps) >= tableWriterStatUpdateRate {
			atomic.StoreInt32(&s.statOps, 0)
			s.statsCB(s.stats)
		}

		_, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				err = iter.Close(s.sqlCtx)
				break
			}

			iter.Close(s.sqlCtx)
			return err
		}

		_ = atomic.AddInt32(&s.statOps, 1)
		s.stats.Additions++
	}

	if err != nil {
		return err
	}

	atomic.LoadInt32(&s.statOps)
	atomic.StoreInt32(&s.statOps, 0)
	s.statsCB(s.stats)

	// TODO: Move this to the actual import code
	_, _, err = s.se.Query(s.sqlCtx, "COMMIT")
	return err
}

func (s *SqlEngineMover) createTable(sqlCtx *sql.Context, sch sql.Schema) error {
	colStmts := make([]string, len(sch))
	var primaryKeyCols []string

	for i, col := range sch {
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

	_, _, err := s.se.Query(sqlCtx, query)
	return err
}

func quoteIdentifiers(ids []string) []string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = fmt.Sprintf("`%s`", id)
	}
	return quoted
}
