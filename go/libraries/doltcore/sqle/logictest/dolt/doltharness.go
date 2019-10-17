package dolt

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	dsql "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"vitess.io/vitess/go/vt/proto/query"
)

type DoltHarness struct {
	engine *sqle.Engine
}

func (h *DoltHarness) EngineStr() string {
	return "mysql"
}

func (h *DoltHarness) Init() {
	dEnv := env.Load(context.Background(), env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB)
	if !dEnv.HasDoltDir() {
		panic("Current directory must be a valid dolt repository")
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		panic(verr)
	}

	root = resetEnv(root)
	h.engine = sqlNewEngine(root)
}

func (h *DoltHarness) ExecuteStatement(statement string) error {
	ctx := sql.NewContext(context.Background(), sql.WithPid(rand.Uint64()))
	_, rowIter, err := h.engine.Query(ctx, statement)
	if err != nil {
		return err
	}

	return drainIterator(rowIter)
}

func (h *DoltHarness) ExecuteQuery(statement string) (string, []string, error) {
	ctx := sql.NewContext(context.Background(), sql.WithPid(rand.Uint64()))
	sch, rowIter, err := h.engine.Query(ctx, statement)
	if err != nil {
		return "", nil, err
	}

	schemaString, err := schemaToSchemaString(sch)
	if err != nil {
		return "", nil, err
	}

	results, err := rowsToResultStrings(rowIter)
	if err != nil {
		return "", nil, err
	}

	return schemaString, results, nil
}

func drainIterator(iter sql.RowIter) error {
	if iter == nil {
		return nil
	}

	for {
		_, err := iter.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
}

// Returns the rows in the iterator given as an array of their string representations, as expected by the test files
func rowsToResultStrings(iter sql.RowIter) ([]string, error) {
	var results []string
	if iter == nil {
		return results, nil
	}

	for {
		row, err := iter.Next()
		if err == io.EOF {
			return results, nil
		} else if err != nil {
			return nil, err
		} else {
			for _, col := range row {
				results = append(results, toSqlString(col))
			}
		}
	}

	panic("iterator never returned io.EOF") // unreachable, required for compile
}

func toSqlString(val interface{}) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case float32, float64:
		// exactly 3 decimal points for floats
		return fmt.Sprintf("%.3f", v)
	case int:
		return strconv.Itoa(v)
	case uint:
		return strconv.Itoa(int(v))
	case int8:
		return strconv.Itoa(int(v))
	case uint8:
		return strconv.Itoa(int(v))
	case int16:
		return strconv.Itoa(int(v))
	case uint16:
		return strconv.Itoa(int(v))
	case int32:
		return strconv.Itoa(int(v))
	case uint32:
		return strconv.Itoa(int(v))
	case int64:
		return strconv.Itoa(int(v))
	case uint64:
		return strconv.Itoa(int(v))
	case string:
		return v
	// Mysql returns 1 and 0 for boolean values, mimic that
	case bool:
		if v {
			return "1"
		} else {
			return "0"
		}
	default:
		panic(fmt.Sprintf("No conversion for value %v of type %T", val, val))
	}
}

func schemaToSchemaString(sch sql.Schema) (string, error) {
	b := strings.Builder{}
	for _, col := range sch {
		switch col.Type.Type() {
		case query.Type_INT32, query.Type_INT64, query.Type_BIT:
			b.WriteString("I")
		case query.Type_TEXT, query.Type_VARCHAR:
			b.WriteString("T")
		case query.Type_FLOAT32, query.Type_FLOAT64:
			b.WriteString("R")
		default:
			return "", fmt.Errorf("Unhandled type: %v", col.Type)
		}
	}
	return b.String(), nil
}

func resetEnv(root *doltdb.RootValue) *doltdb.RootValue {
	tableNames, err := root.GetTableNames(context.Background())
	if err != nil {
		panic(err)
	}
	newRoot, err := root.RemoveTables(context.Background(), tableNames...)
	if err != nil {
		panic(err)
	}
	return newRoot
}

func sqlNewEngine(root *doltdb.RootValue) *sqle.Engine {
	db := dsql.NewDatabase("dolt", root)
	engine := sqle.NewDefault()
	engine.AddDatabase(db)
	return engine
}