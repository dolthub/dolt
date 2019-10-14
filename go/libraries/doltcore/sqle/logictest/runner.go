package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/sirupsen/logrus"
	"github.com/src-d/go-mysql-server/sql"
	"io"
	"os"
	"path/filepath"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	sqle "github.com/src-d/go-mysql-server"
	"strconv"
	"strings"
	"vitess.io/vitess/go/vt/proto/query"
)

// Specify as many files / directories as requested as arguments. All test files specified will be run.
func main() {
	args := os.Args[1:]

	var testFiles []string
	for _, arg := range args {
		abs, err := filepath.Abs(arg)
		if err != nil {
			panic(err)
		}

		stat, err := os.Stat(abs)
		if err != nil {
			panic(err)
		}

		if stat.IsDir() {
			filepath.Walk(arg, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}

				if strings.HasSuffix(path, ".test") {
					testFiles = append(testFiles, path)
				}
				return nil
			})
		} else {
			testFiles = append(testFiles, abs)
		}
	}

	for _, file := range testFiles {
		logrus.Info("Running test file %s", file)
		runTestFile(file)
	}
}

func runTestFile(file string) {
	dEnv := env.Load(context.Background(), env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB)
	if !dEnv.HasDoltDir() {
		panic("Current directory must be a valid dolt repository")
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		panic(verr)
	}

	root = resetEnv(root)
	engine := sqlNewEngine(root)
	ctx := sql.NewEmptyContext()

	testRecords, err := ParseTestFile(file)
	if err != nil {
		panic(err)
	}

	for _, record := range testRecords {
		sqlSch, rowIter, err := engine.Query(ctx, record.query)
		if record.expectError {
			if err != nil {
				logrus.Error("Unexpected error but didn't get one for statement %v", record.query)
			}
		} else if err != nil {
			logrus.Error("Unexpected error %v", err)
		}

		// For queries, examine the results
		if !record.isStatement {
			verifySchema(record, sqlSch)
			verifyResults(record, rowIter)
		}
	}

}

func verifyResults(record *Record, iter sql.RowIter) {
	if len(record.result) == 1 && strings.Contains(record.result[0], "values hashing to") {
		verifyHash(record, iter)
	} else {
		verifyRows(record, iter)
	}
}

func verifyRows(record *Record, iter sql.RowIter) {
	results := rowsToResultStrings(iter)
	if len(results) != len(record.result) {
		logrus.Error("Incorrect number of results. Expected %v, got %v", len(record.result), len(results))
		return
	}

	for i := range record.result {
		if record.result[i] != results[i] {
			logrus.Error("Incorrect result. Expected %v, got %v", record.result[i], results[i])
		}
	}
}

func verifyHash(record *Record, iter sql.RowIter) {
	hash := record.HashResult()
	results := rowsToResultStrings(iter)
	computedHash, err := hashResults(results)
	if err != nil {
		logrus.Error("Error hashing results: %v", err)
		return
	}

	if hash != computedHash {
		logrus.Error("Hash of results differ. Expected %v, got %v", hash, computedHash)
	}
}

// Returns the rows in the iterator given as an array of their string representations, as expected by the test files
func rowsToResultStrings(iter sql.RowIter) []string {
	var results []string
	for {
		row, err := iter.Next()
		if err == io.EOF {
			return results
		} else if err != nil {
			logrus.Error("Error while iterating over results: %v", err)
		} else {
			for _, col := range row {
				results = append(results, toSqlString(col))
			}
		}
	}

	panic("iterator never returned io.EOF")
}

func toSqlString(col interface{}) string {
	switch v := col.(type) {
	case float32, float64:
		// exactly 3 decimal points for floats
		return fmt.Sprintf("%.3f", v)
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64:
		return strconv.Itoa(v.(int))
	case string:
		return v
	default:
		panic(fmt.Sprintf("No conversion for value %v of type %T", col, col))
	}
}

func hashResults(results []string) (string, error) {
	h := md5.New()
	for _, r := range results {
		if _, err := h.Write(append([]byte(r), byte('\n'))); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}


func verifySchema(record *Record, sch sql.Schema) {
	if len(record.schema) != len(sch) {
		logrus.Error("Schemas have different lengths. Expected %d, was %d", len(record.schema), len(sch))
		return
	}

	for i := range record.schema {
		typeChar := record.schema[i]
		column := sch[i]
		switch typeChar {
		case 'I':
			if column.Type.Type() != query.Type_INT32 {
				logrus.Error("Expected integer, got %s", column.Type.String())
			}
		case 'T':
			if column.Type.Type() != query.Type_TEXT {
				logrus.Error("Expected text, got %s", column.Type.String())
			}
		case 'R':
			if column.Type.Type() != query.Type_FLOAT32 {
				logrus.Error("Expected float, got %s", column.Type.String())
			}
		}
	}
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
	db := dsqle.NewDatabase("dolt", root)
	engine := sqle.NewDefault()
	engine.AddDatabase(db)
	return engine
}