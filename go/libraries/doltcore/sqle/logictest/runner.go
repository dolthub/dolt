package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/sirupsen/logrus"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"vitess.io/vitess/go/vt/proto/query"
)

var currTestFile string
var currRecord *Record

// Specify as many files / directories as requested as arguments. All test files specified will be run.
func main() {
	args := os.Args[1:]

	logrus.SetLevel(logrus.InfoLevel)

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
		logrus.Info("Running test file", file)
		runTestFile(file)
	}
}

func runTestFile(file string) {
	currTestFile = file
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

	testRecords, err := ParseTestFile(file)
	if err != nil {
		panic(err)
	}

	for _, record := range testRecords {
		currRecord = record
		ctx := sql.NewEmptyContext()
		sqlSch, rowIter, err := engine.Query(ctx, record.query)
		if record.expectError {
			if err != nil {
				logFailure("Expected error but didn't get one")
			} else {
				logSuccess()
			}
		} else if err != nil {
			logFailure("Unexpected error %v", err)
			continue
		}

		// For queries, examine the results
		if !record.isStatement {
			verifySchema(record, sqlSch)
			verifyResults(record, rowIter)
		} else {
			drainIterator(rowIter)
		}
	}
}

func drainIterator(iter sql.RowIter) {
	for {
		_, err := iter.Next()
		if err == io.EOF {
			return
		} else if err != nil {
			logFailure("Unexpected error %v", err)
		}
	}
}

func verifyResults(record *Record, iter sql.RowIter) {
	if record.IsHashResult() {
		verifyHash(record, iter)
	} else {
		verifyRows(record, iter)
	}
}

func verifyRows(record *Record, iter sql.RowIter) {
	results := rowsToResultStrings(iter)
	if len(results) != len(record.result) {
		logFailure(fmt.Sprintf("Incorrect number of results. Expected %v, got %v", len(record.result), len(results)))
		return
	}

	for i := range record.result {
		if record.result[i] != results[i] {
			logFailure("Incorrect result at position %d. Expected %v, got %v", i, record.result[i], results[i])
			return
		}
	}

	logSuccess()
}

func verifyHash(record *Record, iter sql.RowIter) {
	hash := record.HashResult()
	results := rowsToResultStrings(iter)
	computedHash, err := hashResults(results)
	if err != nil {
		logFailure("Error hashing results: %v", err)
		return
	}

	if hash != computedHash {
		logFailure("Hash of results differ. Expected %v, got %v", hash, computedHash)
	} else {
		logSuccess()
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
			logFailure("Error while iterating over results: %v", err)
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
		logFailure("Schemas have different lengths. Expected %d, was %d", len(record.schema), len(sch))
		return
	}

	for i := range record.schema {
		typeChar := record.schema[i]
		column := sch[i]
		switch typeChar {
		case 'I':
		switch column.Type.Type() {
			case query.Type_INT32, query.Type_INT64:
		default:
			logFailure("Expected integer, got %s", column.Type.String())
			return
		}
		case 'T':
			switch column.Type.Type() {
			case query.Type_TEXT, query.Type_VARCHAR:
			default:
				logFailure("Expected text, got %s", column.Type.String())
				return
			}
		case 'R':
			switch column.Type.Type() {
			case query.Type_FLOAT32, query.Type_FLOAT64:
			default:
				logFailure("Expected float, got %s", column.Type.String())
				return
			}
		}
	}

	logSuccess()
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

func logFailure(message string, args ...interface{}) {
	newMsg := logMessagePrefix() + " not ok: " + message
	logrus.Error(fmt.Sprintf(newMsg, args...))
}

func logSuccess() {
	logrus.Info(logMessagePrefix(), " ok")
}

func logMessagePrefix() string {
	return fmt.Sprintf("%s:%d: %s", testFilePath(currTestFile), currRecord.lineNum, truncateQuery(currRecord.query))
}

func testFilePath(f string) string {
	var pathElements []string
	filename := f
	for len(pathElements) < 4 && len(filename) > 0 {
		dir, file := filepath.Split(filename)
		pathElements = append([]string{file}, pathElements...)
		filename = filepath.Clean(dir)
	}
	return filepath.Join(pathElements...)
}

func truncateQuery(query string) string {
	if len(query) > 50 {
		return query[:47] + "..."
	}
	return query
}