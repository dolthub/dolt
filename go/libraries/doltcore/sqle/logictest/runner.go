package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/logictest/parser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/sirupsen/logrus"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"vitess.io/vitess/go/vt/proto/query"
)

var currTestFile string
var currRecord *parser.Record

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

	testRecords, err := parser.ParseTestFile(file)
	if err != nil {
		panic(err)
	}

	for _, record := range testRecords {
		currRecord = record
		ctx := sql.NewEmptyContext()
		sqlSch, rowIter, err := engine.Query(ctx, record.Query())
		if record.ExpectError() {
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
		if !record.IsStatement() {
			verifySchema(record, sqlSch)
			verifyResults(record, rowIter)
		} else {
			drainIterator(rowIter)
			logSuccess()
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

func verifyResults(record *parser.Record, iter sql.RowIter) {
	results := rowsToResultStrings(iter)

	if len(results) != record.NumResults() {
		logFailure(fmt.Sprintf("Incorrect number of results. Expected %v, got %v", record.NumResults(), len(results)))
		return
	}

	if record.IsHashResult() {
		verifyHash(record, results)
	} else {
		verifyRows(record, results)
	}
}

func verifyRows(record *parser.Record, results []string) {
	results = record.SortResults(results)

	for i := range record.Result() {
		if record.Result()[i] != results[i] {
			logFailure("Incorrect result at position %d. Expected %v, got %v", i, record.Result()[i], results[i])
			return
		}
	}

	logSuccess()
}

func verifyHash(record *parser.Record, results []string) {
	results = record.SortResults(results)

	computedHash, err := hashResults(results)
	if err != nil {
		logFailure("Error hashing results: %v", err)
		return
	}

	if record.HashResult() != computedHash {
		logFailure("Hash of results differ. Expected %v, got %v", record.HashResult(), computedHash)
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

func toSqlString(val interface{}) string {
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
	default:
		panic(fmt.Sprintf("No conversion for value %v of type %T", val, val))
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

func verifySchema(record *parser.Record, sch sql.Schema) {
	schemaString := schemaToSchemaString(sch)
	if schemaString != record.Schema() {
		logFailure("Schemas differ. Expected %s, got %s", record.Schema(), schemaString)
	}
}

func schemaToSchemaString(sch sql.Schema) string {
	b := strings.Builder{}
	for _, col := range sch {
		switch col.Type.Type() {
		case query.Type_INT32, query.Type_INT64:
			b.WriteString("I")
		case query.Type_TEXT, query.Type_VARCHAR:
			b.WriteString("T")
		case query.Type_FLOAT32, query.Type_FLOAT64:
			b.WriteString("R")
		default:
			panic("Unhandled type: " + col.Type.String())
		}
	}
	return b.String()
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
	fmt.Println(fmt.Sprintf(newMsg, args...))
}

func logSuccess() {
	fmt.Println(logMessagePrefix(), "ok")
}

func logMessagePrefix() string {
	return fmt.Sprintf("%s %s:%d: %s",
		time.Now().Format(time.RFC3339Nano),
		testFilePath(currTestFile),
		currRecord.LineNum(),
		truncateQuery(currRecord.Query()))
}

func testFilePath(f string) string {
	var pathElements []string
	filename := f
	for len(pathElements) < 4 && len(filename) > 0 {
		dir, file := filepath.Split(filename)
		pathElements = append([]string{file}, pathElements...)
		filename = filepath.Clean(dir)
	}
	return strings.ReplaceAll(filepath.Join(pathElements...), "\\", "/")
}

func truncateQuery(query string) string {
	if len(query) > 50 {
		return query[:47] + "..."
	}
	return query
}