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
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"vitess.io/vitess/go/vt/proto/query"
)

var currTestFile string
var currRecord *parser.Record

var _, TruncateQueriesInLog = os.LookupEnv("SQLLOGICTEST_TRUNCATE_QUERIES")

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
		if !executeRecord(engine, record) {
			break
		}
	}
}

// Executes a single record and returns whether execution of records should continue
func executeRecord(engine *sqle.Engine, record *parser.Record) (cont bool){
	currRecord = record

	defer func() {
		if r := recover(); r != nil {
			toLog := r
			if str, ok := r.(string); ok {
				// attempt to keep entries on one line
				toLog = strings.ReplaceAll(str, "\n", " ")
			} else if err, ok := r.(error); ok {
				// attempt to keep entries on one line
				toLog = strings.ReplaceAll(err.Error(), "\n", " ")
			}
			logFailure("Caught panic: %v", toLog)
		}
	}()

	if !record.ShouldExecuteForEngine("mysql") {
		// Log a skip for queries and statements only, not other control records
		if record.Type() == parser.Query || record.Type() == parser.Statement {
			logSkip()
		}
		return true
	}

	switch record.Type() {
	case parser.Statement:
		ctx := sql.NewContext(context.Background(), sql.WithPid(rand.Uint64()))
		_, rowIter, err := engine.Query(ctx, record.Query())
		drainIterator(rowIter)

		if record.ExpectError() {
			if err != nil {
				logFailure("Expected error but didn't get one")
				return true
			}
		} else if err != nil {
			logFailure("Unexpected error %v", err)
			return true
		}

		logSuccess()
		return true
	case parser.Query:
		ctx := sql.NewContext(context.Background(), sql.WithPid(rand.Uint64()))
		sqlSch, rowIter, err := engine.Query(ctx, record.Query())
		if err != nil {
			logFailure("Unexpected error %v", err)
			return true
		}

		if verifySchema(record, sqlSch) {
			verifyResults(record, rowIter)
		} else {
			drainIterator(rowIter)
		}
		return true
	case parser.Halt:
		return false
	default:
		panic(fmt.Sprintf("Uncrecognized record type %v", record.Type()))
	}
}

func drainIterator(iter sql.RowIter) {
	if iter == nil {
		return
	}

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

func hashResults(results []string) (string, error) {
	h := md5.New()
	for _, r := range results {
		if _, err := h.Write(append([]byte(r), byte('\n'))); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// Returns whether the schema given matches the record's expected schema, and logging an error if not.
func verifySchema(record *parser.Record, sch sql.Schema) bool {
	schemaString := schemaToSchemaString(sch)
	if schemaString != record.Schema() {
		logFailure("Schemas differ. Expected %s, got %s", record.Schema(), schemaString)
		return false
	}
	return true
}

func schemaToSchemaString(sch sql.Schema) string {
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
			b.WriteString("X")
			logFailure("Unhandled type: " + col.Type.String())
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
	failureMessage := fmt.Sprintf(newMsg, args...)
	failureMessage = strings.ReplaceAll(strings.ReplaceAll(failureMessage, "\r", " "), "\n", " ")
	fmt.Println(failureMessage)
}

func logSkip() {
	fmt.Println(logMessagePrefix(), "skipped")
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
		// Stop recursing at the leading "test/" directory (root directory for the sqllogictest files)
		if file == "test" {
			break
		}
		pathElements = append([]string{file}, pathElements...)
		filename = filepath.Clean(dir)
	}

	return strings.ReplaceAll(filepath.Join(pathElements...), "\\", "/")
}

func truncateQuery(query string) string {
	if TruncateQueriesInLog && len(query) > 50 {
		return query[:47] + "..."
	}
	return query
}