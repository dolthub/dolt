// Copyright 2019 Liquidata, Inc.
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

package logictest

import (
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/logictest/parser"
)

var currTestFile string
var currRecord *parser.Record

var _, TruncateQueriesInLog = os.LookupEnv("SQLLOGICTEST_TRUNCATE_QUERIES")

// Runs the test files found under any of the paths given. Can specify individual test files, or directories that
// contain test files somewhere underneath. All files named *.test enountered under a directory will be attempted to be
// parsed as a test file, and will panic for malformed test files or paths that don't exist.
func RunTestFiles(harness Harness, paths ...string) {
	var testFiles []string
	for _, arg := range paths {
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
		runTestFile(harness, file)
	}
}

func runTestFile(harness Harness, file string) {
	currTestFile = file

	harness.Init()

	testRecords, err := parser.ParseTestFile(file)
	if err != nil {
		panic(err)
	}

	for _, record := range testRecords {
		if !executeRecord(harness, record) {
			break
		}
	}
}

// Executes a single record and returns whether execution of records should continue
func executeRecord(harness Harness, record *parser.Record) (cont bool) {
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

	if !record.ShouldExecuteForEngine(harness.EngineStr()) {
		// Log a skip for queries and statements only, not other control records
		if record.Type() == parser.Query || record.Type() == parser.Statement {
			logSkip()
		}
		return true
	}

	switch record.Type() {
	case parser.Statement:
		err := harness.ExecuteStatement(record.Query())

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
		schemaStr, results, err := harness.ExecuteQuery(record.Query())
		if err != nil {
			logFailure("Unexpected error %v", err)
			return true
		}

		// Only log one error per record, so if schema comparison fails don't bother with result comparison
		if verifySchema(record, schemaStr) {
			verifyResults(record, results)
		}
		return true
	case parser.Halt:
		return false
	default:
		panic(fmt.Sprintf("Uncrecognized record type %v", record.Type()))
	}
}

func verifyResults(record *parser.Record, results []string) {
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
func verifySchema(record *parser.Record, schemaStr string) bool {
	if schemaStr != record.Schema() {
		logFailure("Schemas differ. Expected %s, got %s", record.Schema(), schemaStr)
		return false
	}
	return true
}

func logFailure(message string, args ...interface{}) {
	newMsg := logMessagePrefix() + " not ok: " + message
	failureMessage := fmt.Sprintf(newMsg, args...)
	failureMessage = strings.ReplaceAll(failureMessage, "\n", " ")
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
