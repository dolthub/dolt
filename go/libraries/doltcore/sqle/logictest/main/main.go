// Copyright 2019 Dolthub, Inc.
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

package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/dolthub/sqllogictest/go/logictest"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/logictest/dolt"
)

var resultFormat = flag.String("r", "json", "format of parsed results")

// Runs all sqllogictest test files (or directories containing them) given as arguments.
// Usage: $command (run|parse) [version] [file1.test dir1/ dir2/]
// In run mode, runs the tests and prints results to stdout.
// In parse mode, parses test results from the file given and prints them to STDOUT in a format to be imported by dolt.
func main() {
	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		panic("Usage: logictest (run|parse) [version] file1 file2 ...")
	}

	if args[0] == "run" {
		h := &dolt.DoltHarness{}
		logictest.RunTestFiles(h, args[1:]...)
	} else if args[0] == "parse" {
		if len(args) < 3 {
			panic("Usage: logictest [-r(csv|json)] parse <version> (file | dir/)")
		}
		parseTestResults(args[1], args[2])
	} else {
		panic("Unrecognized command " + args[0])
	}
}

func parseTestResults(version, f string) {
	entries, err := logictest.ParseResultFile(f)
	if err != nil {
		panic(err)
	}

	records := make([]*DoltResultRecord, len(entries))
	for i, e := range entries {
		records[i] = NewDoltRecordResult(e, version)
	}

	if *resultFormat == "csv" {
		err := writeResultsCsv(records)
		if err != nil {
			panic(err)
		}
	} else {
		b, err := JSONMarshal(records)
		if err != nil {
			panic(err)
		}

		_, err = os.Stdout.Write(b)
		if err != nil {
			panic(err)
		}
	}
}

// Custom json marshalling function is necessary to avoid escaping <, > and & to html unicode escapes
func JSONMarshal(records []*DoltResultRecord) ([]byte, error) {
	rows := &TestResultArray{Rows: records}
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(rows)
	return buffer.Bytes(), err
}

func NewDoltRecordResult(e *logictest.ResultLogEntry, version string) *DoltResultRecord {
	var result string
	switch e.Result {
	case logictest.Ok:
		result = "ok"
	case logictest.NotOk:
		result = "not ok"
	case logictest.Skipped:
		result = "skipped"
	case logictest.Timeout:
		result = "timeout"
	case logictest.DidNotRun:
		result = "did not run"
	}
	return &DoltResultRecord{
		Version:      version,
		TestFile:     e.TestFile,
		LineNum:      e.LineNum,
		Query:        e.Query,
		Duration:     e.Duration.Milliseconds(),
		Result:       result,
		ErrorMessage: e.ErrorMessage,
	}
}

type TestResultArray struct {
	Rows []*DoltResultRecord `json:"rows"`
}

type DoltResultRecord struct {
	Version      string `json:"version"`
	TestFile     string `json:"test_file"`
	Query        string `json:"query_string"`
	Result       string `json:"result"`
	ErrorMessage string `json:"error_message,omitempty"`
	LineNum      int    `json:"line_num"`
	Duration     int64  `json:"duration"`
}

// fromResultCsvHeaders returns supported csv headers for a Result
func fromResultCsvHeaders() []string {
	return []string{
		"version",
		"test_file",
		"line_num",
		"query_string",
		"duration",
		"result",
		"error_message",
	}
}

// fromHeaderColumnValue returns the value from the DoltResultRecord for the given
// header field
func fromHeaderColumnValue(h string, r *DoltResultRecord) (string, error) {
	var val string
	switch h {
	case "version":
		val = r.Version
	case "test_file":
		val = r.TestFile
	case "line_num":
		val = fmt.Sprintf("%d", r.LineNum)
	case "query_string":
		val = r.Query
	case "duration":
		val = fmt.Sprintf("%d", r.Duration)
	case "result":
		val = r.Result
	case "error_message":
		val = r.ErrorMessage
	default:
		return "", fmt.Errorf("unsupported header field")
	}
	return val, nil
}

// writeResultsCsv writes []*DoltResultRecord to stdout in csv format
func writeResultsCsv(results []*DoltResultRecord) (err error) {
	csvWriter := csv.NewWriter(os.Stdout)

	// write header
	headers := fromResultCsvHeaders()
	if err := csvWriter.Write(headers); err != nil {
		return err
	}

	// write rows
	for _, r := range results {
		row := make([]string, 0)
		for _, field := range headers {
			val, err := fromHeaderColumnValue(field, r)
			if err != nil {
				return err
			}
			row = append(row, val)
		}
		err = csvWriter.Write(row)
		if err != nil {
			return err
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return err
	}
	return
}
