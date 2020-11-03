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
	"flag"
	"log"
	"os"
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const (
	smallSet  = 1000
	mediumSet = 100000
	largeSet  = 10000000
)

var outputPath = flag.String("outputPath", "./", "the path where the serialized results file will be stored.")
var outputFormat = flag.String("outputFormat", ".csv", "the format used to serialize the benchmarking results.")
var resultsTableName = flag.String("resultsTableName", "results", "the name of the results table.")
var csvFlag = flag.Bool("csv", false, "test importing .csv file into dolt")
var jsonFlag = flag.Bool("json", false, "test importing .json file into dolt")
var sqlFlag = flag.Bool("sql", false, "test importing .sql file into dolt")

var flagStrs = []flagStr{
	{b: csvFlag, s: csvExt},
	{b: jsonFlag, s: jsonExt},
	{b: sqlFlag, s: sqlExt},
}

type flagStr struct {
	b *bool
	s string
}

func main() {
	flag.Parse()

	results := make([]result, 0)

	testFmts := make([]string, 0)
	for _, fs := range flagStrs {
		if *fs.b {
			if fs.s == sqlExt {
				log.Fatal("benchmarking dolt sql imports currently disabled")
			}
			testFmts = append(testFmts, fs.s)
		}
	}

	if len(testFmts) == 0 {
		log.Fatal("must provide flag(s) format for testing dolt imports, ie -csv, -json, -sql \n")
	}

	for _, frmt := range testFmts {
		benchmarks := []struct {
			Name    string
			Format  string
			Rows    int
			Columns int
			BM      func(b *testing.B)
		}{
			{
				Name:    "dolt_import_small",
				Format:  frmt,
				Rows:    smallSet,
				Columns: len(genSampleCols()),
				BM:      BenchmarkDoltImport(smallSet, genSampleCols(), frmt),
			},
			{
				Name:    "dolt_import_medium",
				Format:  frmt,
				Rows:    mediumSet,
				Columns: len(genSampleCols()),
				BM:      BenchmarkDoltImport(mediumSet, genSampleCols(), frmt),
			},
			{
				Name:    "dolt_import_large",
				Format:  frmt,
				Rows:    largeSet,
				Columns: len(genSampleCols()),
				BM:      BenchmarkDoltImport(largeSet, genSampleCols(), frmt),
			},
		}

		for _, b := range benchmarks {
			br := testing.Benchmark(b.BM)
			res := result{
				name:    b.Name,
				format:  b.Format,
				rows:    b.Rows,
				columns: b.Columns,
				br:      br,
			}
			results = append(results, res)
		}
	}

	// benchmark other dolt commands with and just use a single import format
	for _, frmt := range []string{csvExt} {
		benchmarks := []struct {
			Name    string
			Format  string
			Rows    int
			Columns int
			BM      func(b *testing.B)
		}{
			{
				Name:    "dolt_export_small",
				Format:  frmt,
				Rows:    smallSet,
				Columns: len(genSampleCols()),
				BM:      BenchmarkDoltExport(smallSet, genSampleCols(), frmt),
			},
			{
				Name:    "dolt_export_medium",
				Format:  frmt,
				Rows:    mediumSet,
				Columns: len(genSampleCols()),
				BM:      BenchmarkDoltExport(mediumSet, genSampleCols(), frmt),
			},
			{
				Name:    "dolt_export_large",
				Format:  frmt,
				Rows:    largeSet,
				Columns: len(genSampleCols()),
				BM:      BenchmarkDoltExport(largeSet, genSampleCols(), frmt),
			},
			{
				Name:    "dolt_sql_select_small",
				Format:  frmt,
				Rows:    smallSet,
				Columns: len(genSampleCols()),
				BM:      BenchmarkDoltSQLSelect(smallSet, genSampleCols(), frmt),
			},
			{
				Name:    "dolt_sql_select_medium",
				Format:  frmt,
				Rows:    mediumSet,
				Columns: len(genSampleCols()),
				BM:      BenchmarkDoltSQLSelect(mediumSet, genSampleCols(), frmt),
			},
			{
				Name:    "dolt_sql_select_large",
				Format:  frmt,
				Rows:    largeSet,
				Columns: len(genSampleCols()),
				BM:      BenchmarkDoltSQLSelect(largeSet, genSampleCols(), frmt),
			},
		}

		for _, b := range benchmarks {
			br := testing.Benchmark(b.BM)
			res := result{
				name:    b.Name,
				format:  b.Format,
				rows:    b.Rows,
				columns: b.Columns,
				br:      br,
			}
			results = append(results, res)
		}
	}

	// write results data
	serializeResults(results, *outputPath, *resultsTableName, *outputFormat)

	// cleanup temp dolt data dir
	removeTempDoltDataDir(filesys.LocalFS)

	os.Exit(0)
}
