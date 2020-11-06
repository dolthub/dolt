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
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

type result struct {
	name    string
	format  string
	rows    int
	columns int
	br      testing.BenchmarkResult
}

// RDSImpl is a Dataset containing results of benchmarking
type RSImpl struct {
	// Schema defines the structure of the Dataset
	Schema *SeedSchema

	// Results are results of benchmarking
	Results []result

	// TableName is the name of the results table
	TableName string

	// w is the writer where the results will be written
	w io.Writer
}

// NewRSImpl creates a new RSImpl
func NewRSImpl(w io.Writer, sch *SeedSchema, results []result, tableName string) *RSImpl {
	return &RSImpl{
		Schema:    sch,
		Results:   results,
		TableName: tableName,
		w:         w,
	}
}

// GenerateData writes the results to a io.Writer
func (rds *RSImpl) GenerateData() {
	writeResultsToWriter(rds.w, rds.Results, rds.Schema.Columns, rds.TableName, rds.Schema.FileFormatExt)
}

// Change returns a DataSet that is a mutation of this Dataset by the given percentage
func (rds *RSImpl) Change(pct float32) Dataset {
	// TODO
	return &RSImpl{}
}

func writeResultsToWriter(wc io.Writer, results []result, cols []*SeedColumn, tableName, format string) {
	switch format {
	case csvExt:
		generateCSVResults(wc, results, cols, tableName, format)
	default:
		log.Fatalf("cannot generate results data, file format %s unsupported \n", format)
	}
}

func generateCSVResults(wc io.Writer, results []result, cols []*SeedColumn, tableName, format string) {
	header := makeCSVHeaderStr(cols, tableName, format)

	_, err := wc.Write([]byte(header))
	if err != nil {
		log.Fatal(err)
	}

	for i, result := range results {
		row := getResultsRow(result, cols)

		_, err := wc.Write([]byte(formatRow(row, cols, i, len(results)-1, tableName, format)))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func getResultsRow(res result, cols []*SeedColumn) []string {
	row := make([]string, len(cols))

	// set name
	row[0] = res.name
	// set format
	row[1] = res.format
	// set rows
	row[2] = fmt.Sprintf("%d", res.rows)
	// set cols
	row[3] = fmt.Sprintf("%d", res.columns)
	// set iterations
	row[4] = fmt.Sprintf("%d", res.br.N)
	// set time
	row[5] = res.br.T.String()
	// set bytes
	row[6] = fmt.Sprintf("%v", res.br.Bytes)
	// set mem_allocs
	row[7] = fmt.Sprintf("%v", res.br.MemAllocs)
	// set mem_bytes
	row[8] = fmt.Sprintf("%v", res.br.MemBytes)
	// set alloced_bytes_per_op
	row[9] = fmt.Sprintf("%v", res.br.AllocedBytesPerOp())
	//set allocs_per_op
	row[10] = fmt.Sprintf("%v", res.br.AllocsPerOp())
	// set datetime
	t := time.Now()
	row[11] = fmt.Sprintf("%04d-%02d-%02d %02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute())
	return row
}

func genResultsCols() []*SeedColumn {
	return []*SeedColumn{
		NewSeedColumn("name", false, types.StringKind, supplied),
		NewSeedColumn("format", false, types.StringKind, supplied),
		NewSeedColumn("rows", false, types.StringKind, supplied),
		NewSeedColumn("columns", false, types.StringKind, supplied),
		NewSeedColumn("iterations", false, types.StringKind, supplied),
		NewSeedColumn("time", false, types.TimestampKind, supplied),
		NewSeedColumn("bytes", false, types.IntKind, supplied),
		NewSeedColumn("mem_allocs", false, types.IntKind, supplied),
		NewSeedColumn("mem_bytes", false, types.IntKind, supplied),
		NewSeedColumn("alloced_bytes_per_op", false, types.StringKind, supplied),
		NewSeedColumn("allocs_per_op", false, types.StringKind, supplied),
		NewSeedColumn("date_time", false, types.StringKind, supplied),
	}
}

func serializeResults(results []result, path, tableName, format string) {
	var sch *SeedSchema
	switch format {
	case csvExt:
		sch = NewSeedSchema(len(results), genResultsCols(), csvExt)
	default:
		log.Fatalf("cannot serialize results, unsupported file format %s \n", format)
	}
	now := time.Now()
	fs := filesys.LocalFS
	resultsFile := filepath.Join(path, fmt.Sprintf("benchmark_results-%04d-%02d-%02d%s", now.Year(), now.Month(), now.Day(), format))
	wc, err := fs.OpenForWrite(resultsFile, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer wc.Close()

	ds := NewRSImpl(wc, sch, results, tableName)
	ds.GenerateData()
}
