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

package mvdata

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

type DataFormat string

const (
	InvalidDataFormat DataFormat = "invalid"
	DoltDB            DataFormat = "doltdb"
	CsvFile           DataFormat = ".csv"
	PsvFile           DataFormat = ".psv"
	XlsxFile          DataFormat = ".xlsx"
	JsonFile          DataFormat = ".json"
	SqlFile           DataFormat = ".sql"
	StdIO			  DataFormat = "stdio"
)

func (df DataFormat) ReadableStr() string {
	switch df {
	case DoltDB:
		return "dolt table"
	case StdIO:
		return "std i/o"
	case CsvFile:
		return "csv file"
	case PsvFile:
		return "psv file"
	case XlsxFile:
		return "xlsx file"
	case JsonFile:
		return "json file"
	case SqlFile:
		return "sql file"
	default:
		return "invalid"
	}
}

type DataLocation interface {
	fmt.Stringer

	// Exists returns true if the DataLocation already exists
	Exists(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error)

	// NewReader creates a TableReadCloser for the DataLocation
	NewReader(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, schPath string, opts interface{}) (rdCl table.TableReadCloser, sorted bool, err error)

	// NewCreatingWriter will create a TableWriteCloser for a DataLocation that will create a new table, or overwrite an existing table.
	NewCreatingWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, sortedInput bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error)

	// NewUpdatingWriter will create a TableWriteCloser for a DataLocation that will update and append rows based on their primary key.
	NewUpdatingWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, srcIsSorted bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error)
}

func NewDataLocation(path, fileFmtStr string) DataLocation {
	dataFmt := DFFromString(fileFmtStr)

	if len(path) == 0 {
		return StdIODataLocation{dataFmt}
	} else if fileFmtStr == "" {
		if doltdb.IsValidTableName(path) {
			return TableDataLocation{path}
		} else {
			switch strings.ToLower(filepath.Ext(path)) {
			case string(CsvFile):
				dataFmt = CsvFile
			case string(PsvFile):
				dataFmt = PsvFile
			case string(XlsxFile):
				dataFmt = XlsxFile
			case string(JsonFile):
				dataFmt = JsonFile
			case string(SqlFile):
				dataFmt = SqlFile
			}
		}
	}

	return FileDataLocation{path, dataFmt}
}


func mapByTag(src, dest DataLocation) bool {
	_, srcIsTable := src.(TableDataLocation)
	_, destIsTable := dest.(TableDataLocation)

	return srcIsTable && destIsTable
}





