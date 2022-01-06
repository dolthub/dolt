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

package mvdata

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// DataFormat is an enumeration of the valid data formats
type DataFormat string

const (
	// InvalidDataFormat is the format of a data lotacion that isn't valid
	InvalidDataFormat DataFormat = "invalid"

	// DoltDB is the format of a data location for a dolt table
	DoltDB DataFormat = "doltdb"

	// CsvFile is the format of a data location that is a .csv file
	CsvFile DataFormat = ".csv"

	// PsvFile is the format of a data location that is a .psv file
	PsvFile DataFormat = ".psv"

	// XlsxFile is the format of a data location that is a .xlsx file
	XlsxFile DataFormat = ".xlsx"

	// JsonFile is the format of a data location that is a json file
	JsonFile DataFormat = ".json"

	// SqlFile is the format of a data location that is a .sql file
	SqlFile DataFormat = ".sql"

	// ParquetFile is the format of a data location that is a .paquet file
	ParquetFile DataFormat = ".parquet"
)

// ReadableStr returns a human readable string for a DataFormat
func (df DataFormat) ReadableStr() string {
	switch df {
	case DoltDB:
		return "dolt table"
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
	case ParquetFile:
		return "parquet file"
	default:
		return "invalid"
	}
}

// DataLocation is an interface that can be used to read or write from the source or the destination of a move operation.
type DataLocation interface {
	fmt.Stringer

	// Exists returns true if the DataLocation already exists
	Exists(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error)

	// NewReader creates a TableReadCloser for the DataLocation
	NewReader(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, opts interface{}) (rdCl table.SqlRowReader, sorted bool, err error)

	// NewCreatingWriter will create a TableWriteCloser for a DataLocation that will create a new table, or overwrite
	// an existing table.
	NewCreatingWriter(ctx context.Context, mvOpts DataMoverOptions, root *doltdb.RootValue, outSch schema.Schema, opts editor.Options, wr io.WriteCloser) (table.SqlTableWriter, error)
}

// NewDataLocation creates a DataLocation object from a path and a format string.  If the path is the name of a table
// then a TableDataLocation will be returned.  If the path is empty a StreamDataLocation is returned.  Otherwise a
// FileDataLocation is returned.  For FileDataLocations and StreamDataLocations, if a file format is provided explicitly
// then it is used as the format, otherwise, when it can be, it is inferred from the path for files.  Inference is based
// on the file's extension.
func NewDataLocation(path, fileFmtStr string) DataLocation {
	dataFmt := DFFromString(fileFmtStr)

	if len(path) == 0 {
		return StreamDataLocation{Format: dataFmt, Reader: cli.InStream, Writer: cli.OutStream}
	} else if fileFmtStr == "" {
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
		case string(ParquetFile):
			dataFmt = ParquetFile
		}
	}

	return FileDataLocation{path, dataFmt}
}
