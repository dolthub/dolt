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
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/parquet"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/sqlexport"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/xlsx"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// DFFromString returns a data object from a string.
func DFFromString(dfStr string) DataFormat {
	switch strings.ToLower(dfStr) {
	case "csv", ".csv":
		return CsvFile
	case "psv", ".psv":
		return PsvFile
	case "xlsx", ".xlsx":
		return XlsxFile
	case "json", ".json":
		return JsonFile
	case "sql", ".sql":
		return SqlFile
	case "parquet", ".parquet":
		return ParquetFile
	default:
		return InvalidDataFormat
	}
}

// FileDataLocation is a file that that can be imported from or exported to.
type FileDataLocation struct {
	// Path is the path of the file on the filesystem
	Path string

	// Format is the DataFormat of the file
	Format DataFormat
}

// String returns a string representation of the data location.
func (dl FileDataLocation) String() string {
	return dl.Format.ReadableStr() + ":" + dl.Path
}

// Exists returns true if the DataLocation already exists
func (dl FileDataLocation) Exists(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	exists, _ := fs.Exists(dl.Path)
	return exists, nil
}

// NewReader creates a TableReadCloser for the DataLocation
func (dl FileDataLocation) NewReader(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, opts interface{}) (rdCl table.SqlRowReader, sorted bool, err error) {
	exists, isDir := fs.Exists(dl.Path)

	if !exists {
		return nil, false, os.ErrNotExist
	} else if isDir {
		return nil, false, filesys.ErrIsDir
	}

	switch dl.Format {
	case CsvFile:
		delim := ","

		if opts != nil {
			csvOpts, _ := opts.(CsvOptions)

			if len(csvOpts.Delim) != 0 {
				delim = csvOpts.Delim
			}
		}

		rd, err := csv.OpenCSVReader(root.VRW().Format(), dl.Path, fs, csv.NewCSVInfo().SetDelim(delim))

		return rd, false, err

	case PsvFile:
		rd, err := csv.OpenCSVReader(root.VRW().Format(), dl.Path, fs, csv.NewCSVInfo().SetDelim("|"))
		return rd, false, err

	case XlsxFile:
		xlsxOpts := opts.(XlsxOptions)
		rd, err := xlsx.OpenXLSXReader(ctx, root.VRW(), dl.Path, fs, &xlsx.XLSXFileInfo{SheetName: xlsxOpts.SheetName})
		return rd, false, err

	case JsonFile:
		var sch schema.Schema
		jsonOpts, _ := opts.(JSONOptions)
		if jsonOpts.SchFile != "" {
			tn, s, err := SchAndTableNameFromFile(ctx, jsonOpts.SchFile, fs, root)
			if err != nil {
				return nil, false, err
			}
			if tn != jsonOpts.TableName {
				return nil, false, fmt.Errorf("table name '%s' from schema file %s does not match table arg '%s'", tn, jsonOpts.SchFile, jsonOpts.TableName)
			}
			sch = s
		} else {
			if opts == nil {
				return nil, false, errors.New("Unable to determine table name on JSON import")
			}
			tbl, exists, err := root.GetTable(context.TODO(), jsonOpts.TableName)
			if !exists {
				return nil, false, errors.New(fmt.Sprintf("The following table could not be found:\n%v", jsonOpts.TableName))
			}
			if err != nil {
				return nil, false, errors.New(fmt.Sprintf("An error occurred attempting to read the table:\n%v", err.Error()))
			}
			sch, err = tbl.GetSchema(context.TODO())
			if err != nil {
				return nil, false, errors.New(fmt.Sprintf("An error occurred attempting to read the table schema:\n%v", err.Error()))
			}
		}

		rd, err := json.OpenJSONReader(root.VRW(), dl.Path, fs, sch)
		return rd, false, err

	case ParquetFile:
		var tableSch schema.Schema
		parquetOpts, _ := opts.(ParquetOptions)
		if parquetOpts.SchFile != "" {
			tn, s, tnErr := SchAndTableNameFromFile(ctx, parquetOpts.SchFile, fs, root)
			if tnErr != nil {
				return nil, false, tnErr
			}
			if tn != parquetOpts.TableName {
				return nil, false, fmt.Errorf("table name '%s' from schema file %s does not match table arg '%s'", tn, parquetOpts.SchFile, parquetOpts.TableName)
			}
			tableSch = s
		} else {
			if opts == nil {
				return nil, false, errors.New("Unable to determine table name on JSON import")
			}
			tbl, tableExists, tErr := root.GetTable(context.TODO(), parquetOpts.TableName)
			if !tableExists {
				return nil, false, errors.New(fmt.Sprintf("The following table could not be found:\n%v", parquetOpts.TableName))
			}
			if tErr != nil {
				return nil, false, errors.New(fmt.Sprintf("An error occurred attempting to read the table:\n%v", err.Error()))
			}
			tableSch, err = tbl.GetSchema(context.TODO())
			if err != nil {
				return nil, false, errors.New(fmt.Sprintf("An error occurred attempting to read the table schema:\n%v", err.Error()))
			}
		}
		rd, rErr := parquet.OpenParquetReader(root.VRW(), dl.Path, tableSch)
		return rd, false, rErr
	}

	return nil, false, errors.New("unsupported format")
}

// NewCreatingWriter will create a TableWriteCloser for a DataLocation that will create a new table, or overwrite
// an existing table.
func (dl FileDataLocation) NewCreatingWriter(ctx context.Context, mvOpts DataMoverOptions, root *doltdb.RootValue, outSch schema.Schema, opts editor.Options, wr io.WriteCloser) (table.SqlTableWriter, error) {
	switch dl.Format {
	case CsvFile:
		return csv.NewCSVWriter(wr, outSch, csv.NewCSVInfo())
	case PsvFile:
		return csv.NewCSVWriter(wr, outSch, csv.NewCSVInfo().SetDelim("|"))
	case XlsxFile:
		panic("writing to xlsx files is not supported yet")
	case JsonFile:
		return json.NewJSONWriter(wr, outSch)
	case SqlFile:
		if mvOpts.IsBatched() {
			return sqlexport.OpenBatchedSQLExportWriter(ctx, wr, root, mvOpts.SrcName(), outSch, opts)
		} else {
			return sqlexport.OpenSQLExportWriter(ctx, wr, root, mvOpts.SrcName(), outSch, opts)
		}
	case ParquetFile:
		return parquet.NewParquetWriter(outSch, mvOpts.DestName())
	}

	panic("Invalid Data Format." + string(dl.Format))
}
