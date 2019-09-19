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
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/sqlexport"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/xlsx"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

// DFFFromString returns a data object from a string.
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
func (dl FileDataLocation) NewReader(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, schPath string, opts interface{}) (rdCl table.TableReadCloser, sorted bool, fileMatchesSchema bool, err error) {
	exists, isDir := fs.Exists(dl.Path)

	if !exists {
		return nil, false, false, os.ErrNotExist
	} else if isDir {
		return nil, false, false, filesys.ErrIsDir
	}

	switch dl.Format {
	case CsvFile:
		delim := ","
		csvOpts, _ := opts.(CsvOptions)
		if len(csvOpts.Delim) != 0 {
			delim = csvOpts.Delim
		}

		var outSch schema.Schema = nil
		sch, tableExists, err := GetOutSchema(csvOpts.TableName, root)
		if tableExists {
			outSch = sch
		}
		rd, fileMatchesSchema, err := csv.OpenCSVReader(root.VRW().Format(), dl.Path, fs, csv.NewCSVInfo().SetDelim(delim), outSch)
		return rd, false, fileMatchesSchema, err

	case PsvFile:
		var outSch schema.Schema = nil
		csvOpts, _ := opts.(CsvOptions)
		sch, tableExists, err := GetOutSchema(csvOpts.TableName, root)
		if tableExists {
			outSch = sch
		}
		rd, fileMatchesSchema, err := csv.OpenCSVReader(root.VRW().Format(), dl.Path, fs, csv.NewCSVInfo().SetDelim("|"), outSch)
		return rd, false, fileMatchesSchema, err

	case XlsxFile:
		var outSch schema.Schema = nil
		xlsxOpts := opts.(XlsxOptions)
		sch, tableExists, err := GetOutSchema(xlsxOpts.SheetName, root)
		if tableExists {
			outSch = sch
		}
		rd, fileMatchesSchema, err := xlsx.OpenXLSXReader(root.VRW().Format(), dl.Path, fs, &xlsx.XLSXFileInfo{SheetName: xlsxOpts.SheetName}, outSch)
		return rd, false, fileMatchesSchema, err

	case JsonFile:
		var sch schema.Schema = nil
		if schPath == "" {
			if opts == nil {
				return nil, false, false, errors.New("Unable to determine table name on JSON import")
			}
			jsonOpts, _ := opts.(JSONOptions)
			sch, _, err = GetOutSchema(jsonOpts.TableName, root)
			if err != nil {
				return nil, false, false, err
			}
		}
		rd, fileMatchesSchema, err := json.OpenJSONReader(root.VRW().Format(), dl.Path, fs, json.NewJSONInfo(), sch, schPath)
		return rd, false, fileMatchesSchema, err
	}

	return nil, false, false, errors.New("unsupported format")
}

// NewCreatingWriter will create a TableWriteCloser for a DataLocation that will create a new table, or overwrite
// an existing table.
func (dl FileDataLocation) NewCreatingWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, sortedInput bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	switch dl.Format {
	case CsvFile:
		return csv.OpenCSVWriter(dl.Path, fs, outSch, csv.NewCSVInfo())
	case PsvFile:
		return csv.OpenCSVWriter(dl.Path, fs, outSch, csv.NewCSVInfo().SetDelim("|"))
	case XlsxFile:
		panic("writing to xlsx files is not supported yet")
	case JsonFile:
		return json.OpenJSONWriter(dl.Path, fs, outSch, json.NewJSONInfo())
	case SqlFile:
		return sqlexport.OpenSQLExportWriter(dl.Path, mvOpts.TableName, fs, outSch)
	}

	panic("Invalid Data Format." + string(dl.Format))
}

// NewUpdatingWriter will create a TableWriteCloser for a DataLocation that will update and append rows based on
// their primary key.
func (dl FileDataLocation) NewUpdatingWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, srcIsSorted bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	panic("Updating of files is not supported")
}

// NewReplacingWriter will create a TableWriteCloser for a DataLocation that will overwrite an existing table using
// the same schema
func (dl FileDataLocation) NewReplacingWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, srcIsSorted bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	panic("Replacing files is not supported")
}

func GetOutSchema(tableName string, root *doltdb.RootValue) (schema.Schema, bool, error) {
	table, exists, err := root.GetTable(context.TODO(), tableName)
	if !exists {
		return nil, exists, errors.New(fmt.Sprintf("The following table could not be found:\n%v", tableName))
	}
	if err != nil {
		return nil, exists, errors.New(fmt.Sprintf("An error occurred attempting to read the table:\n%v", err.Error()))
	}
	sch, err := table.GetSchema(context.TODO())
	if err != nil {
		return nil, exists, errors.New(fmt.Sprintf("An error occurred attempting to read the table schema:\n%v", err.Error()))
	}
	return sch, exists, nil
}
