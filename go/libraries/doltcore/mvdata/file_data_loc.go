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
	"os"
	"path/filepath"
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

type FileDataLocation struct {
	Path   string
	Format DataFormat
}

func (dl FileDataLocation) String() string {
	return dl.Format.ReadableStr() + ":" + dl.Path
}

func (dl FileDataLocation) Exists(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	exists, _ := fs.Exists(dl.Path)
	return exists, nil
}

func (dl FileDataLocation) NewReader(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, schPath string, opts interface{}) (rdCl table.TableReadCloser, sorted bool, err error) {
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
		sheetName := filepath.Base(dl.Path)
		sheetName = filepath.Ext(sheetName)
		rd, err := xlsx.OpenXLSXReader(root.VRW().Format(), dl.Path, fs, xlsx.NewXLSXInfo(sheetName))
		return rd, false, err

	case JsonFile:
		rd, err := json.OpenJSONReader(root.VRW().Format(), dl.Path, fs, json.NewJSONInfo(), schPath)
		return rd, false, err
	}

	return nil, false, errors.New("unsupported format")
}

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

func (dl FileDataLocation) NewUpdatingWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, srcIsSorted bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	panic("Updating of files is not supported")
}
