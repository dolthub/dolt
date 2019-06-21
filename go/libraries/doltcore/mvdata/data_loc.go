package mvdata

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/sqlexport"
	"os"
	"path/filepath"
	"strings"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/xlsx"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
	"github.com/pkg/errors"
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
)

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
	default:
		return "invalid"
	}
}

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

type DataLocation struct {
	Path   string
	Format DataFormat
}

func (dl *DataLocation) String() string {
	return dl.Format.ReadableStr() + ":" + dl.Path
}

func NewDataLocation(path, fileFmtStr string) *DataLocation {
	var dataFmt DataFormat

	if fileFmtStr == "" {
		if doltdb.IsValidTableName(path) {
			dataFmt = DoltDB
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
	} else {
		dataFmt = DFFromString(fileFmtStr)
	}

	return &DataLocation{path, dataFmt}
}

func (dl *DataLocation) IsFileType() bool {
	switch dl.Format {
	case DoltDB:
		return false
	case InvalidDataFormat:
		panic("Invalid format")
	}

	return true
}

func (dl *DataLocation) CreateReader(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, schPath string, tblName string) (rdCl table.TableReadCloser, sorted bool, err error) {
	if dl.Format == DoltDB {
		tbl, ok := root.GetTable(ctx, dl.Path)

		if !ok {
			return nil, false, doltdb.ErrTableNotFound
		}

		sch := tbl.GetSchema(ctx)
		rd := noms.NewNomsMapReader(ctx, tbl.GetRowData(ctx), sch)
		return rd, true, nil
	} else {
		exists, isDir := fs.Exists(dl.Path)

		if !exists {
			return nil, false, os.ErrNotExist
		} else if isDir {
			return nil, false, filesys.ErrIsDir
		}

		switch dl.Format {
		case CsvFile:
			rd, err := csv.OpenCSVReader(dl.Path, fs, csv.NewCSVInfo())
			return rd, false, err

		case PsvFile:
			rd, err := csv.OpenCSVReader(dl.Path, fs, csv.NewCSVInfo().SetDelim('|'))
			return rd, false, err

		case XlsxFile:
			rd, err := xlsx.OpenXLSXReader(dl.Path, fs, xlsx.NewXLSXInfo(), tblName)
			return rd, false, err

		case JsonFile:
			rd, err := json.OpenJSONReader(dl.Path, fs, json.NewJSONInfo(), schPath)
			return rd, false, err
		}
	}

	panic("Unsupported table format should have failed before reaching here. ")
}

func (dl *DataLocation) Exists(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) bool {
	if dl.IsFileType() {
		exists, _ := fs.Exists(dl.Path)
		return exists
	}

	if dl.Format == DoltDB {
		return root.HasTable(ctx, dl.Path)
	}

	panic("Invalid Data Format.")
}

var ErrNoPK = errors.New("schema does not contain a primary key")

func (dl *DataLocation) CreateOverwritingDataWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, sortedInput bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	if dl.RequiresPK() && outSch.GetPKCols().Size() == 0 {
		return nil, ErrNoPK
	}

	switch dl.Format {
	case DoltDB:
		if sortedInput {
			return noms.NewNomsMapCreator(ctx, root.VRW(), outSch), nil
		} else {
			m := types.NewMap(ctx, root.VRW())
			return noms.NewNomsMapUpdater(ctx, root.VRW(), m, outSch, statsCB), nil
		}

	case CsvFile:
		return csv.OpenCSVWriter(dl.Path, fs, outSch, csv.NewCSVInfo())
	case PsvFile:
		return csv.OpenCSVWriter(dl.Path, fs, outSch, csv.NewCSVInfo().SetDelim('|'))
	case XlsxFile:
		return xlsx.OpenXLSXWriter(dl.Path, fs, outSch, xlsx.NewXLSXInfo())
	case JsonFile:
		return json.OpenJSONWriter(dl.Path, fs, outSch, json.NewJSONInfo())
	case SqlFile:
		return sqlexport.OpenSQLExportWriter(dl.Path, mvOpts.TableName, fs, outSch)
	}

	panic("Invalid Data Format." + string(dl.Format))
}

// CreateUpdatingDataWriter will create a TableWriteCloser for a DataLocation that will update and append rows based
// on their primary key.
func (dl *DataLocation) CreateUpdatingDataWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, srcIsSorted bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	switch dl.Format {
	case DoltDB:
		tableName := dl.Path
		tbl, ok := root.GetTable(ctx, tableName)

		if !ok {
			return nil, errors.New("Could not find table " + tableName)
		}

		m := tbl.GetRowData(ctx)
		return noms.NewNomsMapUpdater(ctx, root.VRW(), m, outSch, statsCB), nil

	case CsvFile, PsvFile, JsonFile, XlsxFile, SqlFile:
		panic("Update not supported for this file type.")
	}

	panic("Invalid Data Format.")
}

// MustWriteSorted returns whether this DataLocation must be written to in primary key order
func (dl *DataLocation) MustWriteSorted() bool {
	return false
}

// RequiresPK returns whether this DataLocation requires a primary key
func (dl *DataLocation) RequiresPK() bool {
	return dl.Format == DoltDB
}

func mapByTag(src, dest *DataLocation) bool {
	return src.Format == DoltDB && dest.Format == DoltDB
}
