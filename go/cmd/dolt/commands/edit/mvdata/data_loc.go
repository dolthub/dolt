package mvdata

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema/jsonenc"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/nbf"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/untyped/csv"
	"github.com/pkg/errors"
	"path/filepath"
	"strings"
)

type DataFormat string

const (
	InvalidDataFormat DataFormat = "invalid"
	DoltDB            DataFormat = "doltdb"
	CsvFile           DataFormat = ".csv"
	PsvFile           DataFormat = ".psv"
	NbfFile           DataFormat = ".nbf"
)

func (df DataFormat) ReadableStr() string {
	switch df {
	case DoltDB:
		return "dolt table"

	case CsvFile:
		return "csv file"

	case PsvFile:
		return "psv file"

	case NbfFile:
		return "nbf file"
	}

	return "invalid"
}

func DFFromString(dfStr string) DataFormat {
	switch strings.ToLower(dfStr) {
	case "csv", ".csv":
		return CsvFile
	case "psv", ".psv":
		return PsvFile
	case "nbf", ".nbf":
		return NbfFile
	}

	return InvalidDataFormat
}

type DataLocation struct {
	Path   string
	Format DataFormat
}

func (dl *DataLocation) String() string {
	return dl.Format.ReadableStr() + ":" + dl.Path
}

func NewDataLocation(path, fileFmtStr string) *DataLocation {
	dataFmt := DFFromString(fileFmtStr)

	if fileFmtStr == "" {
		if doltdb.IsValidTableName(path) {
			dataFmt = DoltDB
		}
		ext := filepath.Ext(path)

		switch strings.ToLower(ext) {
		case string(CsvFile):
			dataFmt = CsvFile
		case string(PsvFile):
			dataFmt = PsvFile
		case string(NbfFile):
			dataFmt = NbfFile
		}
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

func (dl *DataLocation) CreateReader(root *doltdb.RootValue, fs filesys.ReadableFS) (rdCl table.TableReadCloser, sorted bool, err errhand.VerboseError) {
	if dl.Format == DoltDB {
		tbl, ok := root.GetTable(dl.Path)

		if !ok {
			derr := errhand.BuildDError("Table %s does not exist.", dl.Path).Build()
			return nil, false, derr
		}

		sch := tbl.GetSchema(root.VRW())
		rd := noms.NewNomsMapReader(tbl.GetRowData(), sch)
		return rd, true, nil
	} else {
		exists, isDir := fs.Exists(dl.Path)

		if !exists {
			derr := errhand.BuildDError("Attempted to create a reader for a file that doesn't exist.").
				AddDetails(`The file "%s" does not exist.`, dl.Path).Build()

			return nil, false, derr
		} else if isDir {
			derr := errhand.BuildDError("Attempted to create a reader for a file that doesn't exist.").
				AddDetails(`"%s" is a directory and not a file.`, dl.Path).Build()
			return nil, false, derr
		}

		switch dl.Format {
		case CsvFile:
			rd, err := csv.OpenCSVReader(dl.Path, fs, csv.NewCSVInfo())
			derr := errhand.BuildIf(err, "Failed to open csv reader for %s", dl.Path).AddCause(err).Build()
			return rd, false, derr

		case PsvFile:
			rd, err := csv.OpenCSVReader(dl.Path, fs, csv.NewCSVInfo().SetDelim('|'))
			derr := errhand.BuildIf(err, "Failed to open psv reader for %s", dl.Path).AddCause(err).Build()
			return rd, false, derr

		case NbfFile:
			rd, err := nbf.OpenNBFReader(dl.Path, fs)
			derr := errhand.BuildIf(err, "Failed to open psv reader for %s", dl.Path).Build()
			return rd, true, derr
		}
	}

	panic("Unsupported table format should have failed before reaching here. ")
}

func (dl *DataLocation) Exists(root *doltdb.RootValue, fs filesys.ReadableFS) bool {
	if dl.IsFileType() {
		exists, _ := fs.Exists(dl.Path)
		return exists
	}

	if dl.Format == DoltDB {
		return root.HasTable(dl.Path)
	}

	panic("Invalid Data Format.")
}

func (dl *DataLocation) CreateOverwritingDataWriter(root *doltdb.RootValue, fs filesys.WritableFS, sortedInput bool, outSch *schema.Schema) (table.TableWriteCloser, errhand.VerboseError) {
	if dl.RequiresPK() && outSch.GetPKIndex() == -1 {

		builder := errhand.BuildDError("Attempting to write to a %s with a schema that does not contain a primary key.", dl.Format.ReadableStr())
		schemaJSon, err := jsonenc.SchemaToJSON(outSch)

		if err == nil {
			builder.AddDetails("Schema:\n%s", schemaJSon)
		} else {
			builder.AddDetails("Unable to serialize schema as json.")
		}

		return nil, builder.Build()
	}

	switch dl.Format {
	case DoltDB:
		if sortedInput {
			return noms.NewNomsMapCreator(root.VRW(), outSch), nil
		} else {
			m := types.NewMap(root.VRW())
			return noms.NewNomsMapUpdater(root.VRW(), m, outSch), nil
		}
	case CsvFile:
		tWr, err := csv.OpenCSVWriter(dl.Path, fs, outSch, csv.NewCSVInfo())
		errhand.BuildIf(err, "Failed to create a csv writer to create/overwrite %s.", dl.Path).Build()
		return tWr, nil

	case PsvFile:
		tWr, err := csv.OpenCSVWriter(dl.Path, fs, outSch, csv.NewCSVInfo().SetDelim('|'))
		derr := errhand.BuildIf(err, "Failed to create a csv writer to create/overwrite %s.", dl.Path).Build()
		return tWr, derr

	case NbfFile:
		tWr, err := nbf.OpenNBFWriter(dl.Path, fs, outSch)
		derr := errhand.BuildIf(err, "Failed to create a csv writer to create/overwrite %s.", dl.Path).Build()
		return tWr, derr
	}

	panic("Invalid Data Format.")
}

func (dl *DataLocation) CreateUpdatingDataWriter(root *doltdb.RootValue, fs filesys.WritableFS, srcIsSorted bool, outSch *schema.Schema) (table.TableWriteCloser, error) {
	switch dl.Format {
	case DoltDB:
		tableName := dl.Path
		tbl, ok := root.GetTable(tableName)

		if !ok {
			return nil, errors.New("Could not find table " + tableName)
		}

		m := tbl.GetRowData()
		return noms.NewNomsMapUpdater(root.VRW(), m, outSch), nil

	case CsvFile, PsvFile, NbfFile:
		panic("Update not supported for this file type.")
	}

	panic("Invalid Data Format.")
}

func (dl *DataLocation) MustWriteSorted() bool {
	return dl.Format == NbfFile
}

func (dl *DataLocation) RequiresPK() bool {
	return dl.Format == NbfFile || dl.Format == DoltDB
}
