package merge

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"io"
)

const (
	mergeVersionProp  = "merge_version"
	mergeRowOperation = "row_operation"
)

type MergeVersion int

const (
	BaseVersion MergeVersion = iota
	OurVersion
	TheirVersion
)

type ConflictReader struct {
	confItr      types.MapIterator
	unionedSch   *schema.Schema
	baseConv     *table.RowConverter
	conv         *table.RowConverter
	mergeConv    *table.RowConverter
	bufferedRows [3]*table.Row
	currIdx      int
}

func NewConflictReader(tbl *doltdb.Table) (*ConflictReader, error) {
	base, sch, mergeSch, err := tbl.GetConflictSchemas()

	if err != nil {
		return nil, err
	}

	unionedSch := untyped.UntypedSchemaUnion(base, sch, mergeSch)

	var baseMapping, mapping, mergeMapping *schema.FieldMapping
	baseMapping, err = schema.NewInferredMapping(base, unionedSch)

	if err != nil {
		return nil, err
	}

	mapping, err = schema.NewInferredMapping(sch, unionedSch)

	if err != nil {
		return nil, err
	}

	mergeMapping, err = schema.NewInferredMapping(mergeSch, unionedSch)

	if err != nil {
		return nil, err
	}

	confItr := tbl.GetConflicts().Iterator()

	baseConv, err := table.NewRowConverter(baseMapping)
	conv, err := table.NewRowConverter(mapping)
	mergeConv, err := table.NewRowConverter(mergeMapping)

	return &ConflictReader{
		confItr,
		unionedSch,
		baseConv,
		conv,
		mergeConv,
		[3]*table.Row{},
		0}, nil
}

// GetSchema gets the schema of the rows that this reader will return
func (cr *ConflictReader) GetSchema() *schema.Schema {
	return cr.unionedSch
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (cr *ConflictReader) ReadRow() (*table.Row, error) {
	for {
		if cr.currIdx == 0 {
			key, value := cr.confItr.Next()

			if key == nil {
				return nil, io.EOF
			}

			conflict := doltdb.ConflictFromNomsList(value.(types.List))
			baseRow := f(key, conflict.Base, cr.baseConv)
			row := f(key, conflict.Value, cr.conv)
			mergeRow := f(key, conflict.MergeValue, cr.mergeConv)

			if baseRow != nil {
				if mergeRow != nil && row != nil {
					cr.bufferedRows[2] = table.NewRowWithProperties(baseRow, map[string]interface{}{mergeVersionProp: BaseVersion})
					cr.bufferedRows[1] = table.NewRowWithProperties(mergeRow, map[string]interface{}{mergeVersionProp: TheirVersion, mergeRowOperation: types.DiffChangeModified})
					cr.bufferedRows[0] = table.NewRowWithProperties(row, map[string]interface{}{mergeVersionProp: OurVersion, mergeRowOperation: types.DiffChangeModified})
					cr.currIdx = 3
				} else if row != nil {
					cr.bufferedRows[2] = table.NewRowWithProperties(baseRow, map[string]interface{}{mergeVersionProp: BaseVersion})
					cr.bufferedRows[1] = table.NewRowWithProperties(baseRow, map[string]interface{}{mergeVersionProp: TheirVersion, mergeRowOperation: types.DiffChangeRemoved})
					cr.bufferedRows[0] = table.NewRowWithProperties(row, map[string]interface{}{mergeVersionProp: OurVersion, mergeRowOperation: types.DiffChangeModified})
					cr.currIdx = 3
				} else {
					cr.bufferedRows[2] = table.NewRowWithProperties(baseRow, map[string]interface{}{mergeVersionProp: BaseVersion})
					cr.bufferedRows[1] = table.NewRowWithProperties(mergeRow, map[string]interface{}{mergeVersionProp: TheirVersion, mergeRowOperation: types.DiffChangeModified})
					cr.bufferedRows[0] = table.NewRowWithProperties(baseRow, map[string]interface{}{mergeVersionProp: OurVersion, mergeRowOperation: types.DiffChangeRemoved})
					cr.currIdx = 3
				}
			} else {
				if mergeRow != nil {
					cr.bufferedRows[0] = table.NewRowWithProperties(mergeRow, map[string]interface{}{mergeVersionProp: TheirVersion, mergeRowOperation: types.DiffChangeAdded})
					cr.currIdx++
				}

				if row != nil {
					cr.bufferedRows[1] = table.NewRowWithProperties(row, map[string]interface{}{mergeVersionProp: OurVersion, mergeRowOperation: types.DiffChangeAdded})
					cr.currIdx++
				}
			}
		}

		cr.currIdx--
		result := cr.bufferedRows[cr.currIdx]

		if result.CurrData() != nil {
			return result, nil
		}
	}
}

func f(key, fields types.Value, rowConv *table.RowConverter) *table.RowData {
	if types.IsNull(fields) {
		return nil
	}

	srcData := table.RowDataFromPKAndValueList(rowConv.SrcSch, key, fields.(types.List))
	row, err := rowConv.ConvertRowData(srcData)

	if err != nil {
		// bug or corrupt?
		panic("conversion error.")
	}

	return row
}

// Close should release resources being held
func (cr *ConflictReader) Close() error {
	return nil
}
