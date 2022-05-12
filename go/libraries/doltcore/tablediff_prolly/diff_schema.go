package tablediff_prolly

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

func CalculateDiffSchema(fromSch schema.Schema, toSch schema.Schema) (schema.Schema, error) {
	colCollection := fromSch.GetAllCols()
	colCollection = colCollection.Append(
		schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
		schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
	fromSch = schema.MustSchemaFromCols(colCollection)

	colCollection = toSch.GetAllCols()
	colCollection = colCollection.Append(
		schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false),
		schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))
	toSch = schema.MustSchemaFromCols(colCollection)

	cols := make([]schema.Column, toSch.GetAllCols().Size()+fromSch.GetAllCols().Size()+1)

	i := 0
	err := toSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		toCol, err := schema.NewColumnWithTypeInfo("to_"+col.Name, uint64(i), col.TypeInfo, false, col.Default, false, col.Comment)
		if err != nil {
			return true, err
		}
		cols[i] = toCol
		i++
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	j := toSch.GetAllCols().Size()
	err = fromSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		fromCol, err := schema.NewColumnWithTypeInfo("from_"+col.Name, uint64(i), col.TypeInfo, false, col.Default, false, col.Comment)
		if err != nil {
			return true, err
		}
		cols[j] = fromCol

		j++
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	cols[len(cols)-1] = schema.NewColumn("diff_type", schema.DiffTypeTag, types.StringKind, false, schema.NotNullConstraint{})

	return schema.UnkeyedSchemaFromCols(schema.NewColCollection(cols...)), nil
}
