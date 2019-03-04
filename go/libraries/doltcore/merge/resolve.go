package merge

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
)

type AutoResolver func(key types.Value, conflict doltdb.Conflict) (types.Value, error)

func Ours(key types.Value, cnf doltdb.Conflict) (types.Value, error) {
	return cnf.Value, nil
}

func Theirs(key types.Value, cnf doltdb.Conflict) (types.Value, error) {
	return cnf.MergeValue, nil
}

func ResolveTable(vrw types.ValueReadWriter, tbl *doltdb.Table, autoResFunc AutoResolver) (*doltdb.Table, error) {
	if !tbl.HasConflicts() {
		return nil, doltdb.ErrNoConflicts
	}

	tblSchRef := tbl.GetSchemaRef()
	tblSchVal := tblSchRef.TargetValue(vrw)
	tblSch, err := encoding.UnmarshalNomsValue(tblSchVal)

	if err != nil {
		return nil, err
	}

	schemas, conflicts, err := tbl.GetConflicts()

	if err != nil {
		return nil, err
	}

	rowEditor := tbl.GetRowData().Edit()

	var itrErr error
	conflicts.Iter(func(key, value types.Value) (stop bool) {
		cnf := doltdb.ConflictFromTuple(value.(types.Tuple))

		var updated types.Value
		updated, itrErr = autoResFunc(key, cnf)

		if itrErr != nil {
			return true
		}

		if types.IsNull(updated) {
			rowEditor.Remove(key)
		} else {
			r := row.FromNoms(tblSch, key.(types.Tuple), updated.(types.Tuple))

			if !row.IsValid(r, tblSch) {
				itrErr = table.ErrInvalidRow
				return true
			}

			rowEditor.Set(key, updated)
		}

		return false
	})

	if itrErr != nil {
		return nil, itrErr
	}

	newTbl := doltdb.NewTable(vrw, tblSchVal, rowEditor.Map())
	newTbl = newTbl.SetConflicts(schemas, types.NewMap(vrw))

	return newTbl, nil
}
