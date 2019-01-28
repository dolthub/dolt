package merge

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
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
	tblSch, err := noms.UnmarshalNomsValue(tblSchVal)

	if err != nil {
		return nil, err
	}

	conflicts := tbl.GetConflicts()
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
			rd := table.RowDataFromPKAndValueList(tblSch, key, updated.(types.Tuple))

			if !rd.IsValid() {
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

	return doltdb.NewTable(vrw, tblSchVal, rowEditor.Map()), nil
}
