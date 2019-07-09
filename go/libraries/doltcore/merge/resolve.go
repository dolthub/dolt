package merge

import (
	"context"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

type AutoResolver func(key types.Value, conflict doltdb.Conflict) (types.Value, error)

func Ours(key types.Value, cnf doltdb.Conflict) (types.Value, error) {
	return cnf.Value, nil
}

func Theirs(key types.Value, cnf doltdb.Conflict) (types.Value, error) {
	return cnf.MergeValue, nil
}

func ResolveTable(ctx context.Context, vrw types.ValueReadWriter, tbl *doltdb.Table, autoResFunc AutoResolver) (*doltdb.Table, error) {
	if !tbl.HasConflicts() {
		return nil, doltdb.ErrNoConflicts
	}

	tblSchRef := tbl.GetSchemaRef()
	tblSchVal := tblSchRef.TargetValue(ctx, vrw)
	tblSch, err := encoding.UnmarshalNomsValue(ctx, vrw.Format(), tblSchVal)

	if err != nil {
		return nil, err
	}

	schemas, conflicts, err := tbl.GetConflicts(ctx)

	if err != nil {
		return nil, err
	}

	rowEditor := tbl.GetRowData(ctx).Edit()

	var itrErr error
	conflicts.Iter(ctx, func(key, value types.Value) (stop bool) {
		cnf := doltdb.ConflictFromTuple(value.(types.Tuple))

		var updated types.Value
		updated, itrErr = autoResFunc(key, cnf)

		if itrErr != nil {
			return true
		}

		if types.IsNull(updated) {
			rowEditor.Remove(key)
		} else {
			r := row.FromNoms(vrw.Format(), tblSch, key.(types.Tuple), updated.(types.Tuple))

			if !row.IsValid(r, tblSch) {
				itrErr = table.NewBadRow(r)
				return true
			}

			rowEditor.Set(key, updated)
		}

		return false
	})

	if itrErr != nil {
		return nil, itrErr
	}

	newTbl := doltdb.NewTable(ctx, vrw, tblSchVal, rowEditor.Map(ctx))
	newTbl = newTbl.SetConflicts(ctx, schemas, types.NewMap(ctx, vrw))

	return newTbl, nil
}
