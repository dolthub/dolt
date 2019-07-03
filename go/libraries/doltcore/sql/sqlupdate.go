package sql

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"vitess.io/vitess/go/vt/sqlparser"
)

type UpdateResult struct {
	Root             *doltdb.RootValue
	NumRowsUpdated   int
	NumErrorsIgnored int
	NumRowsUnchanged int
	// TODO: update ignore not supported by parser yet
}

func ExecuteUpdate(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, s *sqlparser.Update, query string) (*UpdateResult, error) {
	tableExprs := s.TableExprs
	if len(tableExprs) != 1 {
		return errUpdate("Exactly one table to update must be specified")
	}

	var tableName string
	tableExpr := tableExprs[0]
	switch t := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch e := t.Expr.(type) {
		case sqlparser.TableName:
			tableName = e.Name.String()
		case *sqlparser.Subquery:
			return errUpdate("Subqueries are not supported: %v.", query)
		default:
			return errUpdate("Unrecognized expression: %v", nodeToString(e))
		}
	case *sqlparser.ParenTableExpr:
		return errUpdate("Only simple table expression are supported")
	case *sqlparser.JoinTableExpr:
		return errUpdate("Joins are not supported")
	default:
		return errUpdate("Unsupported update statement %v", query)
	}

	if !root.HasTable(ctx, tableName) {
		return errUpdate("Unknown table '%s'", tableName)
	}
	table, _ := root.GetTable(ctx, tableName)
	tableSch := table.GetSchema(ctx)

	setVals := make(map[uint64]*RowValGetter)
	schemas := map[string]schema.Schema{tableName: tableSch}
	aliases := NewAliases()
	rss := resultset.Identity(tableName, tableSch)

	for _, update := range s.Exprs {
		colName := update.Name.Name.String()
		column, ok := tableSch.GetAllCols().GetByName(colName)
		if !ok {
			return errUpdate(UnknownColumnErrFmt, colName)
		}

		if column.IsPartOfPK {
			return errUpdate("Cannot update primary key column '%v'", colName)
		}

		if _, ok = setVals[column.Tag]; ok {
			return errUpdate("Repeated column: '%v'", colName)
		}

		// TODO: support aliases, multiple table updates
		getter, err := getterFor(update.Expr, schemas, aliases)
		if err != nil {
			return nil, err
		}

		if getter.NomsKind != column.Kind {
			getter, err = ConversionValueGetter(getter, column.Kind)
			if err != nil {
				return errUpdate("Error processing update clause '%v': %v", nodeToString(update), err.Error())
			}
		}

		if err = getter.Init(rss); err != nil {
			return errUpdate(err.Error())
		}

		setVals[column.Tag] = getter
	}

	// TODO: support aliases in where clauses
	filter, err := createFilterForWhere(s.Where, schemas, aliases)
	if err != nil {
		return errUpdate(err.Error())
	}
	if err := filter.Init(rss); err != nil {
		return errUpdate(err.Error())
	}

	// Perform the update
	var result UpdateResult
	rowData := table.GetRowData(ctx)
	me := rowData.Edit()
	rowReader := noms.NewNomsMapReader(ctx, rowData, tableSch)

	for {
		r, err := rowReader.ReadRow(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if !filter.filter(r) {
			continue
		}

		var anyColChanged bool

		for tag, getter := range setVals {
			currVal, _ := r.GetColVal(tag)
			val := getter.Get(r)

			if (currVal == nil && val != nil) || (currVal != nil && !currVal.Equals(types.Format_7_18, val)) {
				anyColChanged = true
			}

			r, err = r.SetColVal(tag, val, tableSch)
			if err != nil {
				return nil, err
			}
		}

		if !row.IsValid(r, tableSch) {
			col, constraint := row.GetInvalidConstraint(r, tableSch)
			return nil, errFmt(ConstraintFailedFmt, col.Name, constraint)
		}

		tvs := r.NomsMapKey(types.Format_7_18, tableSch).(row.TupleVals)
		key := tvs.Value(ctx)

		if anyColChanged {
			result.NumRowsUpdated += 1
		} else {
			result.NumRowsUnchanged += 1
		}

		me.Set(key, r.NomsMapValue(types.Format_7_18, tableSch))
	}
	table = table.UpdateRows(ctx, me.Map(ctx))

	result.Root = root.PutTable(ctx, db, tableName, table)
	return &result, nil
}

func errUpdate(errorFmt string, args ...interface{}) (*UpdateResult, error) {
	return nil, errors.New(fmt.Sprintf(errorFmt, args...))
}
