package sql

import (
	"context"
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func ExecuteShow(ctx context.Context, root *doltdb.RootValue, show *sqlparser.Show) ([]row.Row, schema.Schema, error) {
	p, schema, err := BuildShowPipeline(ctx, root, show)
	if err != nil {
		return nil, nil, err
	}

	var rows []row.Row // your boat
	rowSink := pipeline.ProcFuncForSinkFunc(
		func(r row.Row, props pipeline.ReadableMap) error {
			rows = append(rows, r)
			return nil
		})

	var executionErr error
	errSink := func(failure *pipeline.TransformRowFailure) (quit bool) {
		executionErr = errors.New(fmt.Sprintf("Execution failed at stage %v for row %v: error was %v",
			failure.TransformName, failure.Row, failure.Details))
		return true
	}

	p.SetOutput(rowSink)
	p.SetBadRowCallback(errSink)

	p.Start()
	err = p.Wait()
	if err != nil {
		return nil, nil, err
	}
	if executionErr != nil {
		return nil, nil, executionErr
	}

	return rows, schema, nil
}

func showTablesSchema() schema.Schema {
	colCollection, _ := schema.NewColCollection(schema.NewColumn("tables", 0, types.StringKind, false))
	return schema.UnkeyedSchemaFromCols(colCollection)
}

func showColumnsSchema() schema.Schema {
	colCollection, _ := schema.NewColCollection(
		schema.NewColumn("Field", 0, types.StringKind, false),
		schema.NewColumn("Type", 1, types.StringKind, false),
		schema.NewColumn("Null", 2, types.StringKind, false),
		schema.NewColumn("Key", 3, types.StringKind, false),
		schema.NewColumn("Default", 4, types.StringKind, false),
		schema.NewColumn("Extra", 5, types.StringKind, false),
	)
	return schema.UnkeyedSchemaFromCols(colCollection)
}

func showCreateTableSchema() schema.Schema {
	colCollection, _ := schema.NewColCollection(
		schema.NewColumn("Table", 0, types.StringKind, false),
		schema.NewColumn("Create Table", 1, types.StringKind, false),
	)
	return schema.UnkeyedSchemaFromCols(colCollection)
}

func BuildShowPipeline(ctx context.Context, root *doltdb.RootValue, show *sqlparser.Show) (*pipeline.Pipeline, schema.Schema, error) {

	switch show.Type {
	case "create table":
		tableName, err := resolveTable(show.Table.Name.String(), root.GetTableNames(ctx), NewAliases())
		if err != nil {
			return nil, nil, err
		}

		table, _ := root.GetTable(ctx, tableName)

		sch := table.GetSchema(ctx)
		schemaStr := SchemaAsCreateStmt(tableName, sch)

		resultSch := showCreateTableSchema()
		rows := toRows(root.VRW().Format(), ([][]string{{tableName, schemaStr}}), resultSch)
		source := pipeline.SourceFuncForRows(rows)
		p := pipeline.NewPartialPipeline(pipeline.ProcFuncForSourceFunc(source))

		return p, resultSch, nil

	case "columns":
		tableName, err := resolveTable(show.OnTable.Name.String(), root.GetTableNames(ctx), NewAliases())
		if err != nil {
			return nil, nil, err
		}

		table, _ := root.GetTable(ctx, tableName)

		tableSch := table.GetSchema(ctx)
		rows := schemaAsShowColumnRows(root.VRW().Format(), tableSch)

		source := pipeline.SourceFuncForRows(rows)
		p := pipeline.NewPartialPipeline(pipeline.ProcFuncForSourceFunc(source))
		return p, showColumnsSchema(), nil

	case "tables":
		tableNames := root.GetTableNames(ctx)
		sch := showTablesSchema()
		rows := toRows(root.VRW().Format(), transpose(tableNames), sch)
		source := pipeline.SourceFuncForRows(rows)
		p := pipeline.NewPartialPipeline(pipeline.ProcFuncForSourceFunc(source))
		return p, sch, nil
	default:
		return nil, nil, errFmt("Unsupported show statement: '%v'", nodeToString(show))
	}
}

// schemaAsShowColumnRows returns the rows for a `show columns from table` or `describe table` for the schema given.
func schemaAsShowColumnRows(nbf *types.NomsBinFormat, tableSch schema.Schema) []row.Row {
	rs := make([]row.Row, tableSch.GetAllCols().Size())
	i := 0
	tableSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		rs[i] = describeColumn(nbf, col)
		i++
		return false
	})
	return rs
}

// describeColumn returns a row describing the column given, using the schema from showColumnsSchema
func describeColumn(nbf *types.NomsBinFormat, col schema.Column) row.Row {
	nullStr := "NO"
	if col.IsNullable() {
		nullStr = "YES"
	}
	keyStr := ""
	if col.IsPartOfPK {
		keyStr = "PRI"
	}

	taggedVals := row.TaggedValues{
		0: types.String(col.Name),
		1: types.String(DoltToSQLType[col.Kind]),
		2: types.String(nullStr),
		3: types.String(keyStr),
		4: types.String("NULL"), // TODO: when schemas store defaults, use them here
		5: types.String(""),     // Extra column reserved for future use
	}
	return row.New(nbf, showColumnsSchema(), taggedVals)
}

// Takes a single-dimensional array of strings and transposes it to a 2D array, with a single element per row.
func transpose(ss []string) [][]string {
	ret := make([][]string, len(ss))
	for i, s := range ss {
		ret[i] = []string{s}
	}
	return ret
}

// Returns a new result set row with the schema given from the 2D array of row values given.
func toRows(nbf *types.NomsBinFormat, ss [][]string, sch schema.Schema) []row.Row {
	rows := make([]row.Row, len(ss))
	for i, r := range ss {
		taggedVals := make(row.TaggedValues)
		for tag, col := range r {
			taggedVals[uint64(tag)] = types.String(col)
		}
		rows[i] = row.New(nbf, sch, taggedVals)
	}
	return rows
}
