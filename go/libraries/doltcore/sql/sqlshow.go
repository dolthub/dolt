package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/xwb1989/sqlparser"
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
		tableName := show.Table.Name.String()
		if !root.HasTable(ctx, tableName) {
			return nil, nil, errFmt(UnknownTableErrFmt, tableName)
		}

		table, _ := root.GetTable(ctx, tableName)

		sch := table.GetSchema(ctx)
		schemaStr, err := SchemaAsCreateStmt(tableName, sch)
		if err != nil {
			return nil, nil, err
		}

		resultSch := showCreateTableSchema()
		rows := toRows(([][]string{{tableName, schemaStr}}), resultSch)
		source := sourceFuncForRows(rows)
		p := pipeline.NewPartialPipeline(pipeline.ProcFuncForSourceFunc(source), &pipeline.TransformCollection{})

		return p, resultSch, nil

	case "tables":
		tableNames := root.GetTableNames(ctx)
		sch := showTablesSchema()
		rows := toRows(transpose(tableNames), sch)
		source := sourceFuncForRows(rows)
		p := pipeline.NewPartialPipeline(pipeline.ProcFuncForSourceFunc(source), &pipeline.TransformCollection{})
		return p, sch, nil
	default:
		return nil, nil, errFmt("Unsupported show statement: '%v'", nodeToString(show))
	}

	return nil, nil, nil
}

// Takes a single-dimensional array of strings and transposes it to a 2D array, with a single element per row.
func transpose(ss []string) [][]string {
	ret := make([][]string, len(ss))
	for i, s := range ss {
		ret [i] = []string{s}
	}
	return ret
}

// Returns a new result set row with the schema given from the 2D array of row values given.
func toRows(ss [][]string, sch schema.Schema) []row.Row {
	rows := make([]row.Row, len(ss))
	for i, r := range ss {
		taggedVals := make(row.TaggedValues)
		for tag, col := range r {
			taggedVals[uint64(tag)] = types.String(col)
		}
		rows[i] = row.New(sch, taggedVals)
	}
	return rows
}