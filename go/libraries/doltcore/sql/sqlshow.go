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

func createShowTablesSchema() schema.Schema {
	colCollection, _ := schema.NewColCollection(schema.NewColumn("tables", 0, types.StringKind, false))
	return schema.UnkeyedSchemaFromCols(colCollection)
}

func BuildShowPipeline(ctx context.Context, root *doltdb.RootValue, show *sqlparser.Show) (*pipeline.Pipeline, schema.Schema, error) {

	switch show.Type {
	case "tables":
		tableNames := root.GetTableNames(ctx)
		sch := createShowTablesSchema()
		rows := toRows(tableNames, sch)
		source := sourceFuncForRows(rows)
		p := pipeline.NewPartialPipeline(pipeline.ProcFuncForSourceFunc(source), &pipeline.TransformCollection{})
		return p, sch, nil
	default:
		return nil, nil, errFmt("Unsupported show statement: '%v'", nodeToString(show))
	}

	return nil, nil, nil
}

func toRows(strings []string, sch schema.Schema) []row.Row {
	if sch.GetAllCols().Size() != 1 {
		panic("toRows requires a schema with a single column")
	}

	rows := make([]row.Row, len(strings))
	for i, s := range strings {
		rows[i] = row.New(sch, row.TaggedValues{0: types.String(s)})
	}
	return rows
}