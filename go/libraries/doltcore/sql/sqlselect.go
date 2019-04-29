package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/xwb1989/sqlparser"
	"strconv"
)

// Struct to represent the salient results of parsing a select statement.
type selectStatement struct {
	tableNames   []string
	selectedCols []string
	aliases      *Aliases
	filterFn     rowFilterFn
	limit        int
}

type selectTransform struct {
	noMoreCallback func()
	filter         rowFilterFn
	limit          int
	count          int
}

// Limits and filter the rows returned by a query
func (st *selectTransform) limitAndFilter(inRow row.Row, props pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {
	if st.limit == -1 || st.count < st.limit {
		if st.filter(inRow) {
			st.count++
			return []*pipeline.TransformedRowResult{{inRow, nil}}, ""
		}
	} else if st.count == st.limit {
		st.noMoreCallback()
	}

	return nil, ""
}

// ExecuteSelect executes the given select query and returns the resultant rows accompanied by their output schema.
func ExecuteSelect(ctx context.Context, root *doltdb.RootValue, s *sqlparser.Select, query string) ([]row.Row, schema.Schema, error) {
	p, schema, err := BuildSelectQueryPipeline(ctx, root, s, query)
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
		executionErr = errors.New(fmt.Sprintf("Query execution failed at stage %v for row %v: error was %v",
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

// BuildSelectQueryPipeline interprets the select statement given, builds a pipeline to execute it, and returns the pipeline
// for the caller to mutate and execute, as well as the schema of the result set. The pipeline will not have any output
// set; one must be assigned before execution.
func BuildSelectQueryPipeline(ctx context.Context, root *doltdb.RootValue, s *sqlparser.Select, query string) (*pipeline.Pipeline, schema.Schema, error) {
	selectStmt := &selectStatement{aliases: NewAliases()}

	if err := processFromClause(ctx, root, selectStmt, s.From, query); err != nil {
		return nil, nil, err
	}

	if err := processSelectedColumns(ctx, root, selectStmt, s.SelectExprs, query); err != nil {
		return nil, nil, err
	}

	if s.Limit != nil && s.Limit.Rowcount != nil {
		limitVal, ok := s.Limit.Rowcount.(*sqlparser.SQLVal)
		if !ok {
			return errSelect("Couldn't parse limit clause: %v", query)
		}
		limitInt, err := strconv.Atoi(nodeToString(limitVal))
		if err != nil {
			return errSelect("Couldn't parse limit clause: %v", query)
		}
		selectStmt.limit = limitInt
	} else {
		selectStmt.limit = -1
	}

	if err := processWhereClause(ctx, root, selectStmt, s); err != nil {
		return nil, nil, err
	}

	return createPipeline(ctx, root, selectStmt)
}

// Processes the from clause of the select statement, storing the result of the analysis in the selectStmt given or
// returning any error encountered.
func processFromClause(ctx context.Context, root *doltdb.RootValue, selectStmt *selectStatement, from sqlparser.TableExprs, query string) error {
	for _, tableExpr := range from {
		var tableName string
		switch te := tableExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			switch e := te.Expr.(type) {
			case sqlparser.TableName:
				tableName = e.Name.String()
			case *sqlparser.Subquery:
				return errFmt("Subqueries are not supported: %v.", query)
			default:
				return errFmt("Unrecognized expression: %v", nodeToString(e))
			}

			if !te.As.IsEmpty() {
				selectStmt.aliases.AddTableAlias(tableName, te.As.String())
			}
			selectStmt.aliases.AddTableAlias(tableName, tableName)
			selectStmt.tableNames = append(selectStmt.tableNames, tableName)

		case *sqlparser.ParenTableExpr:
			return errFmt("Parenthetical table expressions are not supported: %v,", query)
		case *sqlparser.JoinTableExpr:
			return errFmt("Joins are not supported: %v,", query)
		default:
			return errFmt("Unsupported select statement: %v", query)
		}

		if !root.HasTable(ctx, tableName) {
			return errFmt("Unknown table '%s'", tableName)
		}
	}

	return nil
}

// Processes the select expression (columns to return from the query). Adds the results to the selectStatement given,
// or returns an error if it cannot. All aliases must be established in the selectStatement.
func processSelectedColumns(ctx context.Context, root *doltdb.RootValue, selectStmt *selectStatement, colSelections sqlparser.SelectExprs, query string) error {
	var columns []string
	for _, colSelection := range colSelections {
		switch selectExpr := colSelection.(type) {
		case *sqlparser.StarExpr:
			// TODO: this only works for a single table
			tableSch := mustGetSchema(ctx, root, selectStmt.tableNames[0])
			tableSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
				columns = append(columns, col.Name)
				return false
			})
		case *sqlparser.AliasedExpr:
			colAlias := selectExpr.As.String()
			switch colExpr := selectExpr.Expr.(type) {
			case *sqlparser.ColName:

				var tableSch schema.Schema
				colName := colExpr.Name.String()
				if !colExpr.Qualifier.IsEmpty() {
					columnTableName := colExpr.Qualifier.Name.String()
					if tableName, ok := selectStmt.aliases.TablesByAlias[columnTableName]; ok {
						tableSch = mustGetSchema(ctx, root, tableName)
					} else {
						return errFmt("unknown table " + columnTableName)
					}
				} else {
					var err error
					tableSch, err = findSchemaForColumn(ctx, root, colName, selectStmt.tableNames)
					if err != nil {
						return err
					}
				}

				_, ok := tableSch.GetAllCols().GetByName(colName)
				if !ok {
					return errFmt("Unknown column '%v'", colName)
				}

				// an absent column alias will be empty
				if colAlias != "" {
					selectStmt.aliases.AddColumnAlias(colName, colAlias)
				}
				columns = append(columns, colName)
			default:
				return errFmt("Only column selections or * are supported")
			}
		case sqlparser.Nextval:
			return errFmt("Next value is not supported: %v", query)
		}
	}

	selectStmt.selectedCols = columns
	return nil
}

// Finds the schema that contains the column name given among the tables given. Returns an error if no schema contains
// such a column name, or if multiple do. This method is only used for naked column names, not qualified ones. Assumes
// that table names have already been verified to exist.
func findSchemaForColumn(ctx context.Context, root *doltdb.RootValue, colName string, tableNames []string) (schema.Schema, error) {
	schemas := make(map[string]schema.Schema)
	for _, tableName := range tableNames {
		schemas[tableName] = mustGetSchema(ctx, root, tableName)
	}

	var colSchema schema.Schema
	for _, sch := range schemas {
		if _, ok := sch.GetAllCols().GetByName(colName); ok {
			if colSchema != nil {
				return nil, errFmt("Ambiguous column: %v", colName)
			}
			colSchema = sch
		}
	}

	if colSchema == nil {
		return nil, errFmt("Unknown column '%v'", colName)
	}

	return colSchema, nil
}

// Gets the schema for the table name given. Will cause a panic if the table doesn't exist.
func mustGetSchema(ctx context.Context, root *doltdb.RootValue, tableName string) schema.Schema {
	tbl, _ := root.GetTable(ctx, tableName)
	return tbl.GetSchema()
}

// Processes the where clause by applying an appropriate filter fn to the selectStatement given. Returns an error if the
// where clause can't be processed.
func processWhereClause(ctx context.Context, root *doltdb.RootValue, selectStmt *selectStatement, s *sqlparser.Select) error {
	// TODO: make work for more than 1 table
	tableSch := mustGetSchema(ctx, root, selectStmt.tableNames[0])

	filter, err := createFilterForWhere(s.Where, tableSch, selectStmt.aliases)
	if err != nil {
		return err
	}

	selectStmt.filterFn = filter
	return nil
}

// Returns an error with the message specified. Return type includes a nil Pipeline object to conform to the needs of
// BuildSelectQueryPipeline.
func errSelect(fmtMsg string, args ...interface{}) (*pipeline.Pipeline, schema.Schema, error) {
	return nil, nil, errors.New(fmt.Sprintf(fmtMsg, args...))
}

// createPipeline constructs a pipeline to execute the statement and returns it. The constructed pipeline doesn't have
// an output set, and must be supplied one before execution.
func createPipeline(ctx context.Context, root *doltdb.RootValue, statement *selectStatement) (*pipeline.Pipeline, schema.Schema, errhand.VerboseError) {
	tbl, _ := root.GetTable(ctx, statement.tableNames[0])
	tblSch := tbl.GetSchema()

	selTrans := &selectTransform{nil, statement.filterFn, statement.limit, 0}
	transforms := pipeline.NewTransformCollection(pipeline.NewNamedTransform("select", selTrans.limitAndFilter))
	outSchema, verr := addColumnMapTransform(statement, tblSch, transforms)

	if verr != nil {
		return nil, nil, verr
	}

	rd := noms.NewNomsMapReader(ctx, tbl.GetRowData(), tblSch)
	rdProcFunc := pipeline.ProcFuncForReader(ctx, rd)

	p := pipeline.NewPartialPipeline(rdProcFunc, transforms)
	selTrans.noMoreCallback = func() { p.NoMore() }

	p.RunAfter(func() { rd.Close(ctx) })

	return p, outSchema, nil
}

// Adds a transformation that maps column names in a result set to a new set of columns.
func addColumnMapTransform(statement *selectStatement, tableSch schema.Schema, transforms *pipeline.TransformCollection) (schema.Schema, errhand.VerboseError) {
	colColl := tableSch.GetAllCols()

	if len(statement.selectedCols) > 0 {
		cols := make([]schema.Column, 0, len(statement.selectedCols))

		for _, colName := range statement.selectedCols {
			if col, ok := colColl.GetByName(colName); !ok {
				panic("unknown column " + colName)
			} else {
				if alias, ok := statement.aliases.AliasesByColumn[colName]; ok {
					col.Name = alias
				}
				cols = append(cols, col)
			}
		}

		colColl, _ = schema.NewColCollection(cols...)
	}

	outSch := schema.UnkeyedSchemaFromCols(colColl)
	mapping, err := rowconv.TagMapping(tableSch, untyped.UntypeUnkeySchema(outSch))

	if err != nil {
		panic(err)
	}

	rConv, _ := rowconv.NewRowConverter(mapping)
	transform := pipeline.NewNamedTransform("map", rowconv.GetRowConvTransformFunc(rConv))
	transforms.AppendTransforms(transform)

	return mapping.DestSch, nil
}
