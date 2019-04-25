package sql

import (
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
	"github.com/xwb1989/sqlparser"
	"strconv"
)

// Struct to represent the salient results of parsing a select statement.
type SelectStatement struct {
	// Result set schema
	ResultSetSchema *resultset.ResultSetSchema
	// Input table schemas, keyed by table name
	inputSchemas map[string]schema.Schema
	// Columns to be selected
	selectedCols []QualifiedColumn
	// Aliases for columns and tables
	aliases *Aliases
	// Filter function for the result set
	filterFn rowFilterFn
	// Limit of results returned
	limit int
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
func ExecuteSelect(root *doltdb.RootValue, s *sqlparser.Select) ([]row.Row, schema.Schema, error) {
	p, statement, err := BuildSelectQueryPipeline(root, s)
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

	return rows, statement.ResultSetSchema.Schema(), nil
}

// BuildSelectQueryPipeline interprets the select statement given, builds a pipeline to execute it, and returns the pipeline
// for the caller to mutate and execute, as well as the schema of the result set. The pipeline will not have any output
// set; one must be assigned before execution.
func BuildSelectQueryPipeline(root *doltdb.RootValue, s *sqlparser.Select) (*pipeline.Pipeline, *SelectStatement, error) {
	selectStmt := &SelectStatement{aliases: NewAliases(), inputSchemas: make(map[string]schema.Schema)}

	if err := processFromClause(root, selectStmt, s.From); err != nil {
		return nil, nil, err
	}

	if err := processSelectedColumns(root, selectStmt, s.SelectExprs); err != nil {
		return nil, nil, err
	}

	if err := processLimitClause(s, selectStmt); err != nil {
		return nil, nil, err
	}

	if err := createResultSetSchema(selectStmt); err != nil {
		return nil, nil, err
	}

	if err := processWhereClause(selectStmt, s); err != nil {
		return nil, nil, err
	}

	p, err := createPipeline(root, selectStmt)
	if err != nil {
		return nil, nil, err
	}

	return p, selectStmt, nil
}

// Processed the limit clause of the SQL select statement given, storing the result of the analysis in the selectStmt
// given or returning any error encountered.
func processLimitClause(s *sqlparser.Select, selectStmt *SelectStatement) error {
	if s.Limit != nil && s.Limit.Rowcount != nil {
		limitVal, ok := s.Limit.Rowcount.(*sqlparser.SQLVal)
		if !ok {
			return errFmt("Couldn't parse limit clause: %v", nodeToString(s.Limit))
		}
		limitInt, err := strconv.Atoi(nodeToString(limitVal))
		if err != nil {
			return errFmt("Couldn't parse limit clause: %v", nodeToString(s.Limit))
		}
		selectStmt.limit = limitInt
	} else {
		selectStmt.limit = -1
	}

	return nil
}

// Processes the from clause of the select statement, storing the result of the analysis in the selectStmt given or
// returning any error encountered.
func processFromClause(root *doltdb.RootValue, selectStmt *SelectStatement, from sqlparser.TableExprs) error {
	for _, tableExpr := range from {
		var tableName string
		switch te := tableExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			switch e := te.Expr.(type) {
			case sqlparser.TableName:
				tableName = e.Name.String()
			case *sqlparser.Subquery:
				return errFmt("Subqueries are not supported: %v.", nodeToString(e))
			default:
				return errFmt("Unrecognized expression: %v", nodeToString(e))
			}

			if !root.HasTable(tableName) {
				return errFmt("Unknown table '%s'", tableName)
			}

			if !te.As.IsEmpty() {
				selectStmt.aliases.AddTableAlias(tableName, te.As.String())
			}
			selectStmt.aliases.AddTableAlias(tableName, tableName)
			selectStmt.inputSchemas[tableName] = mustGetSchema(root, tableName)

		case *sqlparser.ParenTableExpr:
			return errFmt("Parenthetical table expressions are not supported: %v,", nodeToString(te))
		case *sqlparser.JoinTableExpr:
			return errFmt("Joins expressions are not supported: %v,", nodeToString(te))
		default:
			return errFmt("Unsupported select statement: %v", nodeToString(te))
		}
	}

	return nil
}

// Processes the select expression (columns to return from the query). Adds the results to the SelectStatement given,
// or returns an error if it cannot. All aliases must be established in the SelectStatement.
func processSelectedColumns(root *doltdb.RootValue, selectStmt *SelectStatement, colSelections sqlparser.SelectExprs) error {
	var columns []QualifiedColumn
	for _, colSelection := range colSelections {
		switch selectExpr := colSelection.(type) {
		case *sqlparser.StarExpr:
			for tableName, tableSch:= range selectStmt.inputSchemas {
				tableSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
					columns = append(columns, QualifiedColumn{tableName, col.Name})
					return false
				})
			}
		case *sqlparser.AliasedExpr:
			colAlias := selectExpr.As.String()
			switch colExpr := selectExpr.Expr.(type) {
			case *sqlparser.ColName:

				var tableSch schema.Schema
				var tableName string
				colName := colExpr.Name.String()
				if !colExpr.Qualifier.IsEmpty() {
					columnTableName := colExpr.Qualifier.Name.String()
					var ok bool
					if tableName, ok = selectStmt.aliases.TablesByAlias[columnTableName]; ok{
						tableSch = mustGetSchema(root, tableName)
					} else {
						return errFmt("unknown table " + columnTableName)
					}
				} else {
					var err error
					tableName, tableSch, err = findSchemaForColumn(colName, selectStmt)
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
					selectStmt.aliases.AddColumnAlias(tableName, colName, colAlias)
				}
				columns = append(columns, QualifiedColumn{tableName, colName})
			default:
				return errFmt("Only column selections or * are supported")
			}
		case sqlparser.Nextval:
			return errFmt("Next value is not supported: %v", nodeToString(selectExpr))
		}
	}

	selectStmt.selectedCols = columns
	return nil
}

// Finds the schema that contains the column name given among the tables given. Returns an error if no schema contains
// such a column name, or if multiple do. This method is only used for naked column names, not qualified ones. Assumes
// that table names have already been verified to exist.
func findSchemaForColumn(colName string, statement *SelectStatement) (string, schema.Schema, error) {
	schemas := statement.inputSchemas

	var colSchema schema.Schema
	var tableName string
	for tbl, sch := range schemas {
		if _, ok := sch.GetAllCols().GetByName(colName); ok {
			if colSchema != nil {
				return "", nil, errFmt("Ambiguous column: %v", colName)
			}
			colSchema = sch
			tableName = tbl
		}
	}

	if colSchema == nil {
		return "", nil, errFmt("Unknown column '%v'", colName)
	}

	return tableName, colSchema, nil
}

// Gets the schema for the table name given. Will cause a panic if the table doesn't exist.
func mustGetSchema(root *doltdb.RootValue, tableName string) schema.Schema {
	tbl, _:= root.GetTable(tableName)
	return tbl.GetSchema()
}

// Processes the where clause by applying an appropriate filter fn to the SelectStatement given. Returns an error if the
// where clause can't be processed.
func processWhereClause(selectStmt *SelectStatement, s *sqlparser.Select) error {
	// TODO: make work for more than 1 table
	var tableSch schema.Schema
	for _, sch := range selectStmt.inputSchemas {
		tableSch = sch
		break
	}

	filter, err := createFilterForWhere(s.Where, tableSch, selectStmt.aliases)
	if err != nil {
		return err
	}

	selectStmt.filterFn = filter
	return nil
}

// Returns an error with the message specified. Return type includes a nil Pipeline object to conform to the needs of
// BuildSelectQueryPipeline.
func errSelect(fmtMsg string, args ...interface{}) (*pipeline.Pipeline, *SelectStatement, error) {
	return nil, nil, errors.New(fmt.Sprintf(fmtMsg, args...))
}

// createPipeline constructs a pipeline to execute the statement and returns it. The constructed pipeline doesn't have
// an output set, and must be supplied one before execution.
func createPipeline(root *doltdb.RootValue, statement *SelectStatement) (*pipeline.Pipeline, error) {

	// TODO: make work for more than one table
	var tblSch schema.Schema
	var tblName string
	for name, sch := range statement.inputSchemas {
		tblSch = sch
		tblName = name
		break
	}
	tbl, _ := root.GetTable(tblName)

	selTrans := &selectTransform{nil, statement.filterFn, statement.limit, 0}
	transforms := pipeline.NewTransformCollection(pipeline.NewNamedTransform("select", selTrans.limitAndFilter))
	transforms.AppendTransforms(createOutputSchemaMappingTransform(tblSch, statement.ResultSetSchema))

	rd := noms.NewNomsMapReader(tbl.GetRowData(), tblSch)
	rdProcFunc := pipeline.ProcFuncForReader(rd)

	p := pipeline.NewPartialPipeline(rdProcFunc, transforms)
	selTrans.noMoreCallback = func() {p.NoMore()}

	p.RunAfter(func() { rd.Close() })

	return p, nil
}

func createOutputSchemaMappingTransform(tableSch schema.Schema, rss *resultset.ResultSetSchema) pipeline.NamedTransform {
	mapping := rss.Mapping(tableSch)
	rConv, _ := rowconv.NewRowConverter(mapping)
	return pipeline.NewNamedTransform("mapToResultSet", rowconv.GetRowConvTransformFunc(rConv))
}

// Returns a ResultSetSchema for the given select statement, which contains a target schema and mappings to get there
// from the individual table schemas.
func createResultSetSchema(statement *SelectStatement) error {

	// Iterate over the columns twice: first to get an ordered list to use to create an output schema with
	cols := make([]schema.Column, 0, len(statement.selectedCols))
	for _, selectedCol := range statement.selectedCols {
		colName := selectedCol.ColumnName
		tableName := selectedCol.TableName

		if tableSch, ok := statement.inputSchemas[tableName]; !ok {
			panic ("Unknown table " + tableName)
		} else {
			if col, ok := tableSch.GetAllCols().GetByName(colName); !ok {
				panic("Unknown column " + colName)
			} else {
				if alias, ok := statement.aliases.AliasesByColumn[selectedCol]; ok {
					col.Name = alias
				}
				cols = append(cols, col)
			}
		}
	}

	rss, err := resultset.NewFromColumns(cols...)
	if err != nil {
		return err
	}

	// Then a second time, to create a mapping from the source schema to the column in the result set.
	for _, selectedCol := range statement.selectedCols {
		tableSch := statement.inputSchemas[selectedCol.TableName]
		col, _ := tableSch.GetAllCols().GetByName(selectedCol.ColumnName)

		err = rss.AddColumn(tableSch, col)
		if err != nil {
			return err
		}
	}

	// Finally set the result set schema in the statement
	statement.ResultSetSchema = rss

	return nil
}