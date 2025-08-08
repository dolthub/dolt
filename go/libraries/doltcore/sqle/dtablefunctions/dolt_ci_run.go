// Copyright 2025 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dtablefunctions

import (
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/dolt_ci"
	"github.com/dolthub/dolt/go/store/val"
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"io"
	"strings"
)

var _ sql.TableFunction = (*CiRunTableFunction)(nil)
var _ sql.CatalogTableFunction = (*CiRunTableFunction)(nil)
var _ sql.ExecSourceRel = (*CiRunTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*CiRunTableFunction)(nil)

type CiRunTableFunction struct {
	ctx      *sql.Context
	catalog  sql.Catalog
	database sql.Database
	argument sql.Expression
	engine   *gms.Engine

	workflowName      string
	savedQueryResults []ciStep
}

var ciRunTableSchema = sql.Schema{
	&sql.Column{Name: "job_name", Type: types.Text},
	&sql.Column{Name: "step_name", Type: types.Text},
	&sql.Column{Name: "query", Type: types.Text},
	&sql.Column{Name: "status", Type: types.Text},
	&sql.Column{Name: "error", Type: types.Text},
}

// NewInstance creates a new instance of TableFunction interface
func (crtf *CiRunTableFunction) NewInstance(ctx *sql.Context, database sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &CiRunTableFunction{
		ctx:      ctx,
		database: database,
	}
	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// WithCatalog implements the sql.CatalogTableFunction interface
func (crtf *CiRunTableFunction) WithCatalog(c sql.Catalog) (sql.TableFunction, error) {
	newInstance := *crtf
	newInstance.catalog = c
	pro, ok := c.(sql.DatabaseProvider)
	if !ok {
		return nil, fmt.Errorf("unable to get database provider")
	}
	newInstance.engine = gms.NewDefault(pro)

	workflow, err := expressionToString(crtf.ctx, crtf.argument)
	if err != nil {
		return nil, err
	}
	newInstance.workflowName = workflow

	if err := newInstance.assertWorkflowJobsAndSteps(); err != nil {
		return nil, err
	}
	return &newInstance, nil
}

type ciStep struct {
	stepName, jobName, queryStatement, errStr string
}

func (crtf *CiRunTableFunction) validateWorkflowName() error {
	qry := fmt.Sprintf("SELECT * FROM dolt_ci_workflows where name = '%s'", crtf.workflowName)
	_, iter, _, err := crtf.engine.Query(crtf.ctx, qry)
	if err != nil {
		return err
	}
	if _, err = iter.Next(crtf.ctx); err != nil {
		return fmt.Errorf("could not find workflow with name: %s", crtf.workflowName)
	}

	return nil
}

func (crtf *CiRunTableFunction) getJobs() ([]dolt_ci.WorkflowJob, error) {
	var jobs []dolt_ci.WorkflowJob
	qry := fmt.Sprintf("SELECT * FROM dolt_ci_workflow_jobs where workflow_name_fk = '%s'", crtf.workflowName)
	_, iter, _, err := crtf.engine.Query(crtf.ctx, qry)
	if err != nil {
		return nil, err
	}

	for {
		row, err := iter.Next(crtf.ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		var job dolt_ci.WorkflowJob
		jobId := dolt_ci.WorkflowJobId(row[0].(string))
		job.Id = &jobId
		job.Name = row[1].(string)

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (crtf *CiRunTableFunction) getSavedQueryData(stepId string) (savedQueryId string, savedQueryStatement string, stepExpectType int32, ciErr string, err error) {
	qry := fmt.Sprintf("SELECT id, saved_query_name, expected_results_type FROM dolt_ci_workflow_saved_query_steps WHERE workflow_step_id_fk = '%s'", stepId)
	_, iter, _, err := crtf.engine.Query(crtf.ctx, qry)
	if err != nil {
		return "", "", 0, "", err
	}

	queryIdName, err := iter.Next(crtf.ctx)
	if err != nil {
		return "", "", 0, "", fmt.Errorf("ci tables are malformed")
	}
	savedQueryId = queryIdName[0].(string)
	savedQueryName := queryIdName[1].(string)
	stepExpectType = queryIdName[2].(int32)

	qry = fmt.Sprintf("SELECT query FROM dolt_query_catalog WHERE id = '%s'", savedQueryName)
	_, iter, _, err = crtf.engine.Query(crtf.ctx, qry)
	if err != nil {
		return "", "", 0, "", err
	}

	queryRow, err := iter.Next(crtf.ctx)
	if err == io.EOF {
		ciErr = fmt.Sprintf("saved query does not exist: %s", savedQueryName)
		return savedQueryId, "", stepExpectType, ciErr, nil
	} else if err != nil {
		return "", "", 0, "", err
	}

	savedQueryStatement, err = getStringColAsString(crtf.ctx, queryRow[0])
	if err != nil {
		return "", "", 0, "", err
	}

	return savedQueryId, savedQueryStatement, stepExpectType, ciErr, nil
}

func (crtf *CiRunTableFunction) assertWorkflowJobsAndSteps() error {
	if err := crtf.validateWorkflowName(); err != nil {
		return err
	}

	jobs, err := crtf.getJobs()
	if err != nil {
		return err
	}

	for _, job := range jobs {
		qry := fmt.Sprintf("SELECT * FROM dolt_ci_workflow_steps WHERE workflow_job_id_fk = '%s' ORDER BY step_order ASC", *job.Id)
		_, stepIter, _, err := crtf.engine.Query(crtf.ctx, qry)
		if err != nil {
			return err
		}

		for {
			row, err := stepIter.Next(crtf.ctx)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			// Get the sql statement and id of the saved query. We use ciErr to denote CI failures that should be
			// displayed to the user, and err for internal dolt problems that should halt the process.
			savedQueryId, savedQueryStatement, stepExpectType, ciErr, err := crtf.getSavedQueryData(row[0].(string))
			if err != nil {
				return err
			}

			if ciErr == "" {
				qry, err = getStringColAsString(crtf.ctx, savedQueryStatement)
				if err != nil {
					return err
				}
				_, iter, _, err := crtf.engine.Query(crtf.ctx, qry)
				if err != nil {
					ciErr = fmt.Sprintf("query error: %s", err.Error())
				} else if stepExpectType == 1 {
					ciErr = crtf.assertQuery(iter, savedQueryId)
				}
			}

			crtf.savedQueryResults = append(crtf.savedQueryResults, ciStep{row[1].(string), job.Name, savedQueryStatement, ciErr})
		}
	}
	return nil
}

func (crtf *CiRunTableFunction) assertQuery(resultIter sql.RowIter, savedQueryId string) string {
	qry := fmt.Sprintf("SELECT expected_column_count_comparison_type, expected_row_count_comparison_type, expected_column_count, expected_row_count FROM dolt_ci_workflow_saved_query_step_expected_row_column_results"+
		" where workflow_saved_query_step_id_fk = '%s'", savedQueryId)
	_, iter, _, err := crtf.engine.Query(crtf.ctx, qry)
	if err != nil {
		return "error: malformed CI tables, could not get expected row and column counts"
	}

	row, err := iter.Next(crtf.ctx)
	if err != nil {
		return "error: malformed CI tables, could not get expected row and column counts"
	}
	colType := dolt_ci.WorkflowSavedQueryExpectedRowColumnComparisonType(row[0].(int32))
	rowType := dolt_ci.WorkflowSavedQueryExpectedRowColumnComparisonType(row[1].(int32))
	colCount := row[2].(int64)
	rowCount := row[3].(int64)

	var actualRows, actualColumns int64
	for {
		row, err = resultIter.Next(crtf.ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Sprintf("unexpected results from saved query, got error: %s", err.Error())
		}

		actualRows++
		actualColumns = int64(len(row))
	}
	var errs []string
	var colErr, rowErr error

	if colType != 0 {
		colErr = dolt_ci.ValidateQueryExpectedRowOrColumnCount(actualColumns, colCount, colType, "column")
		if colErr != nil {
			errs = append(errs, colErr.Error())
		}
	}
	if rowType != 0 {
		rowErr = dolt_ci.ValidateQueryExpectedRowOrColumnCount(actualRows, rowCount, rowType, "row")
		if rowErr != nil {
			errs = append(errs, rowErr.Error())
		}
	}

	if len(errs) > 0 {
		return strings.Join(errs, "\n")
	}

	return ""
}

// Database implements the sql.Databaser interface
func (crtf *CiRunTableFunction) Database() sql.Database {
	return crtf.database
}

// WithDatabase implements the sql.Databaser interface
func (crtf *CiRunTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	ntf := *crtf
	ntf.database = database
	return &ntf, nil
}

// Expressions implements the sql.Expressioner interface
func (crtf *CiRunTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{crtf.argument}
}

// WithExpressions implements the sql.Expressioner interface
func (crtf *CiRunTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(crtf.Name(), "1", len(expression))
	}

	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(crtf.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(crtf.Name(), expr.String())
		}
	}

	newCrtf := *crtf
	newCrtf.argument = expression[0]

	return &newCrtf, nil
}

// Children implements the sql.Node interface
func (crtf *CiRunTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface
func (crtf *CiRunTableFunction) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return crtf, nil
}

// CheckAuth implements the interface sql.AuthorizationCheckerNode.
func (crtf *CiRunTableFunction) CheckAuth(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	subject := sql.PrivilegeCheckSubject{Database: crtf.database.Name()}
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
}

// Schema implements the sql.Node interface
func (crtf *CiRunTableFunction) Schema() sql.Schema {
	return ciRunTableSchema
}

// Resolved implements the sql.Resolvable interface
func (crtf *CiRunTableFunction) Resolved() bool {
	return crtf.argument.Resolved()
}

func (crtf *CiRunTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (crtf *CiRunTableFunction) String() string { //TODO THIS DOESN'T SEEM VERY SMART
	return fmt.Sprintf("DOLT_CI_RUN('%s')", crtf.argument.String())
}

// Name implements the sql.TableFunction interface
func (crtf *CiRunTableFunction) Name() string {
	return "dolt_ci_run"
}

// RowIter implements the sql.Node interface
func (crtf *CiRunTableFunction) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	// Evaluate the argument to get the string value
	var rows []sql.Row

	for _, step := range crtf.savedQueryResults {
		status := "PASS"
		var row sql.Row
		if step.errStr != "" {
			status = "FAIL"
			row = sql.NewRow(step.jobName, step.stepName, step.queryStatement, status, step.errStr)
		} else {
			row = sql.NewRow(step.jobName, step.stepName, "", status, step.errStr)
		}
		rows = append(rows, row)
	}

	// Create a single row with the argument value in the "hello" column
	return sql.RowsToRowIter(rows...), nil
}

// DataLength estimates total data size for query planning.
func (crtf *CiRunTableFunction) DataLength(ctx *sql.Context) (uint64, error) { //TODO FIX THIS
	return 50, nil
}

// RowCount returns estimated row count for query planning.
func (crtf *CiRunTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) { //TODO FIX THIS
	return 1, true, nil
}

// The dolt_query_catalog system table returns *val.TextStorage types under certain situations,
// so we use a special parser to get the correct string values
func getStringColAsString(sqlCtx *sql.Context, tableValue interface{}) (string, error) {
	if ts, ok := tableValue.(*val.TextStorage); ok {
		return ts.Unwrap(sqlCtx)
	} else if str, ok := tableValue.(string); ok {
		return str, nil
	} else {
		return "", fmt.Errorf("unexpected type %T, was expecting string", tableValue)
	}
}
