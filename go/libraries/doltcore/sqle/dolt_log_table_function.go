// Copyright 2022 Dolthub, Inc.
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

package sqle

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
)

var _ sql.TableFunction = (*LogTableFunction)(nil)

type LogTableFunction struct {
	ctx *sql.Context

	revisionExpr       sql.Expression
	secondRevisionExpr sql.Expression

	notRevision string
	minParents  int
	showParents bool
	decoration  string

	database sql.Database
}

var logTableSchema = sql.Schema{
	&sql.Column{Name: "commit_hash", Type: types.Text},
	&sql.Column{Name: "committer", Type: types.Text},
	&sql.Column{Name: "email", Type: types.Text},
	&sql.Column{Name: "date", Type: types.Datetime},
	&sql.Column{Name: "message", Type: types.Text},
}

// NewInstance creates a new instance of TableFunction interface
func (ltf *LogTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &LogTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Database implements the sql.Databaser interface
func (ltf *LogTableFunction) Database() sql.Database {
	return ltf.database
}

// WithDatabase implements the sql.Databaser interface
func (ltf *LogTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	ltf.database = database
	return ltf, nil
}

// Name implements the sql.TableFunction interface
func (ltf *LogTableFunction) Name() string {
	return "dolt_log"
}

// Resolved implements the sql.Resolvable interface
func (ltf *LogTableFunction) Resolved() bool {
	if ltf.revisionExpr != nil && ltf.secondRevisionExpr != nil {
		return ltf.revisionExpr.Resolved() && ltf.secondRevisionExpr.Resolved()
	}
	if ltf.revisionExpr != nil {
		return ltf.revisionExpr.Resolved()
	}
	return true
}

// String implements the Stringer interface
func (ltf *LogTableFunction) String() string {
	return fmt.Sprintf("DOLT_LOG(%s)", ltf.getOptionsString())
}

func (ltf *LogTableFunction) getOptionsString() string {
	var options []string

	if ltf.revisionExpr != nil {
		options = append(options, ltf.revisionExpr.String())
	}

	if ltf.secondRevisionExpr != nil {
		options = append(options, ltf.secondRevisionExpr.String())
	}

	if len(ltf.notRevision) > 0 {
		options = append(options, fmt.Sprintf("--%s %s", cli.NotFlag, ltf.notRevision))
	}

	if ltf.minParents > 0 {
		options = append(options, fmt.Sprintf("--%s %d", cli.MinParentsFlag, ltf.minParents))
	}

	if ltf.showParents {
		options = append(options, fmt.Sprintf("--%s", cli.ParentsFlag))
	}

	if len(ltf.decoration) > 0 && ltf.decoration != "auto" {
		options = append(options, fmt.Sprintf("--%s %s", cli.DecorateFlag, ltf.decoration))
	}

	return strings.Join(options, ", ")
}

// Schema implements the sql.Node interface.
func (ltf *LogTableFunction) Schema() sql.Schema {
	logSchema := logTableSchema

	if ltf.showParents {
		logSchema = append(logSchema, &sql.Column{Name: "parents", Type: types.Text})
	}
	if shouldDecorateWithRefs(ltf.decoration) {
		logSchema = append(logSchema, &sql.Column{Name: "refs", Type: types.Text})
	}

	return logSchema
}

// Children implements the sql.Node interface.
func (ltf *LogTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (ltf *LogTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return ltf, nil
}

// CheckPrivileges implements the interface sql.Node.
func (ltf *LogTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	tblNames, err := ltf.database.GetTableNames(ctx)
	if err != nil {
		return false
	}

	var operations []sql.PrivilegedOperation
	for _, tblName := range tblNames {
		operations = append(operations, sql.NewPrivilegedOperation(ltf.database.Name(), tblName, "", sql.PrivilegeType_Select))
	}

	return opChecker.UserHasPrivileges(ctx, operations...)
}

// Expressions implements the sql.Expressioner interface.
func (ltf *LogTableFunction) Expressions() []sql.Expression {
	exprs := []sql.Expression{}
	if ltf.revisionExpr != nil {
		exprs = append(exprs, ltf.revisionExpr)
	}
	if ltf.secondRevisionExpr != nil {
		exprs = append(exprs, ltf.secondRevisionExpr)
	}
	return exprs
}

// getDoltArgs builds an argument string from sql expressions so that we can
// later parse the arguments with the same util as the CLI
func getDoltArgs(ctx *sql.Context, expressions []sql.Expression, name string) ([]string, error) {
	var args []string

	for _, expr := range expressions {
		childVal, err := expr.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}

		if !types.IsText(expr.Type()) {
			return args, sql.ErrInvalidArgumentDetails.New(name, expr.String())
		}

		text, err := types.Text.Convert(childVal)
		if err != nil {
			return nil, err
		}

		if text != nil {
			args = append(args, text.(string))
		}
	}

	return args, nil
}

func (ltf *LogTableFunction) addOptions(expression []sql.Expression) error {
	args, err := getDoltArgs(ltf.ctx, expression, ltf.Name())
	if err != nil {
		return err
	}

	apr, err := cli.CreateLogArgParser().Parse(args)
	if err != nil {
		return sql.ErrInvalidArgumentDetails.New(ltf.Name(), err.Error())
	}

	if notRevisionStr, ok := apr.GetValue(cli.NotFlag); ok {
		ltf.notRevision = notRevisionStr
	}

	minParents := apr.GetIntOrDefault(cli.MinParentsFlag, 0)
	if apr.Contains(cli.MergesFlag) {
		minParents = 2
	}

	ltf.minParents = minParents
	ltf.showParents = apr.Contains(cli.ParentsFlag)

	decorateOption := apr.GetValueOrDefault(cli.DecorateFlag, "auto")
	switch decorateOption {
	case "short", "full", "auto", "no":
	default:
		return sql.ErrInvalidArgumentDetails.New(ltf.Name(), fmt.Sprintf("invalid --decorate option: %s", decorateOption))
	}
	ltf.decoration = decorateOption

	return nil
}

// WithExpressions implements the sql.Expressioner interface.
func (ltf *LogTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(ltf.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(ltf.Name(), expr.String())
		}
	}

	if err := ltf.addOptions(expression); err != nil {
		return nil, err
	}

	// Gets revisions, excluding any flag-related expression
	var filteredExpressions []sql.Expression
	for i, ex := range expression {
		if !strings.Contains(ex.String(), "--") && !(i > 0 && strings.Contains(expression[i-1].String(), "--")) {
			filteredExpressions = append(filteredExpressions, ex)
		}
	}

	if len(filteredExpressions) > 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(ltf.Name(), "0 to 2", len(filteredExpressions))
	}

	exLen := len(filteredExpressions)
	if exLen > 0 {
		ltf.revisionExpr = filteredExpressions[0]
	}
	if exLen == 2 {
		ltf.secondRevisionExpr = filteredExpressions[1]
	}

	if err := ltf.validateRevisionExpressions(); err != nil {
		return nil, err
	}

	return ltf, nil
}

func (ltf *LogTableFunction) invalidArgDetailsErr(expr sql.Expression, reason string) *errors.Error {
	return sql.ErrInvalidArgumentDetails.New(ltf.Name(), fmt.Sprintf("%s - %s", expr.String(), reason))
}

func (ltf *LogTableFunction) validateRevisionExpressions() error {
	// We must convert the expressions to strings before making string comparisons
	// For dolt_log('^main'), ltf.revisionExpr.String() = "'^main'"" and revisionStr = "^main"
	revisionStr := ""
	secondRevisionStr := ""

	if ltf.revisionExpr != nil {
		revisionStr = mustExpressionToString(ltf.ctx, ltf.revisionExpr)
		if !types.IsText(ltf.revisionExpr.Type()) {
			return sql.ErrInvalidArgumentDetails.New(ltf.Name(), ltf.revisionExpr.String())
		}
		if ltf.secondRevisionExpr == nil && strings.HasPrefix(revisionStr, "^") {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "second revision must exist if first revision contains '^'")
		}
		if strings.Contains(revisionStr, "..") && strings.HasPrefix(revisionStr, "^") {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "revision cannot contain both '..' or '...' and '^'")
		}
	}

	if ltf.secondRevisionExpr != nil {
		secondRevisionStr = mustExpressionToString(ltf.ctx, ltf.secondRevisionExpr)
		if !types.IsText(ltf.secondRevisionExpr.Type()) {
			return sql.ErrInvalidArgumentDetails.New(ltf.Name(), ltf.secondRevisionExpr.String())
		}
		if strings.Contains(secondRevisionStr, "..") {
			return ltf.invalidArgDetailsErr(ltf.secondRevisionExpr, "second revision cannot contain '..' or '...'")
		}
		if strings.Contains(revisionStr, "..") {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "revision cannot contain '..' or '...' if second revision exists")
		}
	}

	if ltf.revisionExpr != nil && ltf.secondRevisionExpr != nil {
		if strings.HasPrefix(revisionStr, "^") && strings.HasPrefix(secondRevisionStr, "^") {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "both revisions cannot contain '^'")
		}
		if !strings.HasPrefix(revisionStr, "^") && !strings.HasPrefix(secondRevisionStr, "^") {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "one revision must contain '^' if two revisions provided")
		}
	}

	if len(ltf.notRevision) > 0 {
		if ltf.revisionExpr == nil && ltf.secondRevisionExpr == nil {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "must have revision in order to use --not")
		}
		if ltf.revisionExpr != nil && (strings.Contains(revisionStr, "..") || strings.HasPrefix(revisionStr, "^")) {
			return ltf.invalidArgDetailsErr(ltf.revisionExpr, "cannot use --not if dots or '^' present in revision")
		}
		if ltf.secondRevisionExpr != nil && strings.HasPrefix(secondRevisionStr, "^") {
			return ltf.invalidArgDetailsErr(ltf.secondRevisionExpr, "cannot use --not if '^' present in second revision")
		}
		if strings.Contains(ltf.notRevision, "..") {
			return sql.ErrInvalidArgumentDetails.New(ltf.Name(), fmt.Sprintf("%s - %s", ltf.notRevision, "--not revision cannot contain '..'"))
		}
		if strings.HasPrefix(ltf.notRevision, "^") {
			return sql.ErrInvalidArgumentDetails.New(ltf.Name(), fmt.Sprintf("%s - %s", ltf.notRevision, "--not revision cannot contain '^'"))
		}
	}

	return nil
}

// RowIter implements the sql.Node interface
func (ltf *LogTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	revisionVal, secondRevisionVal, threeDot, err := ltf.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := ltf.database.(SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", ltf.database)
	}

	sess := dsess.DSessFromSess(ctx.Session)
	var commit *doltdb.Commit

	if len(revisionVal) > 0 {
		cs, err := doltdb.NewCommitSpec(revisionVal)
		if err != nil {
			return nil, err
		}

		commit, err = sqledb.DbData().Ddb.Resolve(ctx, cs, nil)
		if err != nil {
			return nil, err
		}
	} else {
		// If revisionExpr not defined, use session head
		commit, err = sess.GetHeadCommit(ctx, sqledb.Name())
		if err != nil {
			return nil, err
		}
	}

	matchFunc := func(commit *doltdb.Commit) (bool, error) {
		return commit.NumParents() >= ltf.minParents, nil
	}

	cHashToRefs, err := getCommitHashToRefs(ctx, sqledb.DbData().Ddb, ltf.decoration)
	if err != nil {
		return nil, err
	}

	// Two and three dot log
	if len(secondRevisionVal) > 0 {
		secondCs, err := doltdb.NewCommitSpec(secondRevisionVal)
		if err != nil {
			return nil, err
		}

		secondCommit, err := sqledb.DbData().Ddb.Resolve(ctx, secondCs, nil)
		if err != nil {
			return nil, err
		}

		if threeDot {
			mergeBase, err := merge.MergeBase(ctx, commit, secondCommit)
			if err != nil {
				return nil, err
			}

			mergeCs, err := doltdb.NewCommitSpec(mergeBase.String())
			if err != nil {
				return nil, err
			}

			// Use merge base as excluding commit
			mergeCommit, err := sqledb.DbData().Ddb.Resolve(ctx, mergeCs, nil)
			if err != nil {
				return nil, err
			}

			return ltf.NewDotDotLogTableFunctionRowIter(ctx, sqledb.DbData().Ddb, []*doltdb.Commit{commit, secondCommit}, mergeCommit, matchFunc, cHashToRefs)
		}

		return ltf.NewDotDotLogTableFunctionRowIter(ctx, sqledb.DbData().Ddb, []*doltdb.Commit{commit}, secondCommit, matchFunc, cHashToRefs)

	}

	return ltf.NewLogTableFunctionRowIter(ctx, sqledb.DbData().Ddb, commit, matchFunc, cHashToRefs)
}

func getCommitHashToRefs(ctx *sql.Context, ddb *doltdb.DoltDB, decoration string) (map[hash.Hash][]string, error) {
	cHashToRefs := map[hash.Hash][]string{}

	// Get all branches
	branches, err := ddb.GetBranchesWithHashes(ctx)
	if err != nil {
		return nil, err
	}
	for _, b := range branches {
		refName := b.Ref.String()
		if decoration != "full" {
			refName = b.Ref.GetPath() // trim out "refs/heads/"
		}
		cHashToRefs[b.Hash] = append(cHashToRefs[b.Hash], refName)
	}

	// Get all remote branches
	remotes, err := ddb.GetRemotesWithHashes(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range remotes {
		refName := r.Ref.String()
		if decoration != "full" {
			refName = r.Ref.GetPath() // trim out "refs/remotes/"
		}
		cHashToRefs[r.Hash] = append(cHashToRefs[r.Hash], refName)
	}

	// Get all tags
	tags, err := ddb.GetTagsWithHashes(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range tags {
		tagName := t.Tag.GetDoltRef().String()
		if decoration != "full" {
			tagName = t.Tag.Name // trim out "refs/tags/"
		}
		tagName = fmt.Sprintf("tag: %s", tagName)
		cHashToRefs[t.Hash] = append(cHashToRefs[t.Hash], tagName)
	}

	return cHashToRefs, nil
}

// evaluateArguments returns revisionValStr, secondRevisionValStr, and three dot boolean.
// It evaluates the argument expressions to turn them into values this LogTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (ltf *LogTableFunction) evaluateArguments() (string, string, bool, error) {
	var revisionValStr string
	var secondRevisionValStr string
	var err error
	threeDot := false

	if ltf.revisionExpr != nil {
		revisionValStr, secondRevisionValStr, threeDot, err = getRevisionsFromExpr(ltf.ctx, ltf.revisionExpr, true)
		if err != nil {
			return "", "", false, err
		}
	}

	if ltf.secondRevisionExpr != nil {
		rvs, srvs, _, err := getRevisionsFromExpr(ltf.ctx, ltf.secondRevisionExpr, false)
		if err != nil {
			return "", "", false, err
		}
		if len(rvs) > 0 {
			revisionValStr = rvs
		}
		if len(srvs) > 0 {
			secondRevisionValStr = srvs
		}
	}

	if len(ltf.notRevision) > 0 {
		secondRevisionValStr = ltf.notRevision
	}

	return revisionValStr, secondRevisionValStr, threeDot, nil
}

func mustExpressionToString(ctx *sql.Context, expr sql.Expression) string {
	str, err := expressionToString(ctx, expr)
	if err != nil {
		return ""
	}
	return str
}

func expressionToString(ctx *sql.Context, expr sql.Expression) (string, error) {
	val, err := expr.Eval(ctx, nil)
	if err != nil {
		return "", err
	}

	valStr, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("received '%v' when expecting string", val)
	}

	return valStr, nil
}

// getRevisionsFromExpr returns the revisionName and/or secondRevisionName, as
// well as a threeDot boolean from sql expression
func getRevisionsFromExpr(ctx *sql.Context, expr sql.Expression, canDot bool) (string, string, bool, error) {
	revisionValStr, err := expressionToString(ctx, expr)
	if err != nil {
		return "", "", false, err
	}

	if canDot && strings.Contains(revisionValStr, "..") {
		if strings.Contains(revisionValStr, "...") {
			refs := strings.Split(revisionValStr, "...")
			return refs[0], refs[1], true, nil
		}
		refs := strings.Split(revisionValStr, "..")
		return refs[1], refs[0], false, nil
	}

	if strings.HasPrefix(revisionValStr, "^") {
		return "", strings.TrimPrefix(revisionValStr, "^"), false, nil
	}

	return revisionValStr, "", false, nil
}

//------------------------------------
// logTableFunctionRowIter
//------------------------------------

var _ sql.RowIter = (*logTableFunctionRowIter)(nil)

// logTableFunctionRowIter is a sql.RowIter implementation which iterates over each commit as if it's a row in the table.
type logTableFunctionRowIter struct {
	child       doltdb.CommitItr
	showParents bool
	decoration  string
	cHashToRefs map[hash.Hash][]string
	headHash    hash.Hash
}

func (ltf *LogTableFunction) NewLogTableFunctionRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, matchFn func(*doltdb.Commit) (bool, error), cHashToRefs map[hash.Hash][]string) (*logTableFunctionRowIter, error) {
	h, err := commit.HashOf()
	if err != nil {
		return nil, err
	}

	child, err := commitwalk.GetTopologicalOrderIterator(ctx, ddb, []hash.Hash{h}, matchFn)
	if err != nil {
		return nil, err
	}

	return &logTableFunctionRowIter{
		child:       child,
		showParents: ltf.showParents,
		decoration:  ltf.decoration,
		cHashToRefs: cHashToRefs,
		headHash:    h,
	}, nil
}

func (ltf *LogTableFunction) NewDotDotLogTableFunctionRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, commits []*doltdb.Commit, excludingCommit *doltdb.Commit, matchFn func(*doltdb.Commit) (bool, error), cHashToRefs map[hash.Hash][]string) (*logTableFunctionRowIter, error) {
	hashes := make([]hash.Hash, len(commits))

	for i, commit := range commits {
		h, err := commit.HashOf()
		if err != nil {
			return nil, err
		}
		hashes[i] = h
	}

	exHash, err := excludingCommit.HashOf()
	if err != nil {
		return nil, err
	}

	child, err := commitwalk.GetDotDotRevisionsIterator(ctx, ddb, hashes, ddb, []hash.Hash{exHash}, matchFn)
	if err != nil {
		return nil, err
	}

	var headHash hash.Hash

	if len(hashes) == 1 {
		headHash = hashes[0]
	}

	return &logTableFunctionRowIter{
		child:       child,
		showParents: ltf.showParents,
		decoration:  ltf.decoration,
		cHashToRefs: cHashToRefs,
		headHash:    headHash,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *logTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	h, cm, err := itr.child.Next(ctx)
	if err != nil {
		return nil, err
	}

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	row := sql.NewRow(h.String(), meta.Name, meta.Email, meta.Time(), meta.Description)

	if itr.showParents {
		prStr, err := getParentsString(ctx, cm)
		if err != nil {
			return nil, err
		}
		row = row.Append(sql.NewRow(prStr))
	}

	if shouldDecorateWithRefs(itr.decoration) {
		branchNames := itr.cHashToRefs[h]
		isHead := itr.headHash == h
		row = row.Append(sql.NewRow(getRefsString(branchNames, isHead)))
	}

	return row, nil
}

func (itr *logTableFunctionRowIter) Close(_ *sql.Context) error {
	return nil
}

func getRefsString(branchNames []string, isHead bool) string {
	if len(branchNames) == 0 {
		return ""
	}
	var refStr string
	if isHead {
		refStr += "HEAD -> "
	}
	refStr += strings.Join(branchNames, ", ")

	return refStr
}

func getParentsString(ctx *sql.Context, cm *doltdb.Commit) (string, error) {
	parents, err := cm.ParentHashes(ctx)
	if err != nil {
		return "", err
	}

	var prStr string
	for i, h := range parents {
		prStr += h.String()
		if i < len(parents)-1 {
			prStr += ", "
		}
	}

	return prStr, nil
}

// Default ("auto") for the dolt_log table function is "no"
func shouldDecorateWithRefs(decoration string) bool {
	return decoration == "full" || decoration == "short"
}
