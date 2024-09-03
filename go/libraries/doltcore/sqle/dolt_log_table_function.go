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
	"github.com/dolthub/dolt/go/libraries/utils/gpg"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
)

const logTableDefaultRowCount = 10

var _ sql.TableFunction = (*LogTableFunction)(nil)
var _ sql.ExecSourceRel = (*LogTableFunction)(nil)

type LogTableFunction struct {
	ctx *sql.Context

	revisionExprs    []sql.Expression
	notRevisionExprs []sql.Expression
	notRevisionStrs  []string
	tableNames       []string

	minParents    int
	showParents   bool
	showSignature bool
	decoration    string

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

	node, err := newInstance.evalArguments(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Database implements the sql.Databaser interface
func (ltf *LogTableFunction) Database() sql.Database {
	return ltf.database
}

func (ltf *LogTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(ltf.Schema())
	numRows, _, err := ltf.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (ltf *LogTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return logTableDefaultRowCount, false, nil
}

// WithDatabase implements the sql.Databaser interface
func (ltf *LogTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	nltf := *ltf
	nltf.database = database
	return &nltf, nil
}

// Name implements the sql.TableFunction interface
func (ltf *LogTableFunction) Name() string {
	return "dolt_log"
}

// Resolved implements the sql.Resolvable interface
func (ltf *LogTableFunction) Resolved() bool {
	for _, expr := range ltf.revisionExprs {
		return expr.Resolved()
	}
	return true
}

func (ltf *LogTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (ltf *LogTableFunction) String() string {
	return fmt.Sprintf("DOLT_LOG(%s)", ltf.getOptionsString())
}

func (ltf *LogTableFunction) getOptionsString() string {
	var options []string

	for _, expr := range ltf.revisionExprs {
		options = append(options, expr.String())
	}

	for _, expr := range ltf.notRevisionStrs {
		options = append(options, fmt.Sprintf("^%s", expr))
	}

	if ltf.minParents > 0 {
		options = append(options, fmt.Sprintf("--%s %d", cli.MinParentsFlag, ltf.minParents))
	}

	if ltf.showParents {
		options = append(options, fmt.Sprintf("--%s", cli.ParentsFlag))
	}

	if ltf.showSignature {
		options = append(options, fmt.Sprintf("--%s", cli.ShowSignatureFlag))
	}

	if len(ltf.decoration) > 0 && ltf.decoration != "auto" {
		options = append(options, fmt.Sprintf("--%s %s", cli.DecorateFlag, ltf.decoration))
	}

	if len(ltf.tableNames) > 0 {
		options = append(options, "--tables", strings.Join(ltf.tableNames, ","))
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
	if ltf.showSignature {
		logSchema = append(logSchema, &sql.Column{Name: "signature", Type: types.Text})
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
		subject := sql.PrivilegeCheckSubject{Database: ltf.database.Name(), Table: tblName}
		operations = append(operations, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
	}

	return opChecker.UserHasPrivileges(ctx, operations...)
}

// Expressions implements the sql.Expressioner interface.
func (ltf *LogTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{}
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

		text, _, err := types.Text.Convert(childVal)
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

	apr, err := cli.CreateLogArgParser(true).Parse(args)
	if err != nil {
		return sql.ErrInvalidArgumentDetails.New(ltf.Name(), err.Error())
	}

	if notRevisionStrs, ok := apr.GetValueList(cli.NotFlag); ok {
		ltf.notRevisionStrs = append(ltf.notRevisionStrs, notRevisionStrs...)
	}

	if tableNames, ok := apr.GetValueList(cli.TablesFlag); ok {
		ltf.tableNames = append(ltf.tableNames, tableNames...)
	}

	minParents := apr.GetIntOrDefault(cli.MinParentsFlag, 0)
	if apr.Contains(cli.MergesFlag) {
		minParents = 2
	}

	ltf.minParents = minParents
	ltf.showParents = apr.Contains(cli.ParentsFlag)
	ltf.showSignature = apr.Contains(cli.ShowSignatureFlag)

	decorateOption := apr.GetValueOrDefault(cli.DecorateFlag, "auto")
	switch decorateOption {
	case "short", "full", "auto", "no":
	default:
		return ltf.invalidArgDetailsErr(fmt.Sprintf("invalid --decorate option: %s", decorateOption))
	}
	ltf.decoration = decorateOption

	return nil
}

func (ltf *LogTableFunction) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(0, len(exprs))
	}
	return ltf, nil
}

// evalArguments converts the input expressions into string literals and
// formats them as function arguments.
func (ltf *LogTableFunction) evalArguments(expression ...sql.Expression) (sql.Node, error) {
	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(ltf.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(ltf.Name(), expr.String())
		}
	}

	newLtf := *ltf
	if err := newLtf.addOptions(expression); err != nil {
		return nil, err
	}

	// Gets revisions, excluding any flag-related expression
	for i, ex := range expression {
		if !strings.Contains(ex.String(), "--") && !(i > 0 && strings.Contains(expression[i-1].String(), "--")) {
			exStr := strings.ReplaceAll(ex.String(), "'", "")
			if strings.HasPrefix(exStr, "^") {
				newLtf.notRevisionExprs = append(newLtf.notRevisionExprs, ex)
			} else {
				newLtf.revisionExprs = append(newLtf.revisionExprs, ex)
			}
		}
	}

	if err := newLtf.validateRevisionExpressions(); err != nil {
		return nil, err
	}

	return &newLtf, nil
}

func (ltf *LogTableFunction) validateRevisionExpressions() error {
	// We must convert the expressions to strings before making string comparisons
	// For dolt_log('^main'), ltf.revisionExpr.String() = "'^main'"" and revisionStr = "^main"

	revisionStrs, err := mustExpressionsToString(ltf.ctx, ltf.revisionExprs)
	if err != nil {
		return err
	}
	notRevisionStrs, err := mustExpressionsToString(ltf.ctx, ltf.notRevisionExprs)
	if err != nil {
		return err
	}

	for i, revisionStr := range revisionStrs {
		if !types.IsText(ltf.revisionExprs[i].Type()) {
			return ltf.invalidArgDetailsErr(ltf.revisionExprs[i].String())
		}
		if strings.Contains(revisionStr, "..") && (len(revisionStrs) > 1 || ltf.notRevisionExprs != nil || ltf.notRevisionStrs != nil) {
			return ltf.invalidArgDetailsErr("revision cannot contain '..' or '...' if multiple revisions exist")
		}
	}

	for i, notRevisionStr := range notRevisionStrs {
		if !types.IsText(ltf.notRevisionExprs[i].Type()) {
			return ltf.invalidArgDetailsErr(ltf.notRevisionExprs[i].String())
		}
		if strings.Contains(notRevisionStr, "..") {
			return ltf.invalidArgDetailsErr("revision cannot contain both '..' or '...' and '^'")
		}
	}
	for _, notRevStr := range ltf.notRevisionStrs {
		if strings.Contains(notRevStr, "..") {
			return ltf.invalidArgDetailsErr("--not revision cannot contain '..'")
		}
		if strings.HasPrefix(notRevStr, "^") {
			return ltf.invalidArgDetailsErr("--not revision cannot contain '^'")
		}
	}

	return nil
}

// mustExpressionsToString converts a slice of expressions to a slice of resolved strings.
func mustExpressionsToString(ctx *sql.Context, expr []sql.Expression) ([]string, error) {
	var valStrs []string

	for _, ex := range expr {
		valStr, err := expressionToString(ctx, ex)
		if err != nil {
			return nil, err
		}

		valStrs = append(valStrs, valStr)
	}

	return valStrs, nil
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

func (ltf *LogTableFunction) invalidArgDetailsErr(reason string) *errors.Error {
	return sql.ErrInvalidArgumentDetails.New(ltf.Name(), reason)
}

// RowIter implements the sql.Node interface
func (ltf *LogTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	revisionValStrs, notRevisionValStrs, threeDot, err := ltf.evaluateArguments()
	if err != nil {
		return nil, err
	}
	notRevisionValStrs = append(notRevisionValStrs, ltf.notRevisionStrs...)

	sqledb, ok := ltf.database.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", ltf.database)
	}

	sess := dsess.DSessFromSess(ctx.Session)
	var commit *doltdb.Commit

	matchFunc := func(optCmt *doltdb.OptionalCommit) (bool, error) {
		commit, ok := optCmt.ToCommit()
		if !ok {
			return false, nil
		}

		return commit.NumParents() >= ltf.minParents, nil
	}

	cHashToRefs, err := getCommitHashToRefs(ctx, sqledb.DbData().Ddb, ltf.decoration)
	if err != nil {
		return nil, err
	}

	var commits []*doltdb.Commit
	if len(revisionValStrs) == 0 {
		// If no revisions given, use session head
		commit, err = sess.GetHeadCommit(ctx, sqledb.RevisionQualifiedName())
		if err != nil {
			return nil, err
		}
		commits = append(commits, commit)
	}

	dbName := sess.Session.GetCurrentDatabase()
	headRef, err := sess.CWBHeadRef(ctx, dbName)
	if err != nil {
		return nil, err
	}

	for _, revisionStr := range revisionValStrs {
		cs, err := doltdb.NewCommitSpec(revisionStr)
		if err != nil {
			return nil, err
		}

		optCmt, err := sqledb.DbData().Ddb.Resolve(ctx, cs, headRef)
		if err != nil {
			return nil, err
		}
		commit, ok = optCmt.ToCommit()
		if err != nil {
			return nil, doltdb.ErrGhostCommitEncountered
		}

		commits = append(commits, commit)
	}

	var notCommits []*doltdb.Commit
	for _, notRevisionStr := range notRevisionValStrs {
		cs, err := doltdb.NewCommitSpec(notRevisionStr)
		if err != nil {
			return nil, err
		}

		optCmt, err := sqledb.DbData().Ddb.Resolve(ctx, cs, headRef)
		if err != nil {
			return nil, err
		}
		notCommit, ok := optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitEncountered
		}

		notCommits = append(notCommits, notCommit)
	}

	if threeDot {
		mergeBase, err := merge.MergeBase(ctx, commits[0], commits[1])
		if err != nil {
			return nil, err
		}

		mergeCs, err := doltdb.NewCommitSpec(mergeBase.String())
		if err != nil {
			return nil, err
		}

		// Use merge base as excluding commit
		optCmt, err := sqledb.DbData().Ddb.Resolve(ctx, mergeCs, nil)
		if err != nil {
			return nil, err
		}
		mergeCommit, ok := optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitEncountered
		}

		notCommits = append(notCommits, mergeCommit)

		return ltf.NewDotDotLogTableFunctionRowIter(ctx, sqledb.DbData().Ddb, commits, notCommits, matchFunc, cHashToRefs, ltf.tableNames)
	}

	if len(revisionValStrs) <= 1 && len(notRevisionValStrs) == 0 {
		return ltf.NewLogTableFunctionRowIter(ctx, sqledb.DbData().Ddb, commits[0], matchFunc, cHashToRefs, ltf.tableNames)
	}

	return ltf.NewDotDotLogTableFunctionRowIter(ctx, sqledb.DbData().Ddb, commits, notCommits, matchFunc, cHashToRefs, ltf.tableNames)
}

// evaluateArguments returns revisionValStrs, notRevisionValStrs, and three dot boolean.
// It evaluates the argument expressions to turn them into values this LogTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (ltf *LogTableFunction) evaluateArguments() (revisionValStrs []string, notRevisionValStrs []string, threeDot bool, err error) {
	for _, expr := range ltf.revisionExprs {
		valStr, err := expressionToString(ltf.ctx, expr)
		if err != nil {
			return nil, nil, false, err
		}

		if strings.Contains(valStr, "..") {
			if strings.Contains(valStr, "...") {
				refs := strings.Split(valStr, "...")
				return refs, nil, true, nil
			}
			refs := strings.Split(valStr, "..")
			return []string{refs[1]}, []string{refs[0]}, false, nil
		}

		revisionValStrs = append(revisionValStrs, valStr)
	}

	for _, notExpr := range ltf.notRevisionExprs {
		notValStr, err := expressionToString(ltf.ctx, notExpr)
		if err != nil {
			return nil, nil, false, err
		}

		if strings.HasPrefix(notValStr, "^") {
			notValStr = strings.TrimPrefix(notValStr, "^")
		}

		notRevisionValStrs = append(notRevisionValStrs, notValStr)
	}

	return revisionValStrs, notRevisionValStrs, false, nil
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

//------------------------------------
// logTableFunctionRowIter
//------------------------------------

var _ sql.RowIter = (*logTableFunctionRowIter)(nil)

// logTableFunctionRowIter is a sql.RowIter implementation which iterates over each commit as if it's a row in the table.
type logTableFunctionRowIter struct {
	child         doltdb.CommitItr
	showParents   bool
	showSignature bool
	decoration    string
	cHashToRefs   map[hash.Hash][]string
	headHash      hash.Hash

	tableNames []string
}

func (ltf *LogTableFunction) NewLogTableFunctionRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, matchFn func(*doltdb.OptionalCommit) (bool, error), cHashToRefs map[hash.Hash][]string, tableNames []string) (*logTableFunctionRowIter, error) {
	h, err := commit.HashOf()
	if err != nil {
		return nil, err
	}

	child, err := commitwalk.GetTopologicalOrderIterator(ctx, ddb, []hash.Hash{h}, matchFn)
	if err != nil {
		return nil, err
	}

	return &logTableFunctionRowIter{
		child:         child,
		showParents:   ltf.showParents,
		showSignature: ltf.showSignature,
		decoration:    ltf.decoration,
		cHashToRefs:   cHashToRefs,
		headHash:      h,
		tableNames:    tableNames,
	}, nil
}

func (ltf *LogTableFunction) NewDotDotLogTableFunctionRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, commits []*doltdb.Commit, excludingCommits []*doltdb.Commit, matchFn func(*doltdb.OptionalCommit) (bool, error), cHashToRefs map[hash.Hash][]string, tableNames []string) (*logTableFunctionRowIter, error) {
	hashes := make([]hash.Hash, len(commits))
	for i, commit := range commits {
		h, err := commit.HashOf()
		if err != nil {
			return nil, err
		}
		hashes[i] = h
	}

	exHashes := make([]hash.Hash, len(excludingCommits))
	for i, exCommit := range excludingCommits {
		h, err := exCommit.HashOf()
		if err != nil {
			return nil, err
		}
		exHashes[i] = h
	}

	child, err := commitwalk.GetDotDotRevisionsIterator(ctx, ddb, hashes, ddb, exHashes, matchFn)
	if err != nil {
		return nil, err
	}

	var headHash hash.Hash

	if len(hashes) == 1 {
		headHash = hashes[0]
	}

	return &logTableFunctionRowIter{
		child:         child,
		showParents:   ltf.showParents,
		showSignature: ltf.showSignature,
		decoration:    ltf.decoration,
		cHashToRefs:   cHashToRefs,
		headHash:      headHash,
		tableNames:    tableNames,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *logTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	var commitHash hash.Hash
	var commit *doltdb.Commit
	var optCmt *doltdb.OptionalCommit
	var err error
	for {
		commitHash, optCmt, err = itr.child.Next(ctx)
		if err != nil {
			return nil, err
		}
		ok := false
		commit, ok = optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitEncountered
		}

		if itr.tableNames != nil {
			if commit.NumParents() == 0 {
				// if we're at the root commit, we continue without checking if any tables changed
				// we expect EOF to be returned on the next call to Next(), but continue in case there are more commits
				continue
			}
			optCmt, err := commit.GetParent(ctx, 0)
			if err != nil {
				return nil, err
			}
			parent0Cm, ok := optCmt.ToCommit()
			if !ok {
				return nil, doltdb.ErrGhostCommitEncountered
			}

			var parent1Cm *doltdb.Commit
			if commit.NumParents() > 1 {
				optCmt, err = commit.GetParent(ctx, 1)
				if err != nil {
					return nil, err
				}
				parent1Cm, ok = optCmt.ToCommit()
				if !ok {
					return nil, doltdb.ErrGhostCommitEncountered
				}
			}

			parent0RV, err := parent0Cm.GetRootValue(ctx)
			if err != nil {
				return nil, err
			}
			var parent1RV doltdb.RootValue
			if parent1Cm != nil {
				parent1RV, err = parent1Cm.GetRootValue(ctx)
				if err != nil {
					return nil, err
				}
			}
			childRV, err := commit.GetRootValue(ctx)
			if err != nil {
				return nil, err
			}

			didChange := false
			for _, tableName := range itr.tableNames {
				didChange, err = didTableChangeBetweenRootValues(ctx, childRV, parent0RV, parent1RV, tableName)
				if err != nil {
					return nil, err
				}
				if didChange {
					break
				}
			}

			if didChange {
				break
			}
		} else {
			break
		}
	}

	meta, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return nil, err
	}

	row := sql.NewRow(commitHash.String(), meta.Name, meta.Email, meta.Time(), meta.Description)

	if itr.showParents {
		prStr, err := getParentsString(ctx, commit)
		if err != nil {
			return nil, err
		}
		row = row.Append(sql.NewRow(prStr))
	}

	if shouldDecorateWithRefs(itr.decoration) {
		branchNames := itr.cHashToRefs[commitHash]
		isHead := itr.headHash == commitHash
		row = row.Append(sql.NewRow(getRefsString(branchNames, isHead)))
	}

	if itr.showSignature {
		if len(meta.Signature) > 0 {
			out, err := gpg.Verify(ctx, []byte(meta.Signature))
			if err != nil {
				return nil, err
			}

			row = row.Append(sql.NewRow(string(out)))
		} else {
			row = row.Append(sql.NewRow(""))
		}
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

// didTableChangeBetweenRootValues checks if the given table changed between the two given root values.
func didTableChangeBetweenRootValues(ctx *sql.Context, child, parent0, parent1 doltdb.RootValue, tableName string) (bool, error) {
	// TODO: schema
	childHash, childOk, err := child.GetTableHash(ctx, doltdb.TableName{Name: tableName})
	if err != nil {
		return false, err
	}
	parent0Hash, parent0Ok, err := parent0.GetTableHash(ctx, doltdb.TableName{Name: tableName})
	if err != nil {
		return false, err
	}
	var parent1Hash hash.Hash
	var parent1Ok bool
	if parent1 != nil {
		parent1Hash, parent1Ok, err = parent1.GetTableHash(ctx, doltdb.TableName{Name: tableName})
		if err != nil {
			return false, err
		}
	}

	if parent1 == nil {
		if !childOk && !parent0Ok {
			return false, nil
		} else if !childOk && parent0Ok {
			return true, nil
		} else if childOk && !parent0Ok {
			return true, nil
		} else {
			return childHash != parent0Hash, nil
		}
	} else {
		if !childOk && !parent0Ok && !parent1Ok {
			return false, nil
		} else if !childOk && parent0Ok && !parent1Ok {
			return true, nil
		} else if !childOk && !parent0Ok && parent1Ok {
			return true, nil
		} else if !childOk && parent0Ok && parent1Ok {
			return true, nil
		} else if childOk && !parent0Ok && !parent1Ok {
			return true, nil
		} else if childOk && !parent0Ok && parent1Ok {
			return childHash != parent1Hash, nil
		} else if childOk && parent0Ok && !parent1Ok {
			return childHash != parent0Hash, nil
		} else {
			return childHash != parent0Hash || childHash != parent1Hash, nil
		}
	}
}
