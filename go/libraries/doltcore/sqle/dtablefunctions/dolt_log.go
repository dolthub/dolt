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

package dtablefunctions

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/gpg"
	"github.com/dolthub/dolt/go/store/hash"
)

const logTableDefaultRowCount = 10

var _ sql.TableFunction = (*LogTableFunction)(nil)
var _ sql.ExecSourceRel = (*LogTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*LogTableFunction)(nil)

type LogTableFunction struct {
	ctx *sql.Context

	revisionStrs    []string
	notRevisionStrs []string
	tableNames      []string

	minParents    int
	showParents   bool
	showSignature bool
	decoration    string

	database sql.Database

	// argumentExprs stores the original expressions for deferred parsing
	argumentExprs []sql.Expression
}

var logTableSchema = sql.Schema{
	&sql.Column{Name: "commit_hash", Type: types.Text},
	&sql.Column{Name: "committer", Type: types.Text},
	&sql.Column{Name: "email", Type: types.Text},
	&sql.Column{Name: "date", Type: types.Datetime},
	&sql.Column{Name: "message", Type: types.Text},
	&sql.Column{Name: "commit_order", Type: types.Uint64},
}

// NewInstance creates a new instance of TableFunction interface
func (ltf *LogTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &LogTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.deferExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Database implements the sql.Databaser interface
func (ltf *LogTableFunction) Database() sql.Database {
	return ltf.database
}

// DataLength estimates total data size for query planning.
func (ltf *LogTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(ltf.Schema())
	numRows, _, err := ltf.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

// RowCount returns estimated row count for query planning.
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
	// In the new string-based approach, we're resolved when we have argument expressions
	// (which get resolved during normal SQL analysis) or when we have parsed strings
	for _, expr := range ltf.argumentExprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

// IsReadOnly returns true since log operations don't modify data.
func (ltf *LogTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (ltf *LogTableFunction) String() string {
	return fmt.Sprintf("DOLT_LOG(%s)", ltf.getOptionsString())
}

// getOptionsString builds comma-separated argument list for display.
func (ltf *LogTableFunction) getOptionsString() string {
	var options []string

	for _, revStr := range ltf.revisionStrs {
		options = append(options, revStr)
	}

	for _, notRevStr := range ltf.notRevisionStrs {
		options = append(options, fmt.Sprintf("^%s", notRevStr))
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

// CheckAuth implements the interface sql.AuthorizationCheckerNode.
func (ltf *LogTableFunction) CheckAuth(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
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
	// always return the original argument expressions from deferred parsing
	return ltf.argumentExprs
}

// getDoltArgs builds an argument string from sql expressions so that we can
// later parse the arguments with the same util as the CLI
func getDoltArgs(ctx *sql.Context, expressions []sql.Expression, name string) ([]string, error) {
	var args []string

	for _, expr := range expressions {
		// Skip bind variables during analysis phase (can't evaluate yet)
		// During execution phase, bind variables are resolved to literals by SQL engine
		if expression.IsBindVar(expr) {
			continue
		}

		childVal, err := expr.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}

		if !types.IsText(expr.Type()) {
			return args, sql.ErrInvalidArgumentDetails.New(name, expr.String())
		}

		text, _, err := types.Text.Convert(ctx, childVal)
		if err != nil {
			return nil, err
		}

		if text != nil {
			args = append(args, text.(string))
		}
	}

	return args, nil
}

// addOptions modifies struct state (revisionStrs, notRevisionStrs, showParents, etc.) by parsing expressions.
func (ltf *LogTableFunction) addOptions(expressions []sql.Expression) error {
	args, err := getDoltArgs(ltf.ctx, expressions, ltf.Name())
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

	// store revision strs directly from cli parse instead of mapping back exprs
	// avoid circular conv expr -> str -> expr, downstream
	for _, revisionStr := range apr.Args {
		if strings.HasPrefix(revisionStr, "^") {
			revisionStr = strings.TrimPrefix(revisionStr, "^")
			ltf.notRevisionStrs = append(ltf.notRevisionStrs, revisionStr)
		} else {
			ltf.revisionStrs = append(ltf.revisionStrs, revisionStr)
		}
	}

	// validate revision specifications for semantic errors
	return ltf.validateRevisionStrings()
}

// WithExpressions returns copy with expressions stored and revision strings cleared.
func (ltf *LogTableFunction) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	newLtf := *ltf
	newLtf.argumentExprs = exprs
	newLtf.revisionStrs = nil
	newLtf.notRevisionStrs = nil

	return &newLtf, nil
}

// deferExpressions stores the input expressions for later evaluation during execution.
// This table function violates SQL analyzer principles by evaluating expressions
// during analysis. This is necessary because the schema changes based on what
// arguments are supplied (e.g., --parent), and the schema needs to be known
// during analysis time. Bind variables are skipped over during the initial analysis
// of the prepared statement, and get fully resolved when they are bound when the
// prepared statement is later executed.
func (ltf *LogTableFunction) deferExpressions(expressions ...sql.Expression) (sql.Node, error) {
	bindVarsExist := false
	for _, expr := range expressions {
		// functions are not allowed as arguments
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(ltf.Name(), expr.String())
		}
		// also check for UnresolvedFunction which might not implement FunctionExpression
		if _, ok := expr.(*expression.UnresolvedFunction); ok {
			return nil, ErrInvalidNonLiteralArgument.New(ltf.Name(), expr.String())
		}
		if expression.IsBindVar(expr) {
			bindVarsExist = true
		}
	}

	node, _ := ltf.WithExpressions(expressions...)
	newLtf := *node.(*LogTableFunction)

	// Parse literal arguments for schema determination during analysis phase
	// getDoltArgs will skip bind variables (can't evaluate them yet)
	// only return errors if no bind variables exist (incomplete args are expected with bind vars)
	// TODO: schema-affecting flags as bind variables don't add columns to schema
	// this may be a common problem for dynamic table functions that need execution-time schema changes
	if err := newLtf.addOptions(newLtf.argumentExprs); err != nil && !bindVarsExist {
		return nil, err
	}

	return &newLtf, nil
}

// validateRevisionStrings checks the revision strings for semantic errors.
func (ltf *LogTableFunction) validateRevisionStrings() error {
	// validate revision specifications for semantic errors
	// this works with the parsed string values from CLI parser
	// no type validation needed here since getDoltArgs already validates expression types

	for _, revisionStr := range ltf.revisionStrs {
		if strings.Contains(revisionStr, "..") && (len(ltf.revisionStrs) > 1 || len(ltf.notRevisionStrs) > 0) {
			return ltf.invalidArgDetailsErr("revision cannot contain '..' or '...' if multiple revisions exist")
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

// expressionsToString converts a slice of expressions to a slice of resolved strings using Eval.
func expressionsToString(ctx *sql.Context, expr []sql.Expression) ([]string, error) {
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

// expressionToString uses the result of Eval to convert an expression to a string.
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

// invalidArgDetailsErr creates an error with the given reason for invalid arguments.
func (ltf *LogTableFunction) invalidArgDetailsErr(reason string) *errors.Error {
	return sql.ErrInvalidArgumentDetails.New(ltf.Name(), reason)
}

// RowIter implements the sql.Node interface
func (ltf *LogTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// Parse args again during execution phase to handle bind variables
	// At this point, bind variables are resolved to actual values by SQL engine
	if ltf.argumentExprs != nil {
		if err := ltf.addOptions(ltf.argumentExprs); err != nil {
			return nil, err
		}
	}

	revisionValStrs, notRevisionValStrs, threeDot := ltf.evaluateArguments()
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

// evaluateArguments handles range syntax for revisions and returns processed strings.
func (ltf *LogTableFunction) evaluateArguments() (revisionValStrs []string, notRevisionValStrs []string, threeDot bool) {
	for _, revisionStr := range ltf.revisionStrs {
		if strings.Contains(revisionStr, "...") {
			refs := strings.Split(revisionStr, "...")
			return refs, nil, true
		}

		if strings.Contains(revisionStr, "..") {
			refs := strings.Split(revisionStr, "..")
			return []string{refs[1]}, []string{refs[0]}, false
		}

		revisionValStrs = append(revisionValStrs, revisionStr)
	}

	notRevisionValStrs = append(notRevisionValStrs, ltf.notRevisionStrs...)

	return revisionValStrs, notRevisionValStrs, false
}

// getCommitHashToRefs builds map of commit hashes to branch/tag names for decoration.
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
	child         doltdb.CommitItr[*sql.Context]
	showParents   bool
	showSignature bool
	decoration    string
	cHashToRefs   map[hash.Hash][]string
	headHash      hash.Hash

	tableNames []string
}

// NewLogTableFunctionRowIter creates iterator for single commit history traversal.
func (ltf *LogTableFunction) NewLogTableFunctionRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, matchFn func(*doltdb.OptionalCommit) (bool, error), cHashToRefs map[hash.Hash][]string, tableNames []string) (*logTableFunctionRowIter, error) {
	h, err := commit.HashOf()
	if err != nil {
		return nil, err
	}

	child, err := commitwalk.GetTopologicalOrderIterator[*sql.Context](ctx, ddb, []hash.Hash{h}, matchFn)
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

// NewDotDotLogTableFunctionRowIter creates iterator for range queries with inclusion/exclusion commits.
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

	child, err := commitwalk.GetDotDotRevisionsIterator[*sql.Context](ctx, ddb, hashes, ddb, exHashes, matchFn)
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

	height, err := commit.Height()
	if err != nil {
		return nil, err
	}

	row := sql.NewRow(commitHash.String(), meta.Name, meta.Email, meta.Time(), meta.Description, height)

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

// Close releases any resources held by the iterator.
func (itr *logTableFunctionRowIter) Close(_ *sql.Context) error {
	return nil
}

// getRefsString formats branch names into display string with parentheses.
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

// getParentsString returns space-separated parent commit hashes.
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
// shouldDecorateWithRefs returns true if decoration setting enables ref display.
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
