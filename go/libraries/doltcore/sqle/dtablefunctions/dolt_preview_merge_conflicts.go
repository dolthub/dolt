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
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	dtypes "github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ sql.TableFunction = (*PreviewMergeConflictsTableFunction)(nil)
var _ sql.ExecSourceRel = (*PreviewMergeConflictsTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*PreviewMergeConflictsTableFunction)(nil)

type PreviewMergeConflictsTableFunction struct {
	ctx             *sql.Context
	leftBranchExpr  sql.Expression
	rightBranchExpr sql.Expression
	tableNameExpr   sql.Expression
	database        sql.Database

	rootInfo                  rootInfo
	tblName                   doltdb.TableName
	sqlSch                    sql.PrimaryKeySchema
	baseSch, ourSch, theirSch schema.Schema
}

// NewInstance creates a new instance of TableFunction interface
func (pm *PreviewMergeConflictsTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &PreviewMergeConflictsTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (pm *PreviewMergeConflictsTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(pm.Schema())
	numRows, _, err := pm.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (pm *PreviewMergeConflictsTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return previewMergeConflictsDefaultRowCount, false, nil
}

// Database implements the sql.Databaser interface
func (pm *PreviewMergeConflictsTableFunction) Database() sql.Database {
	return pm.database
}

// WithDatabase implements the sql.Databaser interface
func (pm *PreviewMergeConflictsTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	npm := *pm
	npm.database = database
	return &npm, nil
}

// Name implements the sql.TableFunction interface
func (pm *PreviewMergeConflictsTableFunction) Name() string {
	return "dolt_preview_merge_conflicts"
}

// Resolved implements the sql.Resolvable interface
func (pm *PreviewMergeConflictsTableFunction) Resolved() bool {
	return pm.leftBranchExpr.Resolved() && pm.rightBranchExpr.Resolved() && pm.tableNameExpr.Resolved()
}

func (pm *PreviewMergeConflictsTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (pm *PreviewMergeConflictsTableFunction) String() string {
	return fmt.Sprintf("DOLT_PREVIEW_MERGE_CONFLICTS(%s, %s, %s)", pm.leftBranchExpr.String(), pm.rightBranchExpr.String(), pm.tableNameExpr.String())
}

// Schema implements the sql.Node interface.
func (pm *PreviewMergeConflictsTableFunction) Schema() sql.Schema {
	if !pm.Resolved() {
		return nil
	}
	// Lazy schema generation - generate schema on first access
	if pm.sqlSch.Schema == nil {
		err := pm.generateSchema(pm.ctx)
		if err != nil {
			// Schema generation failed, but we can't return an error from Schema()
			// This will surface the error when RowIter() is called
			return nil
		}
	}

	return pm.sqlSch.Schema
}

// Children implements the sql.Node interface.
func (pm *PreviewMergeConflictsTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (pm *PreviewMergeConflictsTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return pm, nil
}

// CheckAuth implements the interface sql.AuthorizationCheckerNode.
func (pm *PreviewMergeConflictsTableFunction) CheckAuth(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if !types.IsText(pm.tableNameExpr.Type()) {
		return ExpressionIsDeferred(pm.tableNameExpr)
	}

	tableNameVal, err := pm.tableNameExpr.Eval(pm.ctx, nil)
	if err != nil {
		return false
	}
	tableName, ok, err := sql.Unwrap[string](ctx, tableNameVal)
	if err != nil {
		return false
	}
	if !ok {
		return false
	}

	subject := sql.PrivilegeCheckSubject{Database: pm.database.Name(), Table: tableName}
	// TODO: Add tests for privilege checking
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
}

// Expressions implements the sql.Expressioner interface.
func (pm *PreviewMergeConflictsTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{pm.leftBranchExpr, pm.rightBranchExpr, pm.tableNameExpr}
}

// WithExpressions implements the sql.Expressioner interface.
func (pm *PreviewMergeConflictsTableFunction) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New(pm.Name(), "3", len(exprs))
	}

	for _, expr := range exprs {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(pm.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(pm.Name(), expr.String())
		}
	}

	newPmcs := *pm
	newPmcs.leftBranchExpr = exprs[0]
	newPmcs.rightBranchExpr = exprs[1]
	newPmcs.tableNameExpr = exprs[2]

	// validate the expressions
	if !types.IsText(newPmcs.leftBranchExpr.Type()) && !expression.IsBindVar(newPmcs.leftBranchExpr) {
		return nil, sql.ErrInvalidArgumentDetails.New(newPmcs.Name(), newPmcs.leftBranchExpr.String())
	}
	if !types.IsText(newPmcs.rightBranchExpr.Type()) && !expression.IsBindVar(newPmcs.rightBranchExpr) {
		return nil, sql.ErrInvalidArgumentDetails.New(newPmcs.Name(), newPmcs.rightBranchExpr.String())
	}
	if !types.IsText(newPmcs.tableNameExpr.Type()) && !expression.IsBindVar(newPmcs.tableNameExpr) {
		return nil, sql.ErrInvalidArgumentDetails.New(newPmcs.Name(), newPmcs.tableNameExpr.String())
	}

	return &newPmcs, nil
}

// generateSchema generates the schema if it hasn't been generated yet
func (pm *PreviewMergeConflictsTableFunction) generateSchema(ctx *sql.Context) error {
	if pm.sqlSch.Schema != nil {
		return nil
	}

	if !pm.Resolved() {
		return fmt.Errorf("table function not resolved")
	}

	sqledb, ok := pm.database.(dsess.SqlDatabase)
	if !ok {
		return fmt.Errorf("unexpected database type: %T", pm.database)
	}

	leftBranchVal, rightBranchVal, tableName, err := pm.evaluateArguments()
	if err != nil {
		return err
	}

	leftBranch, err := interfaceToString(leftBranchVal)
	if err != nil {
		return err
	}
	rightBranch, err := interfaceToString(rightBranchVal)
	if err != nil {
		return err
	}

	ri, err := resolveBranchesToRoots(ctx, sqledb, leftBranch, rightBranch)
	if err != nil {
		return err
	}

	tblName := doltdb.TableName{Name: tableName, Schema: doltdb.DefaultSchemaName}
	baseSch, ourSch, theirSch, err := getConflictSchemasFromRoots(ctx, tblName, ri.leftRoot, ri.rightRoot, ri.baseRoot)
	if err != nil {
		return err
	}

	confSch, _, err := dtables.CalculateConflictSchema(baseSch, ourSch, theirSch)
	if err != nil {
		return err
	}

	sqlSch, err := sqlutil.FromDoltSchema(sqledb.Name(), pm.Name(), confSch)
	if err != nil {
		return err
	}

	pm.sqlSch = sqlSch
	pm.rootInfo = ri
	pm.tblName = tblName
	pm.baseSch = baseSch
	pm.ourSch = ourSch
	pm.theirSch = theirSch

	return nil
}

func getConflictSchemasFromRoots(ctx *sql.Context, tblName doltdb.TableName, leftRoot, rightRoot, baseRoot doltdb.RootValue) (base, sch, mergeSch schema.Schema, err error) {
	ourTbl, ourOk, err := leftRoot.GetTable(ctx, tblName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get table from left root: %w", err)
	}

	baseTbl, baseOk, err := baseRoot.GetTable(ctx, tblName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get table from base root: %w", err)
	}

	theirTbl, theirOk, err := rightRoot.GetTable(ctx, tblName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get table from right root: %w", err)
	}

	if !ourOk && !theirOk && !baseOk {
		return nil, nil, nil, sql.ErrTableNotFound.New(tblName.String())
	}

	schemas, err := extractSchemas(ctx, ourTbl, ourOk, theirTbl, theirOk, baseTbl, baseOk)
	if err != nil {
		return nil, nil, nil, err
	}

	return applySchemaFallbacks(schemas.our, schemas.their, schemas.base, ourOk, theirOk, baseOk)
}

type extractedSchemas struct {
	our, their, base schema.Schema
}

func extractSchemas(ctx *sql.Context, ourTbl *doltdb.Table, ourOk bool, theirTbl *doltdb.Table, theirOk bool, baseTbl *doltdb.Table, baseOk bool) (*extractedSchemas, error) {
	schemas := &extractedSchemas{}
	var err error

	if ourOk {
		schemas.our, err = ourTbl.GetSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema from our table: %w", err)
		}
	}

	if theirOk {
		schemas.their, err = theirTbl.GetSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema from their table: %w", err)
		}
	}

	if baseOk {
		schemas.base, err = baseTbl.GetSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema from base table: %w", err)
		}
	}

	return schemas, nil
}

func applySchemaFallbacks(ourSch, theirSch, baseSch schema.Schema, ourOk, theirOk, baseOk bool) (schema.Schema, schema.Schema, schema.Schema, error) {
	if !theirOk {
		if ourOk {
			theirSch = ourSch
		} else {
			theirSch = baseSch
		}
	}

	if !ourOk {
		if theirOk {
			ourSch = theirSch
		} else {
			ourSch = baseSch
		}
	}

	if !baseOk {
		if schema.SchemasAreEqual(ourSch, theirSch) {
			return ourSch, ourSch, theirSch, nil
		}
		return nil, nil, nil, fmt.Errorf("expected our schema to equal their schema since the table did not exist in the ancestor")
	}

	return ourSch, theirSch, baseSch, nil
}

// RowIter implements the sql.Node interface
func (pm *PreviewMergeConflictsTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	err := pm.generateSchema(ctx)
	if err != nil {
		return nil, err
	}

	merger, err := merge.NewMerger(pm.rootInfo.leftRoot, pm.rootInfo.rightRoot, pm.rootInfo.baseRoot, pm.rootInfo.rightCm, pm.rootInfo.ancCm, pm.rootInfo.leftRoot.VRW(), pm.rootInfo.leftRoot.NodeStore())
	if err != nil {
		return nil, err
	}

	mergeOpts := merge.MergeOpts{
		IsCherryPick:           false,
		KeepSchemaConflicts:    true,
		ReverifyAllConstraints: false,
	}

	tm, err := merger.MakeTableMerger(ctx, pm.tblName, mergeOpts)
	if err != nil {
		return nil, err
	}

	// short-circuit here if we can
	finished, _, stats, err := merger.MaybeShortCircuit(ctx, tm, mergeOpts)
	if err != nil {
		return nil, err
	}
	if finished != nil || stats != nil {
		return &previewMergeConflictsTableFunctionRowIter{}, nil
	}
	// Calculate a merge of the schemas, but don't apply it
	mergeSch, schConflicts, _, diffInfo, err := tm.SchemaMerge(ctx, pm.tblName)
	if err != nil {
		return nil, err
	}
	if schConflicts.Count() > 0 {
		// Cannot calculate data conflicts if there are schema conflicts
		return nil, fmt.Errorf("schema conflicts found: %d", schConflicts.Count())
	}

	if !tm.InvolvesRootObjects() {
		if !dtypes.IsFormat_DOLT(pm.rootInfo.leftRoot.VRW().Format()) {
			return nil, fmt.Errorf("preview_merge_conflicts table function only supports dolt format")
		}
	} else {
		return nil, fmt.Errorf("Dolt does not operate on root objects")
	}

	keyless := schema.IsKeyless(mergeSch)

	leftRows, err := tm.LeftRows(ctx)
	if err != nil {
		return nil, err
	}
	rightRows, err := tm.RightRows(ctx)
	if err != nil {
		return nil, err
	}
	ancRows, err := tm.AncRows(ctx)
	if err != nil {
		return nil, err
	}

	rightHash, err := pm.rootInfo.rightCm.HashOf()
	if err != nil {
		return nil, err
	}

	baseHash, err := pm.rootInfo.ancCm.HashOf()
	if err != nil {
		return nil, err
	}

	vds := dtables.GetConflictValueDescriptors(pm.baseSch, pm.ourSch, pm.theirSch, pm.rootInfo.baseRoot.NodeStore())
	offsets := dtables.GetConflictOffsets(keyless, vds)

	valueMerger := tm.GetNewValueMerger(mergeSch, leftRows)

	differ, err := tree.NewThreeWayDiffer(
		ctx,
		leftRows.NodeStore(),
		leftRows.Tuples(),
		rightRows.Tuples(),
		ancRows.Tuples(),
		valueMerger.TryMerge,
		keyless,
		diffInfo,
		leftRows.Tuples().Order,
	)
	if err != nil {
		return nil, err
	}

	return &previewMergeConflictsTableFunctionRowIter{
		itr:          differ,
		tblName:      pm.tblName,
		vrw:          pm.rootInfo.leftRoot.VRW(),
		ns:           leftRows.NodeStore(),
		ourRows:      leftRows,
		keyless:      keyless,
		ourSch:       pm.ourSch,
		offsets:      offsets,
		vds:          vds,
		baseRootish:  baseHash,
		theirRootish: rightHash,
	}, nil
}

// evaluateArguments returns leftBranchVal amd rightBranchVal.
// It evaluates the argument expressions to turn them into values this PreviewMergeConflictsTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (pm *PreviewMergeConflictsTableFunction) evaluateArguments() (interface{}, interface{}, string, error) {
	leftBranchVal, err := pm.leftBranchExpr.Eval(pm.ctx, nil)
	if err != nil {
		return nil, nil, "", err
	}

	rightBranchVal, err := pm.rightBranchExpr.Eval(pm.ctx, nil)
	if err != nil {
		return nil, nil, "", err
	}

	tableNameVal, err := pm.tableNameExpr.Eval(pm.ctx, nil)
	if err != nil {
		return nil, nil, "", err
	}

	tableName, ok := tableNameVal.(string)
	if !ok {
		return nil, nil, "", ErrInvalidTableName.New(pm.tableNameExpr.String())
	}

	if tableName == "" {
		return nil, nil, "", fmt.Errorf("table name cannot be empty")
	}

	return leftBranchVal, rightBranchVal, tableName, nil
}

//--------------------------------------------------
// previewMergeConflictsTableFunctionRowIter
//--------------------------------------------------

var _ sql.RowIter = &previewMergeConflictsTableFunctionRowIter{}

type previewMergeConflictsTableFunctionRowIter struct {
	itr     *tree.ThreeWayDiffer[val.Tuple, val.TupleDesc]
	tblName doltdb.TableName
	vrw     dtypes.ValueReadWriter
	ns      tree.NodeStore
	ourRows prolly.Map
	keyless bool
	ourSch  schema.Schema

	vds     dtables.ConflictValueDescriptors
	offsets dtables.ConflictOffsets

	baseHash, theirHash       hash.Hash
	baseRows, theirRows       prolly.Map
	baseRootish, theirRootish hash.Hash
}

func (itr *previewMergeConflictsTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if itr.itr == nil {
		return nil, io.EOF
	}

	row := make(sql.Row, itr.offsets.N)
	confVal, err := itr.nextConflictVals(ctx)
	if err != nil {
		return nil, err
	}

	row[0] = confVal.Hash.String()

	if !itr.keyless {
		err = itr.putConflictRowVals(ctx, confVal, row)
		if err != nil {
			return nil, err
		}
	} else {
		err = itr.putKeylessConflictRowVals(ctx, confVal, row)
		if err != nil {
			return nil, err
		}
	}

	return row, nil
}

func (itr *previewMergeConflictsTableFunctionRowIter) nextConflictVals(ctx *sql.Context) (confVal dtables.ConflictVal, err error) {
	for {
		ca, err := itr.itr.Next(ctx)
		if err != nil {
			return dtables.ConflictVal{}, err
		}
		isConflict := ca.Op == tree.DiffOpDivergentModifyConflict || ca.Op == tree.DiffOpDivergentDeleteConflict
		isKeylessConflict := itr.keyless && (ca.Op == tree.DiffOpConvergentAdd || ca.Op == tree.DiffOpConvergentModify || ca.Op == tree.DiffOpConvergentDelete)
		if !isConflict && !isKeylessConflict {
			// If this is not a conflict, continue to next iteration
			continue
		}

		confVal.Key = ca.Key
		confVal.Hash = itr.theirRootish

		buf := make([]byte, 0, len(ca.Key)+len(confVal.Hash))
		buf = append(buf, ca.Key...)
		confVal.Id = dtables.GetConflictId(buf, confVal.Hash)

		err = itr.loadTableMaps(ctx, itr.baseRootish, itr.theirRootish)
		if err != nil {
			return dtables.ConflictVal{}, err
		}

		err = itr.baseRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
			confVal.Base = v
			return nil
		})
		if err != nil {
			return dtables.ConflictVal{}, err
		}
		err = itr.ourRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
			confVal.Ours = v
			return nil
		})
		if err != nil {
			return dtables.ConflictVal{}, err
		}
		err = itr.theirRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
			confVal.Theirs = v
			return nil
		})
		if err != nil {
			return dtables.ConflictVal{}, err
		}

		return confVal, nil
	}
}

// loadTableMaps loads the maps specified in the metadata if they are different from
// the currently loaded maps. |baseHash| and |theirHash| are table hashes.
func (itr *previewMergeConflictsTableFunctionRowIter) loadTableMaps(ctx *sql.Context, baseHash, theirHash hash.Hash) error {
	if itr.baseHash.Compare(baseHash) != 0 {
		rv, err := doltdb.LoadRootValueFromRootIshAddr(ctx, itr.vrw, itr.ns, baseHash)
		if err != nil {
			return err
		}
		baseTbl, ok, err := rv.GetTable(ctx, itr.tblName)
		if err != nil {
			return err
		}

		var idx durable.Index
		if !ok {
			idx, err = durable.NewEmptyPrimaryIndex(ctx, itr.vrw, itr.ns, itr.ourSch)
		} else {
			idx, err = baseTbl.GetRowData(ctx)
		}

		if err != nil {
			return err
		}

		itr.baseRows, err = durable.ProllyMapFromIndex(idx)
		if err != nil {
			return err
		}

		itr.baseHash = baseHash
	}

	if itr.theirHash.Compare(theirHash) != 0 {
		rv, err := doltdb.LoadRootValueFromRootIshAddr(ctx, itr.vrw, itr.ns, theirHash)
		if err != nil {
			return err
		}

		theirTbl, ok, err := rv.GetTable(ctx, itr.tblName)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("failed to find table %s in right root value", itr.tblName)
		}

		idx, err := theirTbl.GetRowData(ctx)
		if err != nil {
			return err
		}
		itr.theirRows, err = durable.ProllyMapFromIndex(idx)
		if err != nil {
			return err
		}
		itr.theirHash = theirHash
	}

	return nil
}

func (itr *previewMergeConflictsTableFunctionRowIter) putConflictRowVals(ctx *sql.Context, confVal dtables.ConflictVal, row sql.Row) error {
	ns := itr.baseRows.NodeStore()
	return dtables.PutConflictRowVals(ctx, confVal, row, itr.offsets, itr.vds, ns)
}

func (itr *previewMergeConflictsTableFunctionRowIter) putKeylessConflictRowVals(ctx *sql.Context, confVal dtables.ConflictVal, row sql.Row) (err error) {
	ns := itr.baseRows.NodeStore()
	return dtables.PutKeylessConflictRowVals(ctx, confVal, row, itr.offsets, itr.vds, ns)
}

func (d *previewMergeConflictsTableFunctionRowIter) Close(context *sql.Context) error {
	if d.itr == nil {
		return nil
	}
	return d.itr.Close()
}
