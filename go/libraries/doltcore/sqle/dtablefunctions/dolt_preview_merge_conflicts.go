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
	"encoding/base64"
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
	"github.com/zeebo/xxh3"
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
// This method is called lazily when the schema is first needed
func (pm *PreviewMergeConflictsTableFunction) generateSchema(ctx *sql.Context) error {
	if pm.sqlSch.Schema != nil {
		return nil // already generated
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
	// Get table references from all three roots
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

	// Check if table exists in at least one root
	if !ourOk && !theirOk && !baseOk {
		return nil, nil, nil, sql.ErrTableNotFound.New(tblName.String())
	}

	// Extract schemas from existing tables
	schemas, err := extractSchemas(ctx, ourTbl, ourOk, theirTbl, theirOk, baseTbl, baseOk)
	if err != nil {
		return nil, nil, nil, err
	}

	// Apply fallback logic for missing schemas
	return applySchemaFallbacks(schemas.our, schemas.their, schemas.base, ourOk, theirOk, baseOk)
}

// extractedSchemas holds the schemas extracted from tables
type extractedSchemas struct {
	our, their, base schema.Schema
}

// extractSchemas retrieves schemas from the provided tables
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

// applySchemaFallbacks applies fallback logic when schemas are missing
func applySchemaFallbacks(ourSch, theirSch, baseSch schema.Schema, ourOk, theirOk, baseOk bool) (schema.Schema, schema.Schema, schema.Schema, error) {
	// Apply fallback for missing "their" schema
	if !theirOk {
		if ourOk {
			theirSch = ourSch
		} else {
			theirSch = baseSch
		}
	}

	// Apply fallback for missing "our" schema
	if !ourOk {
		if theirOk {
			ourSch = theirSch
		} else {
			ourSch = baseSch
		}
	}

	// Handle case where table doesn't exist in ancestor
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
	// Ensure schema is generated before creating row iterator
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

	kd := pm.baseSch.GetKeyDescriptor(pm.rootInfo.baseRoot.NodeStore())
	baseVD := pm.baseSch.GetValueDescriptor(pm.rootInfo.baseRoot.NodeStore())
	oursVD := pm.ourSch.GetValueDescriptor(pm.rootInfo.leftRoot.NodeStore())
	theirsVD := pm.theirSch.GetValueDescriptor(pm.rootInfo.rightRoot.NodeStore())

	b := 1
	var o, t, n int
	if !keyless {
		o = b + kd.Count() + baseVD.Count()
		t = o + kd.Count() + oursVD.Count() + 1
		n = t + kd.Count() + theirsVD.Count() + 2
	} else {
		o = b + baseVD.Count() - 1
		t = o + oursVD.Count()
		n = t + theirsVD.Count() + 4
	}

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
		kd:           kd,
		baseVD:       baseVD,
		oursVD:       oursVD,
		theirsVD:     theirsVD,
		b:            b,
		o:            o,
		t:            t,
		n:            n,
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

	kd                       val.TupleDesc
	baseVD, oursVD, theirsVD val.TupleDesc
	// offsets for each version
	b, o, t int
	n       int

	baseHash, theirHash       hash.Hash
	baseRows, theirRows       prolly.Map
	baseRootish, theirRootish hash.Hash
}

func (itr *previewMergeConflictsTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if itr.itr == nil {
		return nil, io.EOF
	}

	for {
		r := make(sql.Row, itr.n)
		c, exists, err := itr.nextConflictVals(ctx)
		if err != nil {
			return nil, err
		}
		if !exists {
			// Continue to next iteration if conflict does not exist
			continue
		}

		r[0] = c.h.String()

		if !itr.keyless {
			for i := 0; i < itr.kd.Count(); i++ {
				f, err := tree.GetField(ctx, itr.kd, i, c.k, itr.baseRows.NodeStore())
				if err != nil {
					return nil, err
				}
				if c.bV != nil {
					r[itr.b+i] = f
				}
				if c.oV != nil {
					r[itr.o+i] = f
				}
				if c.tV != nil {
					r[itr.t+i] = f
				}
			}

			err = itr.putConflictRowVals(ctx, c, r)
			if err != nil {
				return nil, err
			}
		} else {
			err = itr.putKeylessConflictRowVals(ctx, c, r)
			if err != nil {
				return nil, err
			}
		}

		return r, nil
	}
}

type conf struct {
	k, bV, oV, tV val.Tuple
	h             hash.Hash
	id            string
}

func (itr *previewMergeConflictsTableFunctionRowIter) nextConflictVals(ctx *sql.Context) (c conf, exists bool, err error) {
	ca, err := itr.itr.Next(ctx)
	if err != nil {
		return conf{}, false, err
	}
	isConflict := ca.Op == tree.DiffOpDivergentModifyConflict || ca.Op == tree.DiffOpDivergentDeleteConflict
	isKeylessConflict := itr.keyless && (ca.Op == tree.DiffOpConvergentAdd || ca.Op == tree.DiffOpConvergentModify || ca.Op == tree.DiffOpConvergentDelete)
	if !isConflict && !isKeylessConflict {
		// If this is not a conflict, then we don't need to return anything.
		return conf{}, false, nil
	}

	c.k = ca.Key
	c.h = itr.theirRootish

	if len(ca.Key) == 0 {
		return conf{}, false, fmt.Errorf("empty key found in conflict")
	}
	// TODO: This is mutating the key, which is creating a schema with a different capacity than expected
	// b := xxh3.Hash128(append(ca.Key, c.h[:]...)).Bytes()
	// To ensure that the conflict id is unique, we hash both TheirRootIsh and the key of the table.
	buf := make([]byte, len(ca.Key)+len(c.h))
	copy(buf, ca.Key)
	copy(buf[len(ca.Key):], c.h[:])
	b := xxh3.Hash128(buf).Bytes()
	c.id = base64.RawStdEncoding.EncodeToString(b[:])

	err = itr.loadTableMaps(ctx, itr.baseRootish, itr.theirRootish)
	if err != nil {
		return conf{}, false, err
	}

	err = itr.baseRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
		c.bV = v
		return nil
	})
	if err != nil {
		return conf{}, false, err
	}
	err = itr.ourRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
		c.oV = v
		return nil
	})
	if err != nil {
		return conf{}, false, err
	}
	err = itr.theirRows.Get(ctx, ca.Key, func(_, v val.Tuple) error {
		c.tV = v
		return nil
	})
	if err != nil {
		return conf{}, false, err
	}

	return c, true, nil
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

func (itr *previewMergeConflictsTableFunctionRowIter) putConflictRowVals(ctx *sql.Context, c conf, r sql.Row) error {
	if c.bV != nil {
		for i := 0; i < itr.baseVD.Count(); i++ {
			f, err := tree.GetField(ctx, itr.baseVD, i, c.bV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.b+itr.kd.Count()+i] = f
		}
	}

	if c.oV != nil {
		for i := 0; i < itr.oursVD.Count(); i++ {
			f, err := tree.GetField(ctx, itr.oursVD, i, c.oV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.o+itr.kd.Count()+i] = f
		}
	}
	r[itr.o+itr.kd.Count()+itr.oursVD.Count()] = dtables.GetConflictDiffType(c.bV, c.oV)

	if c.tV != nil {
		for i := 0; i < itr.theirsVD.Count(); i++ {
			f, err := tree.GetField(ctx, itr.theirsVD, i, c.tV, itr.baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[itr.t+itr.kd.Count()+i] = f
		}
	}
	r[itr.t+itr.kd.Count()+itr.theirsVD.Count()] = dtables.GetConflictDiffType(c.bV, c.tV)
	r[itr.t+itr.kd.Count()+itr.theirsVD.Count()+1] = c.id

	return nil
}

func (itr *previewMergeConflictsTableFunctionRowIter) putKeylessConflictRowVals(ctx *sql.Context, c conf, r sql.Row) (err error) {
	ns := itr.baseRows.NodeStore()

	if c.bV != nil {
		// Cardinality
		r[itr.n-3], err = tree.GetField(ctx, itr.baseVD, 0, c.bV, ns)
		if err != nil {
			return err
		}

		for i := 0; i < itr.baseVD.Count()-1; i++ {
			f, err := tree.GetField(ctx, itr.baseVD, i+1, c.bV, ns)
			if err != nil {
				return err
			}
			r[itr.b+i] = f
		}
	} else {
		r[itr.n-3] = uint64(0)
	}

	if c.oV != nil {
		r[itr.n-2], err = tree.GetField(ctx, itr.oursVD, 0, c.oV, ns)
		if err != nil {
			return err
		}

		for i := 0; i < itr.oursVD.Count()-1; i++ {
			f, err := tree.GetField(ctx, itr.oursVD, i+1, c.oV, ns)
			if err != nil {
				return err
			}
			r[itr.o+i] = f
		}
	} else {
		r[itr.n-2] = uint64(0)
	}

	r[itr.o+itr.oursVD.Count()-1] = dtables.GetConflictDiffType(c.bV, c.oV)

	if c.tV != nil {
		r[itr.n-1], err = tree.GetField(ctx, itr.theirsVD, 0, c.tV, ns)
		if err != nil {
			return err
		}

		for i := 0; i < itr.theirsVD.Count()-1; i++ {
			f, err := tree.GetField(ctx, itr.theirsVD, i+1, c.tV, ns)
			if err != nil {
				return err
			}
			r[itr.t+i] = f
		}
	} else {
		r[itr.n-1] = uint64(0)
	}

	o := itr.t + itr.theirsVD.Count() - 1
	r[o] = dtables.GetConflictDiffType(c.bV, c.tV)
	r[itr.n-4] = c.id

	return nil
}

func (d *previewMergeConflictsTableFunctionRowIter) Close(context *sql.Context) error {
	if d.itr == nil {
		return nil
	}
	return d.itr.Close()
}
