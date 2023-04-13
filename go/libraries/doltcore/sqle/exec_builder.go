package sqle

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/rowexec"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
)

type DoltExecBuilder struct {
	rowexec.BaseBuilder
}

func NewDoltExecBuilder() sql.NodeExecBuilder {
	b := &DoltExecBuilder{rowexec.BaseBuilder{}}
	b.WithCustomSources(func(ctx *sql.Context, n sql.Node, r sql.Row) (sql.RowIter, error) {
		switch n := n.(type) {
		case *PatchTableFunction:
			return b.buildPatchTableFunction(ctx, n, r)
		case *LogTableFunction:
			return b.buildLogTableFunction(ctx, n, r)
		case *DiffTableFunction:
			return b.buildDiffTableFunction(ctx, n, r)
		case *DiffSummaryTableFunction:
			return b.buildDiffSummaryTableFunction(ctx, n, r)
		case *DiffStatTableFunction:
			return b.buildDiffStatTableFunction(ctx, n, r)
		default:
			return nil, nil
		}
	})
	return b
}

func (b *DoltExecBuilder) buildLogTableFunction(ctx *sql.Context, ltf *LogTableFunction, row sql.Row) (sql.RowIter, error) {
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

func (b *DoltExecBuilder) buildDiffTableFunction(ctx *sql.Context, dtf *DiffTableFunction, row sql.Row) (sql.RowIter, error) {
	// Everything we need to start iterating was cached when we previously determined the schema of the result
	// TODO: When we add support for joining on table functions, we'll need to evaluate this against the
	//       specified row. That row is what has the left_table context in a join query.
	//       This will expand the test cases we need to cover significantly.
	fromCommitVal, toCommitVal, dotCommitVal, _, err := dtf.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := dtf.database.(SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unable to get dolt database")
	}

	fromCommitStr, toCommitStr, err := loadCommitStrings(ctx, fromCommitVal, toCommitVal, dotCommitVal, sqledb)
	if err != nil {
		return nil, err
	}

	ddb := sqledb.DbData().Ddb
	dp := dtables.NewDiffPartition(dtf.tableDelta.ToTable, dtf.tableDelta.FromTable, toCommitStr, fromCommitStr, dtf.toDate, dtf.fromDate, dtf.tableDelta.ToSch, dtf.tableDelta.FromSch)

	return dtables.NewDiffPartitionRowIter(*dp, ddb, dtf.joiner), nil
}

func (b *DoltExecBuilder) buildDiffSummaryTableFunction(ctx *sql.Context, ds *DiffSummaryTableFunction, row sql.Row) (sql.RowIter, error) {
	fromCommitVal, toCommitVal, dotCommitVal, tableName, err := ds.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := ds.database.(SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", ds.database)
	}

	fromDetails, toDetails, err := loadDetailsForRefs(ctx, fromCommitVal, toCommitVal, dotCommitVal, sqledb)
	if err != nil {
		return nil, err
	}

	deltas, err := diff.GetTableDeltas(ctx, fromDetails.root, toDetails.root)
	if err != nil {
		return nil, err
	}

	sort.Slice(deltas, func(i, j int) bool {
		return strings.Compare(deltas[i].ToName, deltas[j].ToName) < 0
	})

	// If tableNameExpr defined, return a single table diff summary result
	if ds.tableNameExpr != nil {
		delta := findMatchingDelta(deltas, tableName)

		summ, err := getSummaryForDelta(ctx, delta, sqledb, fromDetails, toDetails, true)
		if err != nil {
			return nil, err
		}

		summs := []*diff.TableDeltaSummary{}
		if summ != nil {
			summs = []*diff.TableDeltaSummary{summ}
		}

		return NewDiffSummaryTableFunctionRowIter(summs), nil
	}

	var diffSummaries []*diff.TableDeltaSummary
	for _, delta := range deltas {
		summ, err := getSummaryForDelta(ctx, delta, sqledb, fromDetails, toDetails, false)
		if err != nil {
			return nil, err
		}
		if summ != nil {
			diffSummaries = append(diffSummaries, summ)
		}
	}

	return NewDiffSummaryTableFunctionRowIter(diffSummaries), nil
}

func (b *DoltExecBuilder) buildDiffStatTableFunction(ctx *sql.Context, ds *DiffStatTableFunction, row sql.Row) (sql.RowIter, error) {
	fromCommitVal, toCommitVal, dotCommitVal, tableName, err := ds.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := ds.database.(SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", ds.database)
	}

	fromRefDetails, toRefDetails, err := loadDetailsForRefs(ctx, fromCommitVal, toCommitVal, dotCommitVal, sqledb)
	if err != nil {
		return nil, err
	}

	deltas, err := diff.GetTableDeltas(ctx, fromRefDetails.root, toRefDetails.root)
	if err != nil {
		return nil, err
	}

	// If tableNameExpr defined, return a single table diff stat result
	if ds.tableNameExpr != nil {
		delta := findMatchingDelta(deltas, tableName)
		diffStat, hasDiff, err := getDiffStatNodeFromDelta(ctx, delta, fromRefDetails.root, toRefDetails.root, tableName)
		if err != nil {
			return nil, err
		}
		if !hasDiff {
			return NewDiffStatTableFunctionRowIter([]diffStatNode{}), nil
		}
		return NewDiffStatTableFunctionRowIter([]diffStatNode{diffStat}), nil
	}

	var diffStats []diffStatNode
	for _, delta := range deltas {
		tblName := delta.ToName
		if tblName == "" {
			tblName = delta.FromName
		}
		diffStat, hasDiff, err := getDiffStatNodeFromDelta(ctx, delta, fromRefDetails.root, toRefDetails.root, tblName)
		if err != nil {
			if errors.Is(err, diff.ErrPrimaryKeySetChanged) {
				ctx.Warn(dtables.PrimaryKeyChangeWarningCode, fmt.Sprintf("stat for table %s cannot be determined. Primary key set changed.", tblName))
				// Report an empty diff for tables that have primary key set changes
				diffStats = append(diffStats, diffStatNode{tblName: tblName})
				continue
			}
			return nil, err
		}
		if hasDiff {
			diffStats = append(diffStats, diffStat)
		}
	}

	return NewDiffStatTableFunctionRowIter(diffStats), nil
}

func (b *DoltExecBuilder) buildPatchTableFunction(ctx *sql.Context, p *PatchTableFunction, row sql.Row) (sql.RowIter, error) {
	fromCommitVal, toCommitVal, dotCommitVal, tableName, err := p.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := p.database.(SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unable to get dolt database")
	}

	fromRefDetails, toRefDetails, err := loadDetailsForRefs(ctx, fromCommitVal, toCommitVal, dotCommitVal, sqledb)
	if err != nil {
		return nil, err
	}

	tableDeltas, err := diff.GetTableDeltas(ctx, fromRefDetails.root, toRefDetails.root)
	if err != nil {
		return nil, err
	}

	sort.Slice(tableDeltas, func(i, j int) bool {
		return strings.Compare(tableDeltas[i].ToName, tableDeltas[j].ToName) < 0
	})

	// If tableNameExpr defined, return a single table patch result
	if p.tableNameExpr != nil {
		fromTblExists, err := fromRefDetails.root.HasTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		toTblExists, err := toRefDetails.root.HasTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if !fromTblExists && !toTblExists {
			return nil, sql.ErrTableNotFound.New(tableName)
		}

		delta := findMatchingDelta(tableDeltas, tableName)
		tableDeltas = []diff.TableDelta{delta}
	}

	patches, err := getPatchNodes(ctx, sqledb.DbData(), tableDeltas, fromRefDetails, toRefDetails)
	if err != nil {
		return nil, err
	}

	return newPatchTableFunctionRowIter(patches, fromRefDetails.hashStr, toRefDetails.hashStr), nil
}
