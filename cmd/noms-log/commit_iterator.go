// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
)

type CommitIterator struct {
	db       datas.Database
	branches branchList
}

// Initialize a new CommitIterator with the first commit to be printed.
func NewCommitIterator(db datas.Database, commit types.Struct) *CommitIterator {
	cr := types.NewRef(commit)
	return &CommitIterator{db: db, branches: branchList{branch{cr: cr, commit: commit}}}
}

// Returns information about the next commit to be printed. LogNode contains enough contextual
// info that the commit and associated graph can be correctly printed.
// This works by traversing the "commit" di-graph in a breadth-first manner. Each time it is called,
// the commit in the branchlist with the greatest height is returned. If that commit has multiple
// parents, new branches are added to the branchlist so that they can be traversed in order. When
// more than one branch contains the same node, that indicates that the branches are converging and so
// the branchlist will have branches removed to reflect that.
func (iter *CommitIterator) Next() (LogNode, bool) {
	if iter.branches.IsEmpty() {
		return LogNode{}, false
	}

	// Number of branches present when printing this commit
	startingColCount := len(iter.branches)

	branchIndexes := iter.branches.HighestBranchIndexes()
	col := branchIndexes[0]
	br := iter.branches[col]

	// Any additional indexes, represent other branches with the same ancestor. So they are merging
	// into a common ancestor and are no longer graphed.
	iter.branches = iter.branches.RemoveBranches(branchIndexes[1:])

	// If this commit has parents, then a branch is splitting. Create a branch for each of the parents
	// and splice that into the iterators list of branches.
	branches := branchList{}
	parents := commitRefsFromSet(br.commit.Get(datas.ParentsField).(types.Set))
	for _, p := range parents {
		b := branch{cr: p, commit: iter.db.ReadValue(p.TargetHash()).(types.Struct)}
		branches = append(branches, b)
	}
	iter.branches = iter.branches.Splice(col, 1, branches...)

	// Collect the indexes for any newly created branches.
	newCols := []int{}
	for cnt := 1; cnt < len(parents); cnt++ {
		newCols = append(newCols, col+cnt)
	}

	// Now that the branchlist has been adusted, check to see if there are branches with common
	// ancestors that will be folded together on this commit's graph.
	foldedCols := iter.branches.HighestBranchIndexes()
	node := LogNode{
		cr:               br.cr,
		commit:           br.commit,
		startingColCount: startingColCount,
		endingColCount:   len(iter.branches),
		col:              col,
		newCols:          newCols,
		foldedCols:       foldedCols,
		lastCommit:       iter.branches.IsEmpty(),
	}
	return node, true
}

type LogNode struct {
	cr               types.Ref    // typed ref of commit to be printed
	commit           types.Struct // commit that needs to be printed
	startingColCount int          // how many branches are being tracked when this commit is printed
	endingColCount   int          // home many branches will be tracked when next commit is printed
	col              int          // col to put the '*' character in graph
	newCols          []int        // col to start using '\' in graph
	foldedCols       []int        // cols with common ancestors, that will get folded together
	lastCommit       bool         // this is the last commit that will be returned by iterator
}

func (n LogNode) String() string {
	return fmt.Sprintf("cr: %s(%d), startingColCount: %d, endingColCount: %d, col: %d, newCols: %v, foldedCols: %v, expanding: %t, shrunk: %t, shrinking: %t", n.cr.TargetHash().String()[0:9], n.cr.Height(), n.startingColCount, n.endingColCount, n.col, n.newCols, n.foldedCols, n.Expanding(), n.Shrunk(), n.Shrinking())
}

// True if this commit's graph will expand to show an additional branch
func (n LogNode) Expanding() bool {
	return n.startingColCount < n.endingColCount
}

// True if this commit's graph will show a branch being folded into another branch
func (n LogNode) Shrinking() bool {
	return len(n.foldedCols) > 1
}

// True if the previous commit showed a branch being folded into another branch.
func (n LogNode) Shrunk() bool {
	return n.startingColCount > n.endingColCount
}

type branch struct {
	cr     types.Ref
	commit types.Struct
}

func (b branch) String() string {
	return fmt.Sprintf("%s(%d)", b.cr.TargetHash().String()[0:9], b.cr.Height())
}

type branchList []branch

func (bl branchList) IsEmpty() bool {
	return len(bl) == 0
}

// look through this list of branches and return the one(s) with the max height.
// If there are multiple nodes with max height, the result will contain a list of all nodes with
// maxHeight that are duplicates of the first one found.
// This indicates that two or more branches or converging.
func (bl branchList) HighestBranchIndexes() []int {
	maxHeight := uint64(0)
	var cr types.Ref
	cols := []int{}
	for i, b := range bl {
		if b.cr.Height() > maxHeight {
			maxHeight = b.cr.Height()
			cr = b.cr
			cols = []int{i}
		} else if b.cr.Height() == maxHeight && b.cr.Equals(cr) {
			cols = append(cols, i)
		}
	}
	return cols
}

func (bl branchList) Splice(start int, deleteCount int, branches ...branch) branchList {
	res := append(branchList{}, bl[:start]...)
	res = append(res, branches...)
	return append(res, bl[start+deleteCount:]...)
}

func (bl branchList) RemoveBranches(indexes []int) branchList {
	for i := len(indexes) - 1; i >= 0; i-- {
		bl = bl.Splice(indexes[i], 1)
	}
	return bl
}

func commitRefsFromSet(set types.Set) []types.Ref {
	res := []types.Ref{}
	set.IterAll(func(v types.Value) {
		res = append(res, v.(types.Ref))
	})
	return res
}
