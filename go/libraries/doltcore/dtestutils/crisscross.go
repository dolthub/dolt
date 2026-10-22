// Copyright 2026 Dolthub, Inc.
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

package dtestutils

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
)

// CrissCross is a history in which branches x and y each merge the other's first commit, so that they have two merge
// bases, x1 and y1:
//
//	c1 -- x1 [-- xm] -- x2 [-- x3] [-- xlater]    x
//	  \-- y1 [-- ym] -- y2 [-- y3] [-- ylater]    y
//	x2 = merge(x1 or xm, y1), y2 = merge(y1 or ym, x1)
//	x3 = merge(x2, y2), y3 = merge(y2, x2), when Nested
//
// Each list of statements runs on its branch before the commit of the same name. Merges do not commit by themselves,
// so that X2, Y2, X3 and Y3 can resolve them whether they conflict or not.
type CrissCross struct {
	// Seed makes c1.
	Seed []string
	// X1 and Y1 make x1 and y1.
	X1, Y1 []string
	// XM and YM, when set, make commits xm and ym before x and y merge the other's first commit, such as to change a
	// schema to match before merging.
	XM, YM []string
	// X2 and Y2 resolve the merges that make x2 and y2.
	X2, Y2 []string
	// Nested has x and y criss-cross again, with X3 and Y3 resolving the merges that make x3 and y3.
	Nested bool
	X3, Y3 []string
	// XLater and YLater, when set, make a last commit on x and y.
	XLater, YLater []string
}

// Script returns the SQL statements that build the history, ending on branch x. Each commit gets a branch of its
// name.
func (c CrissCross) Script() []string {
	s := append([]string{"SET @@autocommit = 0;"}, c.Seed...)
	s = append(s, commit("c1")...)
	s = append(s, "CALL DOLT_BRANCH('x');", "CALL DOLT_BRANCH('y');")
	s = append(s, onBranch("x", c.X1, "x1")...)
	s = append(s, onBranch("y", c.Y1, "y1")...)
	if c.YM != nil {
		s = append(s, onBranch("y", c.YM, "ym")...)
	}
	s = append(s, mergeOnBranch("y", "x1", c.Y2, "y2")...)
	if c.XM != nil {
		s = append(s, onBranch("x", c.XM, "xm")...)
	}
	s = append(s, mergeOnBranch("x", "y1", c.X2, "x2")...)
	if c.Nested {
		s = append(s, mergeOnBranch("y", "x2", c.Y3, "y3")...)
		s = append(s, mergeOnBranch("x", "y2", c.X3, "x3")...)
	}
	if c.YLater != nil {
		s = append(s, onBranch("y", c.YLater, "ylater")...)
	}
	if c.XLater != nil {
		s = append(s, onBranch("x", c.XLater, "xlater")...)
	}
	return append(s, "CALL DOLT_CHECKOUT('x');")
}

// ThreeMergeBases is a history in which branches m1 and m2 each merge p1, p2 and p3, in opposite orders, so that they
// have three merge bases:
//
//	c1 -- m1start -- merge p1 -- merge p2 -- merge p3 [-- m1later]    m1
//	 |\-- m2start -- merge p3 -- merge p2 -- merge p1 [-- m2later]    m2
//	 |\-- p1
//	 |\-- p2
//	 \-- p3
type ThreeMergeBases struct {
	// Seed makes c1.
	Seed []string
	// P makes p1, p2 and p3.
	P [3][]string
	// M1Start and M2Start make the first commits of m1 and m2.
	M1Start, M2Start []string
	// M1 resolves m1's merges of p1, p2 and p3, and M2 m2's merges of p1, p2 and p3.
	M1, M2 [3][]string
	// M1Later and M2Later, when set, make a last commit on m1 and m2.
	M1Later, M2Later []string
}

// Script returns the SQL statements that build the history, ending on branch m1.
func (h ThreeMergeBases) Script() []string {
	s := append([]string{"SET @@autocommit = 0;"}, h.Seed...)
	s = append(s, commit("c1")...)
	for _, b := range []string{"p1", "p2", "p3", "m1", "m2"} {
		s = append(s, fmt.Sprintf("CALL DOLT_BRANCH('%s');", b))
	}
	for i, p := range h.P {
		name := fmt.Sprintf("p%d", i+1)
		s = append(s, onBranch(name, p, name)...)
	}
	s = append(s, onBranch("m1", h.M1Start, "m1start")...)
	for _, i := range []int{0, 1, 2} {
		s = append(s, mergeOnBranch("m1", fmt.Sprintf("p%d", i+1), h.M1[i], fmt.Sprintf("m1 merges p%d", i+1))...)
	}
	if h.M1Later != nil {
		s = append(s, onBranch("m1", h.M1Later, "m1later")...)
	}
	s = append(s, onBranch("m2", h.M2Start, "m2start")...)
	for _, i := range []int{2, 1, 0} {
		s = append(s, mergeOnBranch("m2", fmt.Sprintf("p%d", i+1), h.M2[i], fmt.Sprintf("m2 merges p%d", i+1))...)
	}
	if h.M2Later != nil {
		s = append(s, onBranch("m2", h.M2Later, "m2later")...)
	}
	return append(s, "CALL DOLT_CHECKOUT('m1');")
}

func commit(message string) []string {
	return []string{fmt.Sprintf("CALL DOLT_COMMIT('-Am', '%s');", message)}
}

// onBranch runs |statements| on |branch| and commits them as |name|, with a branch of that name.
func onBranch(branch string, statements []string, name string) []string {
	s := append([]string{fmt.Sprintf("CALL DOLT_CHECKOUT('%s');", branch)}, statements...)
	return append(s, commitAndBranch(branch, name)...)
}

// mergeOnBranch merges |source| into |branch|, runs |resolve|, and commits the merge as |name|.
func mergeOnBranch(branch, source string, resolve []string, name string) []string {
	s := []string{fmt.Sprintf("CALL DOLT_CHECKOUT('%s');", branch), fmt.Sprintf("CALL DOLT_MERGE('%s', '--no-commit');", source)}
	s = append(s, resolve...)
	return append(s, commitAndBranch(branch, name)...)
}

// commitAndBranch commits as |name| on |branch|, and names a branch after the commit unless |name| is the branch's
// own name or a phrase.
func commitAndBranch(branch, name string) []string {
	s := commit(name)
	if name != branch && !strings.Contains(name, " ") {
		s = append(s, fmt.Sprintf("CALL DOLT_BRANCH('%s');", name))
	}
	return s
}

// StaleIndexes returns the secondary indexes of the tables of |root|, as "table.index", that differ from the indexes
// their rows build. It skips Full-Text and vector indexes, which are not built from the rows alone.
func StaleIndexes(ctx *sql.Context, root doltdb.RootValue) ([]string, error) {
	names, err := root.GetTableNames(ctx, doltdb.DefaultSchemaName, false)
	if err != nil {
		return nil, err
	}
	var stale []string
	for _, name := range names {
		tbl, _, err := root.GetTable(ctx, doltdb.TableName{Name: name})
		if err != nil {
			return nil, err
		}
		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		rowData, err := tbl.GetRowData(ctx)
		if err != nil {
			return nil, err
		}
		rows, err := durable.ProllyMapFromIndex(rowData)
		if err != nil {
			return nil, err
		}
		for _, index := range sch.Indexes().AllIndexes() {
			if index.IsFullText() || index.IsVector() {
				continue
			}
			stored, err := tbl.GetIndexRowData(ctx, index.Name())
			if err != nil {
				return nil, err
			}
			built, err := creation.BuildProllyIndexExternal(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), sch, name, index, rows, nil, nil)
			if err != nil {
				return nil, err
			}
			storedHash, err := stored.HashOf()
			if err != nil {
				return nil, err
			}
			builtHash, err := built.HashOf()
			if err != nil {
				return nil, err
			}
			if storedHash != builtHash {
				stale = append(stale, name+"."+index.Name())
			}
		}
	}
	return stale, nil
}
