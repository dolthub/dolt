// Copyright 2019 Liquidata, Inc.
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

package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/jedib0t/go-pretty/table"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var blameShortDesc = `Show what revision and author last modified each row of a table`
var blameLongDesc = `Annotates each row in the given table with information from the revision which last modified the row. Optionally, start annotating from the given revision.`

var blameSynopsis = []string{
	`[<rev>] <tablename>`,
}

// blameInfo contains blame information for a row
type blameInfo struct {
	// Key represents the primary key of the row
	Key types.Value

	// CommitHash is the commit hash of the commit which last modified the row
	CommitHash string

	// Author is the name of the author of the commit which last modified the row
	Author string

	// Description is the description of the commit which last modified the row
	Description string

	// Timestamp is the timestamp of the commit which last modified the row
	Timestamp uint64
}

// TimestampTime returns a time.Time object representing the blameInfo timestamp
func (bi *blameInfo) TimestampTime() time.Time {
	return time.Unix(int64(bi.Timestamp/1000), 0)
}

// TimestampString returns a string representing the blameInfo timestamp
func (bi *blameInfo) TimestampString() string {
	return bi.TimestampTime().Format(time.UnixDate)
}

// A blame graph is a map of primary key hashes to blameInfo structs
type blameGraph map[hash.Hash]blameInfo

// Blame implements the `dolt blame` command. Blame annotates each row in the given table with information
// from the revision which last modified the row, optionally starting from a given revision.
//
// Blame is computed as follows:
//
// First, a blame graph is initialized with one node for every row in the table at the given commit (defaulting
// to HEAD of the currently checked-out branch).
//
// Starting from the given commit, walk backwards through the commit graph (currently by following each commit's
// first parent, though this may change in the future).
//
// For each adjacent pair of commits `old` and `new`, check each remaining unblamed node to see if the row it represents
// changed between the commits. If so, mark it with `new` as the blame origin and continue to the next node without blame.
//
// When all nodes have blame information, stop iterating through commits and print the blame graph.
func Blame(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, blameShortDesc, blameLongDesc, blameSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() < 2 {
		usage()
		return 1
	}

	cs, err := parseCommitSpec(dEnv, apr)
	if err != nil {
		cli.PrintErr(err)
		return 1
	}

	tableName := apr.Arg(1)

	if err := runBlame(ctx, dEnv, cs, tableName); err != nil {
		cli.PrintErr(err)
		return 1
	}

	return 0
}

func runBlame(ctx context.Context, dEnv *env.DoltEnv, cs *doltdb.CommitSpec, tableName string) error {
	commit, err := dEnv.DoltDB.Resolve(ctx, cs)
	if err != nil {
		return err
	}

	blameGraph, err := blameGraphFromCommit(ctx, dEnv, commit, tableName)
	if err != nil {
		return err
	}

	pkColNames, err := pkColNamesFromCommit(ctx, commit, tableName)
	if err != nil {
		return err
	}

	cli.Println(blameGraph.String(ctx, pkColNames))
	return nil
}

func blameGraphFromCommit(ctx context.Context, dEnv *env.DoltEnv, commit *doltdb.Commit, tableName string) (*blameGraph, error) {
	// get the commits sorted from newest to oldest ending with `commit`
	commits, err := actions.TimeSortedCommits(ctx, dEnv.DoltDB, commit, -1)
	if err != nil {
		return nil, err
	}

	rows, err := rowsFromCommit(ctx, commit, tableName)
	if err != nil {
		return nil, err
	}

	blameGraph, err := blameGraphFromRows(ctx, rows)
	if err != nil {
		return nil, err
	}

	for _, c := range commits {
		// get the first parent
		parent, err := dEnv.DoltDB.ResolveParent(ctx, c, 0)
		if err != nil {
			return nil, err
		}

		// for each unblamed node, see if it changed between `c` and `parent`.
		// if so, mark it blamed with `c` as the blame origin
		for _, rowPK := range blameGraph.UnblamedNodeKeys() {
			changed, err := rowChanged(ctx, parent, c, tableName, rowPK)
			if err != nil {
				return nil, err
			}

			if changed {
				blameGraph.AssignBlame(rowPK, c)
			}
		}

		// if all nodes have blame, stop iterating commits
		if blameGraph.AllNodesBlamed() {
			break
		}
	}
	return blameGraph, nil
}

// rowsFromCommit returns the row data of the table with the given name at the given commit
func rowsFromCommit(ctx context.Context, commit *doltdb.Commit, tableName string) (types.Map, error) {
	root, err := commit.GetRootValue()
	if err != nil {
		return types.EmptyMap, err
	}

	table, ok, err := root.GetTable(ctx, tableName)
	if err != nil {
		return types.EmptyMap, err
	}
	if !ok {
		return types.EmptyMap, fmt.Errorf("no table named %s found", tableName)
	}

	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	return rowData, nil
}

func getCommitHashes(old, new *doltdb.Commit) (string, string, error) {
	oldHash, err := old.HashOf()
	if err != nil {
		return "", "", fmt.Errorf("error getting hash of old commit: %v", err)
	}
	newHash, err := new.HashOf()
	if err != nil {
		return "", "", fmt.Errorf("error getting hash of new commit: %v", err)
	}
	return oldHash.String(), newHash.String(), nil
}

// maybeTableFromCommit takes a commit and a table name and returns a (possibly nil) pointer to a table
func maybeTableFromCommit(ctx context.Context, c *doltdb.Commit, tableName string) (*doltdb.Table, error) {
	root, err := c.GetRootValue()
	if err != nil {
		return nil, fmt.Errorf("error getting root value of commit: %v", err)
	}
	table, _, err := root.GetTable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("error getting table %s from root value: %v", tableName, err)
	}
	return table, nil
}

// maybeRowFromTable takes a table and a primary key and returns a (possibly nil) pointer to a row
func maybeRowFromTable(ctx context.Context, t *doltdb.Table, rowPK types.Value) (*row.Row, error) {
	schema, err := t.GetSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting schema from table: %v", err)
	}

	row, ok, err := t.GetRow(ctx, rowPK.(types.Tuple), schema)
	if err != nil {
		return nil, fmt.Errorf("error getting row from table: %v", err)
	}
	if !ok {
		return nil, nil
	}

	return &row, err
}

func schemaFromCommit(ctx context.Context, c *doltdb.Commit, tableName string) (schema.Schema, error) {
	t, err := maybeTableFromCommit(ctx, c, tableName)
	if err != nil {
		return nil, fmt.Errorf("error getting table %s from commit: %v", tableName, err)
	}
	if t == nil {
		return nil, fmt.Errorf("no table named %s found in commit", tableName)
	}

	schema, err := t.GetSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting schema from table %s: %v", tableName, err)
	}

	return schema, nil
}

func pkColNamesFromCommit(ctx context.Context, c *doltdb.Commit, tableName string) ([]string, error) {
	schema, err := schemaFromCommit(ctx, c, tableName)
	if err != nil {
		return nil, fmt.Errorf("error getting schema for commit: %v", err)
	}
	return schema.GetPKCols().GetColumnNames(), nil
}

// rowChanged returns true if the row identified by `rowPK` in `tableName` changed between commits `old` and `new`
// It's a bit inefficient when used repeatedly on adjacent commits such as in blame; each iteration it will
// retrieve a row that was already retrieved on the previous iteration (i.e. last iteration's "old" row
// is this iteration's "new" row).
func rowChanged(ctx context.Context, old, new *doltdb.Commit, tableName string, rowPK types.Value) (bool, error) {
	oldHash, newHash, err := getCommitHashes(old, new)
	if err != nil {
		return false, err
	}

	oldTable, err := maybeTableFromCommit(ctx, old, tableName)
	if err != nil {
		return false, fmt.Errorf("error getting table from old commit %s: %v", oldHash, err)
	}
	newTable, err := maybeTableFromCommit(ctx, new, tableName)
	if err != nil {
		return false, fmt.Errorf("error getting table from new commit %s: %v", newHash, err)
	}

	// if the table is in the old commit but not the new one...something's wrong. bail!
	if oldTable != nil && newTable == nil {
		return false, fmt.Errorf("expected to find table with name %v in new commit %s, but didn't", tableName, newHash)
	}
	// if the table is in the new commit but not the old one, it must be new; return true
	if newTable != nil && oldTable == nil {
		return true, nil
	}

	oldRow, err := maybeRowFromTable(ctx, oldTable, rowPK)
	if err != nil {
		return false, fmt.Errorf("error getting row from %s in old commit %s: %v", tableName, oldHash, err)
	}
	newRow, err := maybeRowFromTable(ctx, newTable, rowPK)
	if err != nil {
		return false, fmt.Errorf("error getting row from %s in new commit %s: %v", tableName, newHash, err)
	}

	// if the row is in the old table but not the new one...something's wrong. bail!
	if oldRow != nil && newRow == nil {
		return false, fmt.Errorf("expected to find row with PK %v in table %s in new commit %s, but didn't", rowPK, tableName, newHash)
	}
	// if the row is in the new table but not the old one, it must be new; return true
	if newRow != nil && oldRow == nil {
		return true, nil
	}

	oldSchema, err := oldTable.GetSchema(ctx)
	if err != nil {
		return false, fmt.Errorf("error getting schema from %s in old commit %s: %v", tableName, oldHash, err)
	}

	return !row.AreEqual(*oldRow, *newRow, oldSchema), nil
}

func blameGraphFromRows(ctx context.Context, rows types.Map) (*blameGraph, error) {
	graph := make(blameGraph)
	err := rows.IterAll(ctx, func(key, _ types.Value) error {
		hash, err := key.Hash(types.Format_7_18)
		if err != nil {
			return err
		}
		graph[hash] = blameInfo{Key: key}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &graph, nil
}

// UnblamedNodeKeys returns a slice of primary keys which do not have blame information
func (bg *blameGraph) UnblamedNodeKeys() []types.Value {
	keys := []types.Value{}
	for _, v := range *bg {
		if v.CommitHash == "" {
			keys = append(keys, v.Key)
		}
	}
	return keys
}

// AllNodesBlamed returns true if there are no unblamed nodes in the blame graph
func (bg *blameGraph) AllNodesBlamed() bool {
	return len(bg.UnblamedNodeKeys()) == 0
}

// AssignBlame updates the blame graph to contain blame information from the given commit
// for the row identified by the given primary key
func (bg *blameGraph) AssignBlame(rowPK types.Value, c *doltdb.Commit) error {
	commitHash, err := c.HashOf()
	if err != nil {
		return fmt.Errorf("error getting commit hash: %v", err)
	}

	meta, err := c.GetCommitMeta()
	if err != nil {
		return fmt.Errorf("error getting metadata for commit %s: %v", commitHash.String(), err)
	}

	pkHash, err := rowPK.Hash(types.Format_7_18)
	if err != nil {
		return fmt.Errorf("error getting PK hash for commit %s: %v", commitHash.String(), err)
	}

	(*bg)[pkHash] = blameInfo{
		Key:         rowPK,
		CommitHash:  commitHash.String(),
		Author:      meta.Name,
		Description: meta.Description,
		Timestamp:   meta.Timestamp,
	}

	return nil
}

func getPKStrs(ctx context.Context, pk types.Value) (strs []string) {
	i := 0
	pk.WalkValues(ctx, func(val types.Value) error {
		// even-indexed values are index numbers. they aren't useful, don't print them.
		if i%2 == 1 {
			strs = append(strs, fmt.Sprintf("%v", val))
		}
		i++
		return nil
	})

	return strs
}

func truncateString(str string, maxLength int) string {
	if maxLength < 0 || len(str) <= maxLength {
		return str
	}

	if maxLength == 0 || str == "" {
		return ""
	}

	if maxLength == 1 {
		return "…"
	}

	return str[0:maxLength-1] + "…"
}

var dataColNames = []string{"Commit Msg", "Author", "Time", "Commit"}

// String returns the string representation of this blame graph
func (bg *blameGraph) String(ctx context.Context, pkColNames []string) string {
	// here we have two []string and need one []interface{} (aka table.Row)
	// this works but is not beautiful. if you know a better way, have at it!
	header := []interface{}{}
	for _, cellText := range append(pkColNames, dataColNames...) {
		header = append(header, cellText)
	}

	t := table.NewWriter()
	t.AppendHeader(header)
	for _, v := range *bg {
		pkVals := getPKStrs(ctx, v.Key)
		dataVals := []string{
			truncateString(v.Description, 50),
			v.Author,
			v.TimestampString(),
			v.CommitHash,
		}

		row := []interface{}{}
		for _, cellText := range pkVals {
			row = append(row, cellText)
		}
		for _, cellText := range dataVals {
			row = append(row, cellText)
		}
		t.AppendRow(row)
	}
	return t.Render()
}
