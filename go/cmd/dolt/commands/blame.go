// Copyright 2019 Dolthub, Inc.
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
	"io"
	"sort"
	"strings"
	"time"

	pretty "github.com/jedib0t/go-pretty/table"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var blameDocs = cli.CommandDocumentationContent{
	ShortDesc: `Show what revision and author last modified each row of a table`,
	LongDesc:  `Annotates each row in the given table with information from the revision which last modified the row. Optionally, start annotating from the given revision.`,
	Synopsis: []string{
		`[{{.LessThan}}rev{{.GreaterThan}}] {{.LessThan}}tablename{{.GreaterThan}}`,
	},
}

// blameInfo contains blame information for a row
type blameInfo struct {
	// Key represents the primary key of the row
	Key types.Tuple

	// CommitHash is the commit hash of the commit which last modified the row
	CommitHash string

	// Author is the name of the author of the commit which last modified the row
	Author string

	// Description is the description of the commit which last modified the row
	Description string

	// Timestamp is the timestamp of the commit which last modified the row
	Timestamp int64
}

// TimestampTime returns a time.Time object representing the blameInfo timestamp
func (bi *blameInfo) TimestampTime() time.Time {
	return time.Unix(bi.Timestamp/1000, 0)
}

// TimestampString returns a string representing the blameInfo timestamp
func (bi *blameInfo) TimestampString() string {
	return bi.TimestampTime().Format(time.UnixDate)
}

// A blame graph is a map of primary key hashes to blameInfo structs
type blameGraph map[hash.Hash]blameInfo

type BlameCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd BlameCmd) Name() string {
	return "blame"
}

// Description returns a description of the command
func (cmd BlameCmd) Description() string {
	return "Show what revision and author last modified each row of a table."
}

func (cmd BlameCmd) GatedForNBF(nbf *types.NomsBinFormat) bool {
	return types.IsFormat_DOLT_1(nbf)
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd BlameCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, blameDocs, ap))
}

func (cmd BlameCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// EventType returns the type of the event to log
func (cmd BlameCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_BLAME
}

// Exec implements the `dolt blame` command. Blame annotates each row in the given table with information
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
// Exec executes the command
func (cmd BlameCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, blameDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() == 0 || apr.NArg() > 2 {
		usage()
		return 1
	}

	cs, tableName, err := parseCommitSpecAndTableName(dEnv, apr)
	if err != nil {
		cli.PrintErr(err)
		return 1
	}

	if err := runBlame(ctx, dEnv, cs, tableName); err != nil {
		cli.PrintErr(err)
		return 1
	}

	return 0
}

func parseCommitSpecAndTableName(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (*doltdb.CommitSpec, string, error) {
	// if passed a single arg, assume it's a table name and revision is HEAD
	if apr.NArg() == 1 {
		tableName := apr.Arg(0)
		return dEnv.RepoStateReader().CWBHeadSpec(), tableName, nil
	}

	comSpecStr := apr.Arg(0)
	tableName := apr.Arg(1)

	// support being passed -- as a revision like git does even though it's a little gross
	if comSpecStr == "--" {
		return dEnv.RepoStateReader().CWBHeadSpec(), tableName, nil
	}

	cs, err := doltdb.NewCommitSpec(comSpecStr)
	if err != nil {
		return nil, "", fmt.Errorf("invalid commit %s", comSpecStr)
	}

	return cs, tableName, nil
}

func runBlame(ctx context.Context, dEnv *env.DoltEnv, cs *doltdb.CommitSpec, tableName string) error {
	commit, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		return err
	}

	tbl, err := maybeTableFromCommit(ctx, commit, tableName)
	if err != nil {
		return err
	}
	if tbl == nil {
		return fmt.Errorf("no table named %s found", tableName)
	}

	nbf := tbl.Format()

	blameGraph, err := blameGraphFromCommit(ctx, dEnv, commit, tableName, nbf)
	if err != nil {
		return err
	}

	pkColNames, err := pkColNamesFromCommit(ctx, commit, tableName)
	if err != nil {
		return err
	}

	cli.Println(blameGraph.String(ctx, pkColNames, nbf))
	return nil
}

type blameInput struct {
	Commit       *doltdb.Commit
	Hash         string
	Parent       *doltdb.Commit
	ParentHash   string
	ParentSchema schema.Schema
	ParentTable  *doltdb.Table
	Table        *doltdb.Table
	TableName    string
	Schema       schema.Schema
}

func blameGraphFromCommit(ctx context.Context, dEnv *env.DoltEnv, commit *doltdb.Commit, tableName string, nbf *types.NomsBinFormat) (*blameGraph, error) {
	// get the commits in reverse topological order ending with `commit`
	hash, err := commit.HashOf()
	if err != nil {
		return nil, err
	}
	commits, err := commitwalk.GetTopologicalOrderCommits(ctx, dEnv.DoltDB, hash)
	if err != nil {
		return nil, err
	}

	rows, err := rowsFromCommit(ctx, commit, tableName)
	if err != nil {
		return nil, err
	}

	blameGraph, err := blameGraphFromRows(ctx, nbf, rows)
	if err != nil {
		return nil, err
	}

	// precompute blame inputs for each commit
	blameInputs, err := blameInputsFromCommits(ctx, dEnv, tableName, commits)
	if err != nil {
		return nil, err
	}

ROWLOOP:
	for _, node := range *blameGraph {
		for _, blameInput := range *blameInputs {
			// did the node change between the commit-parent pair represented by blameInput?
			changed, err := rowChanged(ctx, blameInput, node.Key)
			if err != nil {
				return nil, err
			}

			// if so, mark the commit as the blame origin
			if changed {
				blameGraph.AssignBlame(ctx, node.Key, nbf, blameInput.Commit)
				continue ROWLOOP
			}
		}
		// didn't find blame for a row...something's wrong
		var v []string
		pkVals := getPKVal(ctx, node.Key)
		for _, cellValue := range pkVals {
			v = append(v, fmt.Sprintf("%v", cellValue))
		}
		return nil, fmt.Errorf("couldn't find blame for row with primary key %v", strings.Join(v, ", "))
	}

	return blameGraph, nil
}

func blameInputsFromCommits(ctx context.Context, dEnv *env.DoltEnv, tableName string, commits []*doltdb.Commit) (*[]blameInput, error) {
	numCommits := len(commits)
	blameInputs := make([]blameInput, numCommits)
	for i, c := range commits {
		// don't precompute inputs for the initial commit; we don't need them
		if i == numCommits-1 {
			break
		}

		parent, err := dEnv.DoltDB.ResolveParent(ctx, c, 0)
		if err != nil {
			return nil, err
		}

		parentHash, hash, err := getCommitHashes(parent, c)
		if err != nil {
			return nil, err
		}

		tbl, err := maybeTableFromCommit(ctx, c, tableName)
		if err != nil {
			return nil, fmt.Errorf("error getting table from child commit %s: %v", hash, err)
		}
		parentTbl, err := maybeTableFromCommit(ctx, parent, tableName)
		if err != nil {
			return nil, fmt.Errorf("error getting table from parent commit %s: %v", parentHash, err)
		}

		var s schema.Schema
		if tbl != nil {
			s, err = tbl.GetSchema(ctx)
			if err != nil {
				return nil, fmt.Errorf("error getting schema from table %s in child commit %s: %v", tableName, hash, err)
			}
		}

		var parentSchema schema.Schema
		if parentTbl != nil {
			parentSchema, err = parentTbl.GetSchema(ctx)
			if err != nil {
				return nil, fmt.Errorf("error getting schema from table %s in parent commit %s: %v", tableName, parentHash, err)
			}
		}

		blameInputs[i] = blameInput{
			Commit:       c,
			Hash:         hash,
			Parent:       parent,
			ParentHash:   parentHash,
			ParentSchema: parentSchema,
			ParentTable:  parentTbl,
			Table:        tbl,
			TableName:    tableName,
			Schema:       s,
		}
	}
	return &blameInputs, nil
}

// rowsFromCommit returns the row data of the table with the given name at the given commit
func rowsFromCommit(ctx context.Context, commit *doltdb.Commit, tableName string) (types.Map, error) {
	root, err := commit.GetRootValue(ctx)
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

	rowData, err := table.GetNomsRowData(ctx)
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
	root, err := c.GetRootValue(ctx)
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
	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting schema from table: %v", err)
	}

	r, ok, err := table.GetRow(ctx, t, sch, rowPK.(types.Tuple))
	if err != nil {
		return nil, fmt.Errorf("error getting row from table: %v", err)
	}
	if !ok {
		return nil, nil
	}

	return &r, err
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

// rowChanged returns true if the row identified by `rowPK` changed between the parent-child commit pair
// represented by `input`
func rowChanged(ctx context.Context, input blameInput, rowPK types.Value) (bool, error) {
	parentTable := input.ParentTable
	childTable := input.Table

	// if the table is in the parent commit but not the child one...something's wrong. bail!
	if parentTable != nil && childTable == nil {
		return false, fmt.Errorf("expected to find table with name %v in child commit %s, but didn't", input.TableName, input.Hash)
	}
	// if the table is in the child commit but not the parent one, it must be new; return true
	if childTable != nil && parentTable == nil {
		return true, nil
	}

	if input.Schema == nil {
		return false, fmt.Errorf("unexpected nil schema for table %s in child commit %s", input.TableName, input.Hash)
	}
	if input.ParentSchema == nil {
		return false, fmt.Errorf("unexpected nil schema for table %s in parent commit %s", input.TableName, input.ParentHash)
	}

	// if the table schema has changed, every row has changed (according to our current definition of blame)
	if !schema.SchemasAreEqual(input.ParentSchema, input.Schema) {
		return true, nil
	}

	parentRow, err := maybeRowFromTable(ctx, parentTable, rowPK)
	if err != nil {
		return false, fmt.Errorf("error getting row from %s in parent commit %s: %v", input.TableName, input.ParentHash, err)
	}
	childRow, err := maybeRowFromTable(ctx, childTable, rowPK)
	if err != nil {
		return false, fmt.Errorf("error getting row from %s in child commit %s: %v", input.TableName, input.Hash, err)
	}

	// if the row is in the parent table but not the child one...something's wrong. bail!
	if parentRow != nil && childRow == nil {
		return false, fmt.Errorf("expected to find row with PK %v in table %s in child commit %s, but didn't", rowPK, input.TableName, input.Hash)
	}
	// if the row is in the child table but not the parent one, it must be new; return true
	if childRow != nil && parentRow == nil {
		return true, nil
	}

	return !row.AreEqual(*parentRow, *childRow, input.ParentSchema), nil
}

func blameGraphFromRows(ctx context.Context, nbf *types.NomsBinFormat, rows types.Map) (*blameGraph, error) {
	graph := make(blameGraph)
	err := rows.IterAll(ctx, func(key, val types.Value) error {
		hash, err := key.Hash(nbf)
		if err != nil {
			return err
		}
		graph[hash] = blameInfo{Key: key.(types.Tuple)}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &graph, nil
}

// AssignBlame updates the blame graph to contain blame information from the given commit
// for the row identified by the given primary key
func (bg *blameGraph) AssignBlame(ctx context.Context, rowPK types.Tuple, nbf *types.NomsBinFormat, c *doltdb.Commit) error {
	commitHash, err := c.HashOf()
	if err != nil {
		return fmt.Errorf("error getting commit hash: %v", err)
	}

	meta, err := c.GetCommitMeta(ctx)
	if err != nil {
		return fmt.Errorf("error getting metadata for commit %s: %v", commitHash.String(), err)
	}

	pkHash, err := rowPK.Hash(nbf)
	if err != nil {
		return fmt.Errorf("error getting PK hash for commit %s: %v", commitHash.String(), err)
	}

	(*bg)[pkHash] = blameInfo{
		Key:         rowPK,
		CommitHash:  commitHash.String(),
		Author:      meta.Name,
		Description: meta.Description,
		Timestamp:   meta.UserTimestamp,
	}

	return nil
}

func getPKVal(ctx context.Context, pk types.Tuple) (values []types.Value) {
	i := 0
	pk.WalkValues(ctx, func(val types.Value) error {
		// even-indexed values are index numbers. they aren't useful, don't print them.
		if i%2 == 1 {
			values = append(values, val)
		}
		i++
		return nil
	})

	return values
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

type rowMap struct {
	Key   types.Value
	Value []interface{}
}

// String returns the string representation of this blame graph
func (bg *blameGraph) String(ctx context.Context, pkColNames []string, nbf *types.NomsBinFormat) string {
	// here we have two []string and need one []interface{} (aka table.Row)
	// this works but is not beautiful. if you know a better way, have at it!
	header := []interface{}{}
	for _, cellText := range append(pkColNames, dataColNames...) {
		header = append(header, cellText)
	}

	t := pretty.NewWriter()
	t.AppendHeader(header)
	var p []rowMap
	for _, v := range *bg {
		pkVals := getPKVal(ctx, v.Key)
		dataVals := []string{
			truncateString(v.Description, 50),
			v.Author,
			v.TimestampString(),
			v.CommitHash,
		}

		row := []interface{}{}
		for _, cellValue := range pkVals {
			row = append(row, fmt.Sprintf("%v", cellValue))
		}
		for _, cellText := range dataVals {
			row = append(row, cellText)
		}
		pkV, _ := pkVals[0].Value(ctx)
		p = append(p, rowMap{pkV, row})
	}

	sort.Slice(p, func(i, j int) bool {
		isLess, err := p[i].Key.Less(nbf, p[j].Key)
		if err != nil {
			return false
		} else if isLess {
			return true
		} else {
			return false
		}
	})

	for _, k := range p {
		t.AppendRow(k.Value)
	}

	return t.Render()
}
