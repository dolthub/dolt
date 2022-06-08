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

package commands

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"io"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var cherryPickDocs = cli.CommandDocumentationContent{
	ShortDesc: `Apply the changes introduced by an existing commit.`,
	LongDesc: `
Updates tables in the clean working set with changes introduced in cherry-picked commit and creates a new commit with applied changes.

dolt cherry-pick {{.LessThan}}commit{{.GreaterThan}}
   To apply changes from an existing {{.LessThan}}commit{{.GreaterThan}} to current HEAD, the current working tree must be clean (no modifications from the HEAD commit). 
   By default, cherry-pick creates new commit with applied changes.`,
	Synopsis: []string{
		`{{.LessThan}}commit{{.GreaterThan}}`,
	},
}

type CherryPickCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CherryPickCmd) Name() string {
	return "cherry-pick"
}

// Description returns a description of the command
func (cmd CherryPickCmd) Description() string {
	return "Apply the changes introduced by an existing commit from different branch."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd CherryPickCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cli.CreateCherryPickArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, cherryPickDocs, ap))
}

func (cmd CherryPickCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCherryPickArgParser()
}

// EventType returns the type of the event to log
func (cmd CherryPickCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CHERRY_PICK
}

// Exec executes the command
func (cmd CherryPickCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, dumpDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	// This command creates a commit, so we need user identity
	if !cli.CheckUserNameAndEmail(dEnv) {
		return 1
	}

	// TODO : support single commit cherry-pick only for now
	if apr.NArg() == 0 {
		usage()
		return 1
	} else if apr.NArg() > 1 {
		return HandleVErrAndExitCode(errhand.BuildDError("multiple commits not supported yet.").SetPrintUsage().Build(), usage)
	}

	// check for clean working state
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	headHash, err := headRoot.HashOf()
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if !headHash.Equal(workingHash) {
		return HandleVErrAndExitCode(errhand.BuildDError("You must commit any changes before using cherry-pick.").SetPrintUsage().Build(), usage)
	}

	cherryStr := apr.Arg(0)
	if len(cherryStr) == 0 {
		verr := errhand.BuildDError("error: cannot cherry-pick empty string").Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	newWorkingRoot, commitMsg, err := CherryPick(ctx, dEnv, workingRoot, cherryStr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	workingHash, err = newWorkingRoot.HashOf()
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if headHash.Equal(workingHash) {
		cli.Println("No changes were made.")
		return 0
	}

	err = dEnv.UpdateWorkingRoot(ctx, newWorkingRoot)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	res := AddCmd{}.Exec(ctx, "add", []string{"-A"}, dEnv)
	if res != 0 {
		return res
	}

	// Pass in the final parameters for the author string.
	commitParams := []string{"-m", commitMsg}
	authorStr, ok := apr.GetValue(cli.AuthorParam)
	if ok {
		commitParams = append(commitParams, "--author", authorStr)
	}

	return CommitCmd{}.Exec(ctx, "commit", commitParams, dEnv)
}

// CherryPick returns updated Root value for current HEAD and commit message of cherry-picked commit.
func CherryPick(ctx context.Context, dEnv *env.DoltEnv, wRoot *doltdb.RootValue, cherryStr string) (*doltdb.RootValue, string, error) {
	opts := editor.Options{Deaf: dEnv.BulkDbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}

	cherrySpec, err := doltdb.NewCommitSpec(cherryStr)
	if err != nil {
		return nil, "", err
	}
	cherryCommit, err := dEnv.DoltDB.Resolve(ctx, cherrySpec, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		return nil, "", err
	}

	cherryCM, err := cherryCommit.GetCommitMeta(ctx)
	if err != nil {
		return nil, "", err
	}
	commitMsg := cherryCM.Description

	updatedRoot, err := cherryPickACommit(ctx, dEnv.DoltDB, wRoot, cherryCommit, opts)
	if err != nil {
		return nil, "", err
	}

	return updatedRoot, commitMsg, nil
}

// cherryPickACommit takes working root and cherry-pick commit and returns updated root with changes from cherry-pick commit applied.
func cherryPickACommit(ctx context.Context, ddb *doltdb.DoltDB, headRoot *doltdb.RootValue, cherryCM *doltdb.Commit, opts editor.Options) (*doltdb.RootValue, error) {
	var err error
	// parentCommitRoot, cherryCommitRoot
	fromRoot, toRoot, err := getParentAndCherryRoots(ctx, ddb, cherryCM)
	if err != nil {
		return nil, errhand.BuildDError("failed to get cherry-picked commit and its parent commit").AddCause(err).Build()
	}

	workingSetTblNames, err := doltdb.GetAllTableNames(ctx, headRoot)
	if err != nil {
		return nil, errhand.BuildDError("failed to get table names").AddCause(err).Build()
	}
	workingSetTblSet := set.NewStrSet(workingSetTblNames)

	stagedFKs, err := toRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}
	tblsToDrop := set.NewStrSet(nil)

	// changes introduced in cherry-picked commit
	tblDeltas, err := diff.GetTableDeltas(ctx, fromRoot, toRoot)
	for _, td := range tblDeltas {
		if td.IsDrop() {
			if !workingSetTblSet.Contains(td.FromName) {
				continue
			}

			tblsToDrop.Add(td.FromName)
			stagedFKs.RemoveKeys(td.FromFks...)
		} else if td.IsAdd() {
			if workingSetTblSet.Contains(td.ToName) {
				// TODO: (skipped) added table already exists in current HEAD => CONFLICT
				continue
			} else {
				headRoot, err = headRoot.PutTable(ctx, td.ToName, td.ToTable)
				if err != nil {
					return nil, err
				}
			}
		} else {
			if td.IsRename() {
				// rename table before adding the new version, so we don't have
				// two copies of the same table
				headRoot, err = headRoot.RenameTable(ctx, td.FromName, td.ToName)
				if err != nil {
					return nil, err
				}
			}

			// TODO : check for schema changes
			if !schema.SchemasAreEqual(td.FromSch, td.ToSch) {
				headRoot, err = handleSchemaChanges(ctx, headRoot, td.FromSch, td.ToSch, td.ToName)
				if err != nil {
					return nil, err
				}
			}

			rowDiffs, err := getRowDiffs(ctx, td)
			if err != nil {
				return nil, err
			}
			headRoot, err = applyRowDiffs(ctx, headRoot, td.ToName, rowDiffs, opts)
			if err != nil {
				return nil, err
			}

			stagedFKs.RemoveKeys(td.FromFks...)
			err = stagedFKs.AddKeys(td.ToFks...)
			if err != nil {
				return nil, err
			}

			// TODO : what is superSchema doing?
			//ss, _, err := fromRoot.GetSuperSchema(ctx, td.ToName)
			//if err != nil {
			//	return nil, err
			//}
			//
			//headRoot, err = headRoot.PutSuperSchema(ctx, td.ToName, ss)
			//if err != nil {
			//	return nil, err
			//}
		}
	}

	headRoot, err = headRoot.PutForeignKeyCollection(ctx, stagedFKs)
	if err != nil {
		return nil, err
	}

	// RemoveTables also removes that table's ForeignKeys
	headRoot, err = headRoot.RemoveTables(ctx, false, false, tblsToDrop.AsSlice()...)
	if err != nil {
		return nil, err
	}

	return headRoot, nil
}

func handleSchemaChanges(ctx context.Context, root *doltdb.RootValue, parentSch, toSch schema.Schema, tblName string) (*doltdb.RootValue, error) {
	tbl, _, err := root.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if !schema.SchemasAreEqual(sch, parentSch) {
		// if current HEAD table schema is not the same as parent table schema of cherry-picked table, schema changes not supported
		return nil, errhand.BuildDError(fmt.Sprintf("%s table schema in working tree is not the same as parent commit's of cherry-picked commit", tblName)).AddCause(err).Build()
	}

	if !sch.Indexes().Equals(toSch.Indexes()) {
		// any change on indexes, not supported yet
		return nil, errhand.BuildDError(fmt.Sprintf("index on %s table in working tree is not the same as parent commit's of cherry-picked commit", tblName)).AddCause(err).Build()
	}

	newTbl, err := tbl.UpdateSchema(ctx, toSch)
	if err != nil {
		return nil, err
	}

	return root.PutTable(ctx, tblName, newTbl)
}

// getParentAndCherryRoots return root values of parent commit of cherry-picked commit and cherry-picked commit itself.
func getParentAndCherryRoots(ctx context.Context, ddb *doltdb.DoltDB, cherryCommit *doltdb.Commit) (*doltdb.RootValue, *doltdb.RootValue, error) {
	cherryRoot, err := cherryCommit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	var parentRoot *doltdb.RootValue
	if len(cherryCommit.DatasParents()) > 0 {
		parentCM, err := ddb.ResolveParent(ctx, cherryCommit, 0)
		if err != nil {
			return nil, nil, err
		}
		parentRoot, err = parentCM.GetRootValue(ctx)
		if err != nil {
			return nil, nil, err
		}
	} else {
		parentRoot, err = doltdb.EmptyRootValue(ctx, ddb.ValueReadWriter())
		if err != nil {
			return nil, nil, err
		}
	}
	return parentRoot, cherryRoot, nil
}

// getRowDiffs returns diffs for each table delta as a map of keys 'to' and/or 'from' with values in Row format.
func getRowDiffs(ctx context.Context, td diff.TableDelta) ([]map[string]row.Row, error) {
	fromTable := td.FromTable
	toTable := td.ToTable

	if fromTable == nil && toTable == nil {
		return nil, errhand.BuildDError(fmt.Sprintf("error: no changes between commits for table %s", td.ToName)).Build()
	}

	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return nil, errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
	}

	if td.IsAdd() {
		fromSch = toSch
	} else if td.IsDrop() {
		toSch = fromSch
	}

	fromRows, toRows, err := td.GetMaps(ctx)
	if err != nil {
		return nil, errhand.BuildDError("could not get row data for table %s", td.ToName).AddCause(err).Build()
	}

	joiner, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{
			{Name: diff.From, Sch: fromSch},
			{Name: diff.To, Sch: toSch},
		},
		map[string]rowconv.ColNamingFunc{diff.To: toNamer, diff.From: fromNamer},
	)
	if err != nil {
		return nil, errhand.BuildDError("").AddCause(err).Build()
	}

	rd := diff.NewRowDiffer(ctx, fromSch, toSch, 1024)
	if _, ok := rd.(*diff.EmptyRowDiffer); ok {
		cli.Println("warning: skipping data diff due to primary key set change")
		return nil, nil
	}
	rd.Start(ctx, fromRows, toRows)
	defer rd.Close()

	src := diff.NewRowDiffSource(rd, joiner, nil)
	defer src.Close()

	var diffs []map[string]row.Row
	for {
		var r row.Row
		var iterErr error

		r, _, iterErr = src.NextDiff()
		if iterErr == io.EOF {
			break
		} else if iterErr != nil {
			return nil, iterErr
		}
		toAndFromRows, iterErr := joiner.Split(r)
		if iterErr != nil {
			return nil, iterErr
		}

		diffs = append(diffs, toAndFromRows)
	}

	return diffs, nil
}

func applyRowDiffs(ctx context.Context, root *doltdb.RootValue, tName string, diffs []map[string]row.Row, opts editor.Options) (*doltdb.RootValue, error) {
	if len(diffs) == 0 {
		return root, nil
	}
	tbl, _, err := root.GetTable(ctx, tName)
	if err != nil {
		return nil, err
	}
	tblSchema, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	tblEditor, err := editor.NewTableEditor(ctx, tbl, tblSchema, tName, opts)
	if err != nil {
		return nil, err
	}

	for _, diffMap := range diffs {
		to, hasTo := diffMap[diff.To]
		from, hasFrom := diffMap[diff.From]

		if err != nil {
			return nil, err
		}
		if hasTo && hasFrom {
			// UPDATE ROW only if oldRow exists in current working set table
			row, er := getRowInTable(ctx, tbl, tblSchema, from)
			if er != nil {
				return nil, er
			} else if row == nil {
				continue
			}
			err = tblEditor.UpdateRow(ctx, row, to, nil)
			if err != nil {
				cli.Println(err)
				return root, nil
			}
		} else if hasTo && !hasFrom {
			// INSERT ROW
			err = tblEditor.InsertRow(ctx, to, nil)
			if err != nil {
				cli.Println(err)
				return root, nil
			}
		} else {
			// DELETE ROW
			err = tblEditor.DeleteRow(ctx, from)
			if err != nil {
				cli.Println(err)
				return root, nil
			}
		}
	}

	t, err := tblEditor.Table(ctx)
	if err != nil {
		return nil, err
	}

	newRoot, err := root.PutTable(ctx, tName, t)
	if err != nil {
		return nil, err
	}

	return newRoot, nil
}

func getRowInTable(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, r row.Row) (row.Row, error) {
	k, v, err := row.ToNoms(ctx, sch, r)
	if err != nil {
		return nil, err
	}
	rm, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}
	val, exists, err := rm.MaybeGetTuple(ctx, k)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, nil
	} else if !val.Equals(v) {
		return nil, err
	}

	return row.FromNoms(sch, k, v)
}
