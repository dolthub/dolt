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
	"errors"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"io"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var cherryPickDocs = cli.CommandDocumentationContent{
	ShortDesc: `Switch branches or restore working tree tables`,
	LongDesc: `
Updates tables in the working set to match the staged versions. If no paths are given, dolt checkout will also update HEAD to set the specified branch as the current branch.

dolt checkout {{.LessThan}}branch{{.GreaterThan}}
   To prepare for working on {{.LessThan}}branch{{.GreaterThan}}, switch to it by updating the index and the tables in the working tree, and by pointing HEAD at the branch. Local modifications to the tables in the working
   tree are kept, so that they can be committed to the {{.LessThan}}branch{{.GreaterThan}}.

dolt checkout -b {{.LessThan}}new_branch{{.GreaterThan}} [{{.LessThan}}start_point{{.GreaterThan}}]
   Specifying -b causes a new branch to be created as if dolt branch were called and then checked out.

dolt checkout {{.LessThan}}table{{.GreaterThan}}...
  To update table(s) with their values in HEAD `,
	Synopsis: []string{
		`{{.LessThan}}branch{{.GreaterThan}}`,
		`{{.LessThan}}table{{.GreaterThan}}...`,
		`-b {{.LessThan}}new-branch{{.GreaterThan}} [{{.LessThan}}start-point{{.GreaterThan}}]`,
	},
}

type CherryPickCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CherryPickCmd) Name() string {
	return "checkout"
}

// Description returns a description of the command
func (cmd CherryPickCmd) Description() string {
	return "Checkout a branch or overwrite a table from HEAD."
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
	return eventsapi.ClientEventType_CHECKOUT // eventsapi.ClientEventType_CHERRY_PICK
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

	cherryStr := apr.Arg(0)
	if len(cherryStr) == 0 {
		verr := errhand.BuildDError("error: cannot cherry-pick empty string").Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	newWorkingRoot, commitMsg, err := CherryPicking(ctx, dEnv, cherryStr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	} else if newWorkingRoot == nil {
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

func CherryPicking(ctx context.Context, dEnv *env.DoltEnv, cherryStr string) (*doltdb.RootValue, string, error) {
	opts := editor.Options{Deaf: dEnv.BulkDbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}

	// check for clean working state
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return nil, "", err
	}
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, "", err
	}
	headHash, err := headRoot.HashOf()
	if err != nil {
		return nil, "", err
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return nil, "", err
	}
	if !headHash.Equal(workingHash) {
		return nil, "", errors.New("You must commit any changes before using cherry-pick.")
	}

	// get cherry-picked commit
	cherryCS, err := doltdb.NewCommitSpec(cherryStr)
	if err != nil {
		return nil, "", err
	}
	cherryCM, err := dEnv.DoltDB.Resolve(ctx, cherryCS, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		return nil, "", err
	}

	ccm, err := cherryCM.GetCommitMeta(ctx)
	if err != nil {
		return nil, "", err
	}
	commitMsg := ccm.Description

	//headCS, err := doltdb.NewCommitSpec("HEAD")
	//if err != nil {
	//	return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	//}
	//headCM, err := dEnv.DoltDB.Resolve(context.TODO(), headCS, dEnv.RepoStateReader().CWBHeadRef())
	//if err != nil {
	//	return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	//}

	workingRoot, err = cherryPick(ctx, dEnv.DoltDB, workingRoot, cherryCM, opts)
	if err != nil {
		return nil, "", err
	}

	workingHash, err = workingRoot.HashOf()
	if err != nil {
		return nil, "", err
	}

	if headHash.Equal(workingHash) {
		cli.Println("No changes were made.")
		return nil, "", nil
	}

	return workingRoot, commitMsg, nil
}

func cherryPick(ctx context.Context, ddb *doltdb.DoltDB, headRoot *doltdb.RootValue, cherryCM *doltdb.Commit, opts editor.Options) (*doltdb.RootValue, error) {
	// TODO : get changes made in the cherry-picked commit
	var err error
	// fromRoot = parentRoot and toRoot = cherryPickRoot
	fromRoot, toRoot, err := getParentAndCherryRoots(ctx, ddb, cherryCM)
	if err != nil {
		return nil, errhand.BuildDError("error: both tables in tableDelta are nil").Build()
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

	tblDeltas, err := diff.GetTableDeltas(ctx, fromRoot, toRoot)

	tblsToDropFromWorkingSet := set.NewStrSet(nil)
	for _, td := range tblDeltas {
		if td.IsDrop() {
			if !workingSetTblSet.Contains(td.FromName) {
				continue
			}

			tblsToDropFromWorkingSet.Add(td.FromName)
			stagedFKs.RemoveKeys(td.FromFks...)
		} else {
			// if tableName in 'to' commit does not exist in working set tables
			// if it was created in previous commits not in working set?
			if !workingSetTblSet.Contains(td.ToName) {
				continue
			}

			if td.IsRename() {
				// rename table before adding the new version, so we don't have
				// two copies of the same table
				headRoot, err = headRoot.RenameTable(ctx, td.FromName, td.ToName)
				if err != nil {
					return nil, err
				}
			}

			// TODO : check for schema changes AND check for row changes
			rowDiffs, err := getRowDiffs(ctx, td)
			if err != nil {
				return nil, err
			}
			//to, err = to.PutTable(ctx, td.ToName, td.ToTable)
			//if err != nil {
			//	return nil, "", err
			//}
			headRoot, err = applyRowDiffs(ctx, headRoot, td.ToName, td.ToTable, rowDiffs, opts)
			if err != nil {
				return nil, err
			}

			stagedFKs.RemoveKeys(td.FromFks...)
			err = stagedFKs.AddKeys(td.ToFks...)
			if err != nil {
				return nil, err
			}

			// TODO : what is superSchema?
			ss, _, err := fromRoot.GetSuperSchema(ctx, td.ToName)
			if err != nil {
				return nil, err
			}

			toRoot, err = toRoot.PutSuperSchema(ctx, td.ToName, ss)
			if err != nil {
				return nil, err
			}
		}
	}

	toRoot, err = toRoot.PutForeignKeyCollection(ctx, stagedFKs)
	if err != nil {
		return nil, err
	}

	// RemoveTables also removes that table's ForeignKeys
	toRoot, err = toRoot.RemoveTables(ctx, false, false, tblsToDropFromWorkingSet.AsSlice()...)
	if err != nil {
		return nil, err
	}

	return headRoot, nil
}

func applyRowDiffs(ctx context.Context, root *doltdb.RootValue, tName string, table *doltdb.Table, diffs []map[string]row.Row, opts editor.Options) (*doltdb.RootValue, error) {
	tbl, _, _ := root.GetTable(ctx, tName)
	tblSchema, _ := tbl.GetSchema(ctx)
	tableEditor, err := editor.NewTableEditor(ctx, tbl, tblSchema, tName, opts)
	for _, diffMap := range diffs {
		to, hasTo := diffMap[diff.To]
		from, hasFrom := diffMap[diff.From]

		if err != nil {
			return nil, err
		}
		if hasTo && hasFrom {
			// update row
			err = tableEditor.UpdateRow(context.Background(), from, to, nil)
			if err != nil {
				cli.Println(err)
				return root, nil
			}
		} else if hasTo && !hasFrom {
			// insert row
			err = tableEditor.InsertRow(ctx, to, nil)
			if err != nil {
				cli.Println(err)
				return root, nil
			}
		} else {
			// delete row
			err = tableEditor.DeleteRow(ctx, from)
			if err != nil {
				cli.Println(err)
				return root, nil
			}
		}
	}

	t, _ := tableEditor.Table(ctx)
	newRoot, err := root.PutTable(ctx, tName, t)
	if err != nil {
		return nil, err
	}

	return newRoot, nil
}

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

func getRowDiffs(ctx context.Context, td diff.TableDelta) ([]map[string]row.Row, error) {
	fromTable := td.FromTable
	toTable := td.ToTable

	if fromTable == nil && toTable == nil {
		return nil, errhand.BuildDError("error: both tables in tableDelta are nil").Build()
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

		diffs = append(diffs, toAndFromRows)
	}

	return diffs, nil
}
