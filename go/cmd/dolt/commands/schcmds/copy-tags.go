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

package schcmds

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

var copyTagsDocs = cli.CommandDocumentationContent{
	ShortDesc: "Copy the column tags from one branch to the current branch",
	LongDesc: `{{.EmphasisLeft}}dolt schema copy-tags {{.LessThan}}from-branch{{.GreaterThan}} {{.EmphasisRight}}

Copies all column tags from the HEAD commit on the specified branch to the currently checked out branch. Useful 
to fix a merge that is returning many column tag conflict errors after schema changes. Only tables and columns that 
match by name will be synced with the column tags from the specified branch. To update a single column tag, customers
can use the {{.LessThan}}dolt schema update-tag{{.GreaterThan}} command instead. 

This is an advanced command that most customers shouldn't ever need to use. Customers should generally only use this 
command after asking the Dolt team for help with column tag conflict errors. If in doubt, reach out to the Dolt team 
on Discord or GitHub.`,
	Synopsis: []string{
		"{{.LessThan}}from-branch{{.GreaterThan}}",
	},
}

// CopyTagsCmd implements the cli.Command interface for the schema copy-tags command.
type CopyTagsCmd struct{}

var _ cli.Command = CopyTagsCmd{}
var _ cli.HiddenCommand = CopyTagsCmd{}

// Name implements the cli.Command interface.
func (cmd CopyTagsCmd) Name() string {
	return "copy-tags"
}

// Description implements the cli.Command interface.
func (cmd CopyTagsCmd) Description() string {
	return "Update the column tags on one branch to match another branch"
}

// Docs implements the cli.Command interface.
func (cmd CopyTagsCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(copyTagsDocs, ap)
}

// Hidden implements the cli.HiddenCommand interface.
func (cmd CopyTagsCmd) Hidden() bool {
	return true
}

// ArgParser implements the cli.Command interface.
func (cmd CopyTagsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 3)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"from-branch", "The name of the branch from which to copy tags"})
	return ap
}

// Exec implements the cli.Command interface.
func (cmd CopyTagsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, _ cli.CliContext) int {
	fromBranchName, fromRoots, toRoots, usage, err := cmd.validateArgs(ctx, commandStr, args, dEnv)
	if err != nil {
		verr := errhand.BuildDError("error validating arguments")
		return commands.HandleVErrAndExitCode(verr.AddCause(err).Build(), usage)
	}

	// Load all the tags from fromBranch and toBranch
	fromBranchTags, err := getAllTagsForRoots(ctx, fromRoots.Head)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("failed to get tags").AddCause(err).Build(), usage)
	}
	toBranchTags, err := getAllTagsForRoots(ctx, toRoots.Head)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("failed to get tags").AddCause(err).Build(), usage)
	}

	// Iterate over the fromBranch tags
	tagsSynced := 0
	workingRoot := toRoots.Working
	for tag, fromTableAndColumn := range fromBranchTags {
		// Does the main branch have this tag?
		if toTableAndColumn, ok := toBranchTags[tag]; ok {
			// If the tag references the same table/column on both branches, then we don't need to
			// do anything, just move on to the next tag.
			if toTableAndColumn.column == fromTableAndColumn.column &&
				toTableAndColumn.table == fromTableAndColumn.table {
				// This means the tag is identical on both sides, so we don't need to do anything
				continue
			}

			// At this point, we have a tag conflict... that means there is already another column on the target
			// branch that is using this tag that we need to sync. We can't sync this tag yet, because then there
			// would be two columns with the same tag on the target branch. The API for updating a table in a root
			// works with a single table at a time, so we have to ensure tags are valid (i.e. not duplicated) at
			// every table update we store to the root, otherwise writing the table to the root throws an error.
			// So, we need to move the existing tag occurrence to an unused tag and then sync the current tag on
			// the target branch.
			newTag := generateUnusedTag(toBranchTags, tag)
			cli.Printf("changing %s.%s to %d\n"+
				"syncing %s.%s to %d\n",
				toTableAndColumn.table, toTableAndColumn.column, newTag,
				fromTableAndColumn.table, fromTableAndColumn.column, tag)

			// Only count the tag we are syncing from the source branch, not the tag we are moving out of the way
			tagsSynced += 1
			workingRoot, err = updateRootWithNewColumnTag(ctx, workingRoot, toTableAndColumn.table, toTableAndColumn.column, newTag)
			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("failed to update column tag").AddCause(err).Build(), usage)
			}
			toBranchTags[newTag] = toTableAndColumn

			// Now sync the tag on the target branch from the source branch
			workingRoot, err = updateRootWithNewColumnTag(ctx, workingRoot, fromTableAndColumn.table, fromTableAndColumn.column, tag)
			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("failed to update column tag").AddCause(err).Build(), usage)
			}
			toBranchTags[tag] = fromTableAndColumn
		} else {
			// The main branch doesn't have this tag at all, so fix the column
			cli.Printf("syncing %s.%s to %d\n",
				fromTableAndColumn.table, fromTableAndColumn.column, tag)

			tagsSynced += 1
			workingRoot, err = updateRootWithNewColumnTag(ctx, workingRoot, fromTableAndColumn.table, fromTableAndColumn.column, tag)
			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("failed to update column tag").AddCause(err).Build(), usage)
			}
			toBranchTags[tag] = fromTableAndColumn
		}
	}

	if tagsSynced > 0 {
		if err = doltCommitUpdatedTags(ctx, dEnv, workingRoot, fromBranchName); err != nil {
			vErr := errhand.BuildDError("failed to commit column tag updates").AddCause(err).Build()
			return commands.HandleVErrAndExitCode(vErr, usage)
		}
		cli.Printf("\n%d column tags synced from branch %s\n", tagsSynced, fromBranchName)
	} else {
		cli.Printf("\nNo tag changes needed\n")
	}

	return 0
}

// validateArgs validates the |commandStr| and |args| that the user specified on the command line. Returns the name of
// the branch where column tags are taken from, as well as the roots for the target branch as well as the source branch,
// and usage information. An error is returned if any validation errors are detected, such as not finding the specified
// branch or the target branch's working set not being clean.
func (cmd CopyTagsCmd) validateArgs(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) (fromBranchName string, fromRoots, toRoots *doltdb.Roots, usage cli.UsagePrinter, err error) {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, copyTagsDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	doltDB := dEnv.DoltDB(ctx)
	if !types.IsFormat_DOLT(doltDB.Format()) {
		return "", nil, nil, nil, fmt.Errorf("copy-tags is only available for modern database storage formats")
	}

	if len(apr.Args) != 1 {
		return "", nil, nil, nil, fmt.Errorf("must provide <from-branch>")
	}
	fromBranchName = apr.Args[0]

	branches, err := doltDB.GetBranches(ctx)
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("failed to get branches")
	}

	var fromBranchRef *ref.BranchRef
	for _, branch := range branches {
		if branch.GetPath() == fromBranchName {
			branchRef := branch.(ref.BranchRef)
			fromBranchRef = &branchRef
		}
	}

	if fromBranchRef == nil {
		return "", nil, nil, nil, fmt.Errorf("failed to find branch %s", fromBranchName)
	}

	sourceBranchRoots, err := doltDB.ResolveBranchRoots(ctx, *fromBranchRef)
	if err != nil {
		return "", nil, nil, nil, err
	}

	currentBranchRoots, err := dEnv.Roots(ctx)
	if err != nil {
		return "", nil, nil, nil, err
	}

	// Assert that the destination branch does not have any outstanding changes
	if err = validateDestinationBranch(ctx, &currentBranchRoots); err != nil {
		return "", nil, nil, nil, err
	}

	return fromBranchName, &sourceBranchRoots, &currentBranchRoots, usage, nil
}

// validateDestinationBranch checks that the working set of the destination branch does not have outstanding changes
// so that they don't get committed with the tag changes. If outstanding changes are found, an error is returned.
func validateDestinationBranch(ctx context.Context, toRoots *doltdb.Roots) error {
	clean, err := diff.WorkingSetContainsOnlyIgnoredTables(ctx, *toRoots)
	if err != nil {
		return err
	}
	if !clean {
		return fmt.Errorf("current branch's working set is not clean; " +
			"commit or discard any changes and try again")
	}
	return nil
}

// doltCommitUpdatedTags commits tag changes in |workingRoot| for the specified DoltEnv, |dEnv|. The commit message uses
// |fromBranchName| to document the source of the tag changes.
func doltCommitUpdatedTags(ctx context.Context, dEnv *env.DoltEnv, workingRoot doltdb.RootValue, fromBranchName string) (err error) {
	if err = dEnv.UpdateWorkingRoot(ctx, workingRoot); err != nil {
		return err
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return err
	}
	roots.Staged = workingRoot
	roots.Working = workingRoot

	workingSet, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}

	workingSet = workingSet.WithWorkingRoot(workingRoot)
	workingSet = workingSet.WithStagedRoot(workingRoot)

	email, err := dEnv.Config.GetString(config.UserEmailKey)
	if err != nil {
		return err
	}

	name, err := dEnv.Config.GetString(config.UserNameKey)
	if err != nil {
		return err
	}

	doltDB := dEnv.DoltDB(ctx)
	pendingCommit, err := actions.GetCommitStaged(ctx, roots, workingSet, nil, doltDB, actions.CommitStagedProps{
		Name:    name,
		Email:   email,
		Message: "Syncing column tags from " + fromBranchName + " branch",
	})
	if err != nil {
		return err
	}

	headRef, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		return err
	}
	prevHash, err := workingSet.HashOf()
	if err != nil {
		return err
	}

	_, err = doltDB.CommitWithWorkingSet(ctx, headRef, workingSet.Ref(), pendingCommit, workingSet, prevHash, &datas.WorkingSetMeta{
		Name:  name,
		Email: email,
	}, nil)
	return err
}

// generateUnusedTag generates a new tag that is not already in use in the destination branch and has not been
// previously generated
func generateUnusedTag(destTagMap tagMapping, tag uint64) uint64 {
	for i := tag; i < tag+10000; i++ {
		_, destTagMapExists := destTagMap[i]
		if !destTagMapExists {
			return i
		}
	}
	panic("unable to generate a unique tag")
}

// updateRootWithNewColumnTag creates a new RootValue, based off of |root|, where |tableName|'s column |columnName| is
// updated to the specified |tag|. If the table or column does not exist in |root|, then the update is skipped – i.e.
// |root| is returned unchanged, with no error reported.
func updateRootWithNewColumnTag(ctx context.Context, root doltdb.RootValue, tableName string, columnName string, tag uint64) (doltdb.RootValue, error) {
	table, ok, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
	if err != nil {
		return nil, err
	}
	if !ok {
		// If the table doesn't exist on the target branch, just skip it
		return root, nil
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if _, ok := sch.GetAllCols().GetByName(columnName); !ok {
		// If a matching column doesn't exist on the target table, just skip it
		return root, nil
	}

	// Update the column tag in the schema
	updatedSchema, err := updateColumnTag(sch, columnName, tag)
	if err != nil {
		return nil, err
	}

	// Push the updated table schema to a new root
	return updateTableSchema(ctx, root, tableName, updatedSchema)
}

// updateTableSchema returns a new RootValue, based on |root|, where |tableName| has been updated to have schema |sch|.
func updateTableSchema(ctx context.Context, root doltdb.RootValue, tableName string, sch schema.Schema) (doltdb.RootValue, error) {
	// Put the updated table schemas
	tbl, ok, err := root.GetTable(ctx, doltdb.TableName{Name: tableName})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	tbl, err = tbl.UpdateSchema(ctx, sch)
	if err != nil {
		return nil, err
	}

	return root.PutTable(ctx, doltdb.TableName{Name: tableName}, tbl)
}

// tagMapping is a map of column tags to the table and column name that uses that tag.
type tagMapping map[uint64]tableAndColumn

// tableAndColumn
type tableAndColumn struct {
	table, column string
}

// Add adds an entry to this tagMapping for the specified |tag|, |table|, and |column|.
func (tm tagMapping) Add(tag uint64, table, column string) {
	tm[tag] = tableAndColumn{table, column}
}

// getAllTagsForRoots returns a tagMapping of all the tags in the specified |roots|.
func getAllTagsForRoots(ctx context.Context, roots ...doltdb.RootValue) (tags tagMapping, err error) {
	tags = make(tagMapping)
	for _, root := range roots {
		if root == nil {
			continue
		}
		err = root.IterTables(ctx, func(tblName doltdb.TableName, _ *doltdb.Table, sch schema.Schema) (stop bool, err error) {
			sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
				tags.Add(col.Tag, tblName.Name, col.Name)
				return false, nil
			})
			return false, nil
		})
		if err != nil {
			break
		}
	}
	return
}
