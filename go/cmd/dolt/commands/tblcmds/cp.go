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

package tblcmds

import (
	"context"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands/schcmds"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/mvdata"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

var tblCpDocs = cli.CommandDocumentationContent{
	ShortDesc: "Makes a copy of a table",
	LongDesc: `The dolt table cp command makes a copy of a table at a given commit. If a commit is not specified the copy is made of the table from the current working set.

If a table exists at the target location this command will fail unless the {{.EmphasisLeft}}--force|-f{{.EmphasisRight}} flag is provided.  In this case the table at the target location will be overwritten with the copied table.

All changes will be applied to the working tables and will need to be staged using {{.EmphasisLeft}}dolt add{{.EmphasisRight}} and committed using {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}.
`,
	Synopsis: []string{
		"[-f] [{{.LessThan}}commit{{.GreaterThan}}] {{.LessThan}}oldtable{{.GreaterThan}} {{.LessThan}}newtable{{.GreaterThan}}",
	},
}

type copyOptions struct {
	oldTblName string
	newTblName string
	contOnErr  bool
	force      bool
	Src        mvdata.DataLocation
	Dest       mvdata.DataLocation
}

func (co copyOptions) checkOverwrite(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	if co.force {
		return false, nil
	}
	return co.Dest.Exists(ctx, root, fs)
}

func (co copyOptions) WritesToTable() bool {
	return true
}

func (co copyOptions) SrcName() string {
	return co.oldTblName
}

func (co copyOptions) DestName() string {
	return co.newTblName
}

type CpCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CpCmd) Name() string {
	return "cp"
}

// Description returns a description of the command
func (cmd CpCmd) Description() string {
	return "Copies a table"
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd CpCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, tblCpDocs, ap))
}

func (cmd CpCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"commit", "The state at which point the table will be copied."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"oldtable", "The table being copied."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"newtable", "The destination where the table is being copied to."})
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the force flag will allow the target to be overwritten.")
	return ap
}

// EventType returns the type of the event to log
func (cmd CpCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_CP
}

// Exec executes the command
func (cmd CpCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, tblCpDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() < 2 || apr.NArg() > 3 {
		usage()
		return 1
	}

	force := apr.Contains(forceParam)
	working, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	root := working

	var oldTbl, newTbl string
	if apr.NArg() == 3 {
		var cm *doltdb.Commit
		cm, verr = commands.ResolveCommitWithVErr(dEnv, apr.Arg(0), dEnv.RepoState.CWBHeadRef().String())
		if verr != nil {
			return commands.HandleVErrAndExitCode(verr, usage)
		}
		var err error
		root, err = cm.GetRootValue()

		if err != nil {
			verr = errhand.BuildDError("error: failed to get root value").AddCause(err).Build()
			return commands.HandleVErrAndExitCode(verr, usage)
		}

		oldTbl, newTbl = apr.Arg(1), apr.Arg(2)
	} else {
		oldTbl, newTbl = apr.Arg(0), apr.Arg(1)
	}

	if err := schcmds.ValidateTableNameForCreate(newTbl); err != nil {
		return commands.HandleVErrAndExitCode(err, usage)
	}

	_, ok, err := root.GetTable(ctx, oldTbl)

	if err != nil {
		verr = errhand.BuildDError("error: failed to get table").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}
	if !ok {
		verr = errhand.BuildDError("Table '%s' not found in root", oldTbl).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	has, err := working.HasTable(ctx, newTbl)

	if err != nil {
		verr = errhand.BuildDError("error: failed to get tables").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	} else if !force && has {
		verr = errhand.BuildDError("Data already exists in '%s'.  Use -f to overwrite.", newTbl).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	cpOpts := copyOptions{
		oldTblName: oldTbl,
		newTblName: newTbl,
		contOnErr:  true,
		force:      force,
		Src:        mvdata.TableDataLocation{Name: oldTbl},
		Dest:       mvdata.TableDataLocation{Name: newTbl},
	}

	mover, verr := newTableCopyDataMover(ctx, root, dEnv.FS, cpOpts, importStatsCB)

	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	skipped, verr := mvdata.MoveData(ctx, dEnv, mover, cpOpts)

	if skipped > 0 {
		cli.PrintErrln(color.YellowString("Lines skipped: %d", skipped))
	}
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	verr = buildNewIndexes(ctx, dEnv, newTbl)

	return commands.HandleVErrAndExitCode(verr, usage)
}

func newTableCopyDataMover(ctx context.Context, root *doltdb.RootValue, fs filesys.Filesys, co copyOptions, statsCB noms.StatsCB) (*mvdata.DataMover, errhand.VerboseError) {
	var rd table.TableReadCloser
	var err error

	ow, err := co.checkOverwrite(ctx, root, fs)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}
	if ow {
		return nil, errhand.BuildDError("%s already exists. Use -f to overwrite.", co.DestName()).Build()
	}

	rd, srcIsSorted, err := co.Src.NewReader(ctx, root, fs, nil)

	if err != nil {
		return nil, errhand.BuildDError("Error creating reader for %s.", co.newTblName).AddCause(err).Build()
	}

	defer func() {
		if rd != nil {
			rd.Close(ctx)
		}
	}()

	oldTblSch := rd.GetSchema()
	cc, err := root.GenerateTagsForNewColColl(ctx, co.newTblName, oldTblSch.GetAllCols())

	if err != nil {
		return nil, errhand.BuildDError("Error create schema for new table %s", co.newTblName).AddCause(err).Build()
	}

	newTblSch := schema.SchemaFromCols(cc)
	newTblSch.Indexes().Merge(oldTblSch.Indexes().AllIndexes()...)

	transforms, err := mvdata.NameMapTransform(oldTblSch, newTblSch, make(rowconv.NameMapper))

	if err != nil {
		return nil, errhand.BuildDError("Error determining the mapping from input fields to output fields.").AddDetails(
			"When attempting to move data from %s to %s, determine the mapping from input fields to output fields.", co.SrcName(), co.DestName()).AddCause(err).Build()
	}

	wr, err := co.Dest.NewCreatingWriter(ctx, co, root, fs, srcIsSorted, newTblSch, statsCB)

	if err != nil {
		return nil, errhand.BuildDError("Could not create table writer for %s", co.newTblName).AddCause(err).Build()
	}

	imp := &mvdata.DataMover{rd, transforms, wr, co.contOnErr}
	rd = nil

	return imp, nil
}

func buildNewIndexes(ctx context.Context, dEnv *env.DoltEnv, newTblName string) errhand.VerboseError {
	//TODO: change this to not use the executeImport function, and instead the SQL code path
	newWorking, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return errhand.BuildDError("Unable to load the working set to build the indexes.").AddCause(err).Build()
	}
	updatedTable, ok, err := newWorking.GetTable(ctx, newTblName)
	if err != nil {
		return errhand.BuildDError("Unable to load the table to build the indexes.").AddCause(err).Build()
	} else if !ok {
		return errhand.BuildDError("Unable to find the table to build the indexes.").Build()
	}
	updatedTable, err = updatedTable.RebuildIndexData(ctx)
	if err != nil {
		return errhand.BuildDError("Unable to build the indexes.").AddCause(err).Build()
	}
	newWorking, err = newWorking.PutTable(ctx, newTblName, updatedTable)
	if err != nil {
		return errhand.BuildDError("Unable to write the indexes to the working set.").AddCause(err).Build()
	}
	err = dEnv.UpdateWorkingRoot(ctx, newWorking)
	if err != nil {
		return errhand.BuildDError("Unable to update the working set containing the indexes.").AddCause(err).Build()
	}
	return nil
}
