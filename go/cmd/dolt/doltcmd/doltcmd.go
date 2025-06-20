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

package doltcmd

import (
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/admin"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/ci"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/cnfcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/credcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/cvcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/docscmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/indexcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/schcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/tblcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
)

var dumpDocsCommand = &commands.DumpDocsCmd{}
var dumpZshCommand = &commands.GenZshCompCmd{}

var doltSubCommands = []cli.Command{
	commands.InitCmd{},
	commands.StatusCmd{},
	commands.AddCmd{},
	commands.DiffCmd{},
	commands.ResetCmd{},
	commands.CleanCmd{},
	commands.CommitCmd{},
	commands.SqlCmd{VersionStr: doltversion.Version},
	admin.Commands,
	sqlserver.SqlServerCmd{VersionStr: doltversion.Version},
	commands.LogCmd{},
	commands.ShowCmd{},
	commands.BranchCmd{},
	commands.CheckoutCmd{},
	commands.MergeCmd{},
	cnfcmds.Commands,
	commands.CherryPickCmd{},
	commands.RevertCmd{},
	commands.CloneCmd{},
	commands.FetchCmd{},
	commands.PullCmd{},
	commands.PushCmd{},
	commands.ConfigCmd{},
	commands.RemoteCmd{},
	commands.BackupCmd{},
	commands.LoginCmd{},
	credcmds.Commands,
	commands.LsCmd{},
	schcmds.Commands,
	tblcmds.Commands,
	commands.TagCmd{},
	commands.BlameCmd{},
	cvcmds.Commands,
	commands.SendMetricsCmd{},
	commands.MigrateCmd{},
	indexcmds.Commands,
	commands.ReadTablesCmd{},
	commands.GarbageCollectionCmd{},
	commands.FsckCmd{},
	commands.FilterBranchCmd{},
	commands.MergeBaseCmd{},
	commands.RootsCmd{},
	commands.VersionCmd{VersionStr: doltversion.Version},
	commands.DumpCmd{},
	commands.InspectCmd{},
	dumpDocsCommand,
	dumpZshCommand,
	docscmds.Commands,
	commands.StashCmd{},
	&commands.Assist{},
	commands.ProfileCmd{},
	commands.QueryDiff{},
	commands.ReflogCmd{},
	commands.RebaseCmd{},
	commands.ArchiveCmd{},
	ci.Commands,
	commands.DebugCmd{},
}

var DoltCommand = cli.NewSubCommandHandler("dolt", "it's git for data", doltSubCommands)

var globalArgParser = cli.CreateGlobalArgParser("dolt")

var doc = cli.CommandDocumentationContent{
	ShortDesc: "Dolt is git for data",
	LongDesc:  `Dolt comprises of multiple subcommands that allow users to import, export, update, and manipulate data with SQL.`,

	Synopsis: []string{
		"[global flags] subcommand [subcommand arguments]",
	},
}
var globalDocs = cli.CommandDocsForCommandString("dolt", doc, globalArgParser)

var globalSpecialMsg = `
Dolt subcommands are in transition to using the flags listed below as global flags.
Not all subcommands use these flags. If your command accepts these flags without error, then they are supported.
`

func init() {
	dumpDocsCommand.DoltCommand = DoltCommand
	dumpDocsCommand.GlobalDocs = globalDocs
	dumpDocsCommand.GlobalSpecialMsg = globalSpecialMsg
	dumpZshCommand.DoltCommand = DoltCommand
}
