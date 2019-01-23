package main

import (
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/cnfcmds"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/tblcmds"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
)

const (
	Version = "0.3.0"
)

var doltCommand = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "version", Desc: "Displays the current Dolt cli version", Func: commands.Version(Version), ReqRepo: false},
	{Name: "config", Desc: "Dolt configuration.", Func: commands.Config, ReqRepo: false},
	{Name: "init", Desc: "Create an empty Dolt data repository.", Func: commands.Init, ReqRepo: false},
	{Name: "status", Desc: "Show the working tree status.", Func: commands.Status, ReqRepo: true},
	{Name: "add", Desc: "Add table changes to the list of staged table changes.", Func: commands.Add, ReqRepo: true},
	{Name: "reset", Desc: "Remove table changes from the list of staged table changes.", Func: commands.Reset, ReqRepo: true},
	{Name: "commit", Desc: "Record changes to the repository", Func: commands.Commit, ReqRepo: true},
	{Name: "log", Desc: "Show commit logs", Func: commands.Log, ReqRepo: true},
	{Name: "ls", Desc: "List tables in the working set.", Func: commands.Ls, ReqRepo: true},
	{Name: "diff", Desc: "Diff a table.", Func: commands.Diff, ReqRepo: true},
	{Name: "merge", Desc: "Merge a branch", Func: commands.Merge, ReqRepo: true},
	{Name: "branch", Desc: "Create, list, edit, delete Branches.", Func: commands.Branch, ReqRepo: true},
	{Name: "checkout", Desc: "Checkout a branch or overwrite a table from HEAD.", Func: commands.Checkout, ReqRepo: true},
	{Name: "table", Desc: "Commands for creating, reading, updating, and deleting tables.", Func: tblcmds.Commands, ReqRepo: false},
	{Name: "conflicts", Desc: "Commands for viewing and resolving merge conflicts", Func: cnfcmds.Commands, ReqRepo: false},
})

func main() {

	args := os.Args[1:]
	// Currently goland doesn't support running with a different working directory when using go modules.
	// This is a hack that allows a different working directory to be set after the application starts using
	// chdir=<DIR>.  The syntax is not flexible and must match exactly this.
	if len(args) > 0 && strings.HasPrefix(strings.ToLower(args[0]), "chdir=") {
		dir := args[0][6:]
		err := os.Chdir(dir)

		if err != nil {
			panic(err)
		}

		args = args[1:]
	}

	restoreIO := cli.InitIO()
	defer restoreIO()

	dEnv := env.Load(env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB)

	if dEnv.CfgLoadErr != nil {
		cli.PrintErrln(color.RedString("Failed to load the global config.", dEnv.CfgLoadErr))
		os.Exit(1)
	}

	res := doltCommand("dolt", args, dEnv)

	os.Exit(res)
}
