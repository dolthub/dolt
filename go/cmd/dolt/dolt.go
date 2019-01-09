package main

import (
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/tblcmds"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"os"
)

const (
	Version = "0.2.1"
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
	{Name: "branch", Desc: "Create, list, edit, delete Branches.", Func: commands.Branch, ReqRepo: true},
	{Name: "checkout", Desc: "Checkout a branch or overwrite a table from HEAD.", Func: commands.Checkout, ReqRepo: true},
	{Name: "table", Desc: "Commands for creating, reading, updating, and deleting tables.", Func: tblcmds.Commands, ReqRepo: false},
})

func main() {
	os.Chdir("/Users/brian/dolt_test2/")
	resetIO := cli.InitIO()
	defer resetIO()

	dEnv := env.Load(env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB)

	if dEnv.CfgLoadErr != nil {
		cli.PrintErrln(color.RedString("Failed to load the global config.", dEnv.CfgLoadErr))
		os.Exit(1)
	}

	res := doltCommand("dolt", os.Args[1:], dEnv)

	os.Exit(res)
}
