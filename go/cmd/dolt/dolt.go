package main

import (
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/edit"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"os"
)

const (
	Version = "0.2"
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
	{Name: "rm", Desc: "Remove tables from the working set.", Func: commands.Rm, ReqRepo: true},
	{Name: "ls", Desc: "List tables in the working set.", Func: commands.Ls, ReqRepo: true},
	{Name: "show", Desc: "Show a table.", Func: commands.Show, ReqRepo: true},
	{Name: "diff", Desc: "Diff a table.", Func: commands.Diff, ReqRepo: true},
	{Name: "edit", Desc: "Create allows editing of tables.", Func: edit.Commands, ReqRepo: false},
})

func main() {
	cliEnv := env.Load(env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB)

	if cliEnv.CfgLoadErr != nil {
		fmt.Fprintln(os.Stderr, color.RedString("Failed to load the global config.", cliEnv.CfgLoadErr))
		os.Exit(1)
	}

	flag.Parse()
	res := doltCommand("dolt", flag.Args(), cliEnv)

	fmt.Println()
	os.Exit(res)
}
