package main

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/credcmds"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/sqle"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/pkg/profile"
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
	Version = "0.9.5"
)

var doltCommand = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "init", Desc: "Create an empty Dolt data repository.", Func: commands.Init, ReqRepo: false},
	{Name: "status", Desc: "Show the working tree status.", Func: commands.Status, ReqRepo: true},
	{Name: "add", Desc: "Add table changes to the list of staged table changes.", Func: commands.Add, ReqRepo: true},
	{Name: "reset", Desc: "Remove table changes from the list of staged table changes.", Func: commands.Reset, ReqRepo: true},
	{Name: "commit", Desc: "Record changes to the repository.", Func: commands.Commit, ReqRepo: true},
	{Name: "sql", Desc: "Run a SQL query against tables in repository.", Func: commands.Sql, ReqRepo: true},
	{Name: "sqle", Desc: "Run a SQL query against tables in repository.", Func: sqle.Sql, ReqRepo: true, HideFromHelp: true},
	{Name: "sql-server", Desc: "Starts a MySQL-compatible server.", Func: sqlserver.SqlServer, ReqRepo: true},
	{Name: "log", Desc: "Show commit logs.", Func: commands.Log, ReqRepo: true},
	{Name: "diff", Desc: "Diff a table.", Func: commands.Diff, ReqRepo: true},
	{Name: "merge", Desc: "Merge a branch.", Func: commands.Merge, ReqRepo: true},
	{Name: "branch", Desc: "Create, list, edit, delete branches.", Func: commands.Branch, ReqRepo: true},
	{Name: "checkout", Desc: "Checkout a branch or overwrite a table from HEAD.", Func: commands.Checkout, ReqRepo: true},
	{Name: "remote", Desc: "Manage set of tracked repositories.", Func: commands.Remote, ReqRepo: true},
	{Name: "push", Desc: "Push to a dolt remote.", Func: commands.Push, ReqRepo: true},
	{Name: "pull", Desc: "Fetch from a dolt remote data repository and merge.", Func: commands.Pull, ReqRepo: true},
	{Name: "fetch", Desc: "Update the database from a remote data repository.", Func: commands.Fetch, ReqRepo: true},
	{Name: "clone", Desc: "Clone from a remote data repository.", Func: commands.Clone, ReqRepo: false},
	{Name: "creds", Desc: "Commands for managing credentials.", Func: credcmds.Commands, ReqRepo: false},
	{Name: "login", Desc: "Login to a dolt remote host.", Func: commands.Login, ReqRepo: false},
	{Name: "version", Desc: "Displays the current Dolt cli version.", Func: commands.Version(Version), ReqRepo: false},
	{Name: "config", Desc: "Dolt configuration.", Func: commands.Config, ReqRepo: false},
	{Name: "ls", Desc: "List tables in the working set.", Func: commands.Ls, ReqRepo: true},
	{Name: "schema", Desc: "Display the schema for table(s)", Func: commands.Schema, ReqRepo: true},
	{Name: "table", Desc: "Commands for creating, reading, updating, and deleting tables.", Func: tblcmds.Commands, ReqRepo: false},
	{Name: "conflicts", Desc: "Commands for viewing and resolving merge conflicts.", Func: cnfcmds.Commands, ReqRepo: false},
})

var cpuProf = false
var memProf = false

func main() {
	os.Exit(runMain())
}

func runMain() int {
	if cpuProf {
		fmt.Println("cpu profiling enabled.")
		defer profile.Start(profile.CPUProfile).Stop()
	}

	if memProf {
		fmt.Println("mem profiling enabled.")
		defer profile.Start(profile.MemProfile).Stop()
	}

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

	warnIfMaxFilesTooLow()

	dEnv := env.Load(context.TODO(), env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB)

	if dEnv.CfgLoadErr != nil {
		cli.PrintErrln(color.RedString("Failed to load the global config.", dEnv.CfgLoadErr))
		return 1
	}

	return doltCommand("dolt", args, dEnv)
}
