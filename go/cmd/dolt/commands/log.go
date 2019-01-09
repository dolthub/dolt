package commands

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"strings"
)

const (
	numLinesParam = "number"
)

var logShortDesc = `Show commit logs`
var logLongDesc = ``

var logSynopsis = []string{
	"[-n <num_lines>] [<commit>]",
}

type commitLoggerFunc func(*doltdb.CommitMeta, hash.Hash)

func logToStdOutFunc(cm *doltdb.CommitMeta, ch hash.Hash) {
	cli.Println(color.YellowString("commit %s", ch.String()))

	printAuthor(cm)
	printDate(cm)
	printDesc(cm)
}

func printAuthor(cm *doltdb.CommitMeta) {
	cli.Printf("Author: %s <%s>\n", cm.Name, cm.Email)
}

func printDate(cm *doltdb.CommitMeta) {
	timeStr := cm.FormatTS()
	cli.Println("Date:  ", timeStr)
}

func printDesc(cm *doltdb.CommitMeta) {
	formattedDesc := "\n\t" + strings.Replace(cm.Description, "\n", "\n\t", -1) + "\n"
	cli.Println(formattedDesc)
}

func Log(commandStr string, args []string, dEnv *env.DoltEnv) int {
	return logWithLoggerFunc(commandStr, args, dEnv, logToStdOutFunc)
}

func logWithLoggerFunc(commandStr string, args []string, dEnv *env.DoltEnv, loggerFunc commitLoggerFunc) int {
	ap := argparser.NewArgParser()
	ap.SupportsInt(numLinesParam, "n", "num_lines", "Limit the number of commits to output")
	help, usage := cli.HelpAndUsagePrinters(commandStr, logShortDesc, logLongDesc, logSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	var cs *doltdb.CommitSpec
	if apr.NArg() == 0 {
		cs = dEnv.RepoState.CWBHeadSpec()
	} else if apr.NArg() == 1 {
		var err error
		comSpecStr := apr.Arg(0)
		cs, err = doltdb.NewCommitSpec(comSpecStr, dEnv.RepoState.Branch)

		if err != nil {
			cli.PrintErrf("Invalid commit %s\n", comSpecStr)
			return 1
		}
	} else {
		cli.PrintErrln("Invalid usage")
		usage()
		return 1
	}

	commit, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: cannot get HEAD commit for current branch."))
		return 1
	}

	n := apr.GetIntOrDefault(numLinesParam, -1)
	err = logCommit(dEnv.DoltDB, commit, n, loggerFunc)

	if err != nil {
		cli.PrintErrln("Error printing commit.")
		return 1
	}

	return 0
}

func logCommit(ddb *doltdb.DoltDB, commit *doltdb.Commit, n int, loggerFunc commitLoggerFunc) error {
	hash := commit.HashOf()
	cm := commit.GetCommitMeta()
	loggerFunc(cm, hash)

	if n != -1 {
		n = n - 1
	}

	numParents := commit.NumParents()
	for i := 0; i < numParents && (n == -1 || n > 0); i++ {
		parentCommit, err := ddb.ResolveParent(commit, i)

		if err != nil {
			return err
		}

		err = logCommit(ddb, parentCommit, n, loggerFunc)

		if err != nil {
			return err
		}

		return err
	}

	return nil
}
