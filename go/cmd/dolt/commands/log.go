package commands

import (
	"context"
	"strings"

	"github.com/attic-labs/noms/go/hash"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

const (
	numLinesParam = "number"
)

var logShortDesc = `Show commit logs`
var logLongDesc = ``

var logSynopsis = []string{
	"[-n <num_lines>] [<commit>]",
}

type commitLoggerFunc func(*doltdb.CommitMeta, []hash.Hash, hash.Hash)

func logToStdOutFunc(cm *doltdb.CommitMeta, parentHashes []hash.Hash, ch hash.Hash) {
	cli.Println(color.YellowString("commit %s", ch.String()))

	if len(parentHashes) > 1 {
		printMerge(parentHashes)
	}

	printAuthor(cm)
	printDate(cm)
	printDesc(cm)
}

func printMerge(hashes []hash.Hash) {
	cli.Print("Merge:")
	for _, h := range hashes {
		cli.Print(" " + h.String())
	}
	cli.Println()
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
		cs, err = doltdb.NewCommitSpec(comSpecStr, dEnv.RepoState.Head.String())

		if err != nil {
			cli.PrintErrf("Invalid commit %s\n", comSpecStr)
			return 1
		}
	} else {
		usage()
		return 1
	}

	commit, err := dEnv.DoltDB.Resolve(context.TODO(), cs)

	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: cannot get HEAD commit for current branch."))
		return 1
	}

	n := apr.GetIntOrDefault(numLinesParam, -1)
	commits, err := actions.TimeSortedCommits(context.TODO(), dEnv.DoltDB, commit, n)

	if err != nil {
		cli.PrintErrln("Error retrieving commit.")
		return 1
	}

	for _, comm := range commits {
		loggerFunc(comm.GetCommitMeta(), comm.ParentHashes(context.TODO()), comm.HashOf())
	}

	return 0
}
