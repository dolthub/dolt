package commands

import (
	"flag"
	"fmt"
	"github.com/attic-labs/noms/go/hash"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"os"
	"strings"
)

type commitLoggerFunc func(*doltdb.CommitMeta, hash.Hash)

func logToStdOutFunc(cm *doltdb.CommitMeta, ch hash.Hash) {
	fmt.Println(color.YellowString("commit %s", ch.String()))

	printAuthor(cm)
	printDate(cm)
	printDesc(cm)
}

func printAuthor(cm *doltdb.CommitMeta) {
	fmt.Printf("Author: %s <%s>\n", cm.Name, cm.Email)
}

func printDate(cm *doltdb.CommitMeta) {
	timeStr := cm.FormatTS()
	fmt.Println("Date:  ", timeStr)
}

func printDesc(cm *doltdb.CommitMeta) {
	formattedDesc := "\n\t" + strings.Replace(cm.Description, "\n", "\n\t", -1) + "\n"
	fmt.Println(formattedDesc)
}

func Log(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	return logWithLoggerFunc(commandStr, args, cliEnv, logToStdOutFunc)
}

func logUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func logWithLoggerFunc(commandStr string, args []string, cliEnv *env.DoltCLIEnv, loggerFunc commitLoggerFunc) int {
	cwb := cliEnv.RepoState.CWBHeadSpec()
	commit, err := cliEnv.DoltDB.Resolve(cwb)

	if err != nil {
		fmt.Fprintln(os.Stderr, color.HiRedString("Fatal error: cannot get HEAD commit for current branch."))
		return 1
	}

	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = initUsage(fs)

	n := fs.Int("n", 30, "Number of commits to print. -1 To print all commits")

	fs.Parse(args)

	err = logCommit(cliEnv.DoltDB, commit, n, loggerFunc)

	if err != nil {
		fmt.Fprintln(os.Stderr, "Error printing commit.")
		return 1
	}

	return 0
}

func logCommit(ddb *doltdb.DoltDB, commit *doltdb.Commit, n *int, loggerFunc commitLoggerFunc) error {
	hash := commit.HashOf()
	cm := commit.GetCommitMeta()
	loggerFunc(cm, hash)

	if *n != -1 {
		*n = *n - 1
	}

	numParents := commit.NumParents()
	for i := 0; i < numParents && (*n == -1 || *n > 0); i++ {
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
