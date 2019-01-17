package commands

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"sort"
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
	commits, err := timeSortedCommits(dEnv.DoltDB, commit, n)

	if err != nil {
		cli.PrintErrln("Error retrieving commit.")
		return 1
	}

	for _, comm := range commits {
		loggerFunc(comm.GetCommitMeta(), comm.ParentHashes(), comm.HashOf())
	}

	return 0
}

func timeSortedCommits(ddb *doltdb.DoltDB, commit *doltdb.Commit, n int) ([]*doltdb.Commit, error) {
	hashToCommit := make(map[hash.Hash]*doltdb.Commit)
	err := addCommits(ddb, commit, hashToCommit, n)

	if err != nil {
		return nil, err
	}

	idx := 0
	uniqueCommits := make([]*doltdb.Commit, len(hashToCommit))
	for _, v := range hashToCommit {
		uniqueCommits[idx] = v
		idx++
	}

	sort.Slice(uniqueCommits, func(i, j int) bool {
		return uniqueCommits[i].GetCommitMeta().Timestamp > uniqueCommits[j].GetCommitMeta().Timestamp
	})

	return uniqueCommits, nil
}

func addCommits(ddb *doltdb.DoltDB, commit *doltdb.Commit, hashToCommit map[hash.Hash]*doltdb.Commit, n int) error {
	hash := commit.HashOf()
	hashToCommit[hash] = commit

	numParents := commit.NumParents()
	for i := 0; i < numParents && len(hashToCommit) != n; i++ {
		parentCommit, err := ddb.ResolveParent(commit, i)

		if err != nil {
			return err
		}

		err = addCommits(ddb, parentCommit, hashToCommit, n)

		if err != nil {
			return err
		}
	}

	return nil
}
