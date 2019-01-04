package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
)

var mergeShortDest = "Join two or more development histories together"
var mergeLongDesc = ``
var mergeSynopsis = []string{
	"[<commit>...]",
}

func Merge(commandStr string, args []string, dEnv *env.DoltEnv) int {
	/*ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, mergeShortDest, mergeLongDesc, mergeSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "invalid usage")
		usage()
	}

	cm, verr := ResolveCommitWithVErr(dEnv, apr.Arg(0), dEnv.RepoState.Branch)

	if verr == nil {

	}

	return handleCommitErr(verr, usage)*/
	return 0
}
