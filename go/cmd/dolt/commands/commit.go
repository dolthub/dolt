package commands

import (
	"flag"
	"fmt"
	"github.com/attic-labs/noms/go/hash"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/config"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"os"
)

func commitUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func Commit(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = commitUsage(fs)

	msg := fs.String("m", "", "The commit message")

	fs.Parse(args)

	if *msg == "" {
		fmt.Fprintln(os.Stderr, color.RedString("Missing required parameter -m"))
		fs.Usage()
		return 1
	}

	return processCommit(*msg, cliEnv)
}

func processCommit(msg string, cliEnv *env.DoltCLIEnv) int {
	name, email, verr := getNameAndEmail(cliEnv.Config)

	if verr == nil {
		verr = commitStaged(cliEnv, doltdb.NewCommitMeta(name, email, msg))
	}

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	fmt.Println(color.CyanString("Commit completed successfully."))
	return 0
}

func getNameAndEmail(cfg *env.DoltCliConfig) (string, string, errhand.VerboseError) {
	name, nmErr := cfg.GetString(env.UserNameKey)
	email, emErr := cfg.GetString(env.UserEmailKey)

	if nmErr == config.ErrConfigParamNotFound {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserNameKey)
		bdr.AddDetails("dolt config [-global|local] -set %[1]s:\"FIRST LAST\"", env.UserNameKey)
		return "", "", bdr.Build()
	} else if emErr == config.ErrConfigParamNotFound {
		bdr := errhand.BuildDError("Could not determine %s.", env.UserEmailKey)
		bdr.AddDetails("dolt config [-global|local] -set %[1]s:\"EMAIL_ADDRESS\"", env.UserEmailKey)
		return "", "", bdr.Build()
	}

	return name, email, nil
}

func commitStaged(cliEnv *env.DoltCLIEnv, meta *doltdb.CommitMeta) errhand.VerboseError {
	h := hash.Parse(cliEnv.RepoState.Staged)
	_, err := cliEnv.DoltDB.Commit(h, cliEnv.RepoState.Branch, meta)

	if err != nil {
		bdr := errhand.BuildDError("Unable to write commit.")
		bdr.AddCause(err)
		return bdr.Build()
	}

	return nil
}
