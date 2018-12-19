package commands

import (
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"os"
)

func initUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

// Init is used by the init command
func Init(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	if cliEnv.HasLDDir() {
		fmt.Fprintln(os.Stderr, color.RedString("This directory has already been initialized."))
		return 1
	} else if !cliEnv.IsCWDEmpty() {
		fmt.Fprintln(os.Stderr, color.RedString("init must be run on an empty directory"))
		return 1
	}

	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = initUsage(fs)

	name := fs.String("name", "", "The name used in commits to this repo. If not provided will be taken from \""+env.UserNameKey+"\" in the global config.")
	email := fs.String("email", "", "The email address used. If not provided will be taken from \""+env.UserEmailKey+"\" in the global config.")

	fs.Parse(args)

	name = cliEnv.Config.IfEmptyUseConfig(*name, env.UserNameKey)
	email = cliEnv.Config.IfEmptyUseConfig(*email, env.UserEmailKey)

	if *name == "" {
		fmt.Fprintln(os.Stderr,
			color.RedString("Could not determine %[1]s. "+
				"Use the init parameter -name \"FIRST LAST\" to set it for this repo, "+
				"or dolt config -global -set %[1]s:\"FIRST LAST\"", env.UserNameKey))
		return 1
	} else if *email == "" {
		fmt.Fprintln(os.Stderr,
			color.RedString("Could not determine %[1]s. "+
				"Use the init parameter -email \"EMAIL_ADDRESS\" to set it for this repo, "+
				"or dolt config -global -set %[1]s:\"EMAIL_ADDRESS\"", env.UserEmailKey))
		return 1
	}

	err := cliEnv.InitRepo(*name, *email)

	if err != nil {
		fmt.Fprintln(os.Stderr, color.RedString("Failed to initialize directory as a data repo. %s", err.Error()))
		return 1
	}

	fmt.Println(color.CyanString("Successfully initialized dolt data repository."))
	return 0
}
