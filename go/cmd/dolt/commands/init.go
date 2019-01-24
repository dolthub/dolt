package commands

import (
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

const (
	emailParamName    = "email"
	usernameParamName = "name"
)

var initShortDesc = "Create an empty Dolt data repository"
var initLongDesc = `This command creates an empty Dolt data repository - basically a noms database in the local directory with the structure necessary for dolt.

Running dolt init will only succeed when run in a completely empty directory, and subsequent dolt init calls will fail`
var initSynopsis = []string{
	"[<options>] [<path>]",
}

// Init is used by the init command
func Init(commandStr string, args []string, dEnv *env.DoltEnv) int {
	if dEnv.HasDoltDir() {
		cli.PrintErrln(color.RedString("This directory has already been initialized."))
		return 1
	}

	ap := argparser.NewArgParser()
	ap.SupportsString(usernameParamName, "", "name", "The name used in commits to this repo. If not provided will be taken from \""+env.UserNameKey+"\" in the global config.")
	ap.SupportsString(emailParamName, "", "email", "The email address used. If not provided will be taken from \""+env.UserEmailKey+"\" in the global config.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, initShortDesc, initLongDesc, initSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	name, _ := apr.GetValue(usernameParamName)
	email, _ := apr.GetValue(emailParamName)
	name = dEnv.Config.IfEmptyUseConfig(name, env.UserNameKey)
	email = dEnv.Config.IfEmptyUseConfig(email, env.UserEmailKey)

	if name == "" {
		cli.PrintErrln(
			color.RedString("Could not determine %[1]s. "+
				"Use the init parameter -name \"FIRST LAST\" to set it for this repo, "+
				"or dolt config -global -set %[1]s \"FIRST LAST\"", env.UserNameKey))
		usage()
		return 1
	} else if email == "" {
		cli.PrintErrln(
			color.RedString("Could not determine %[1]s. "+
				"Use the init parameter -email \"EMAIL_ADDRESS\" to set it for this repo, "+
				"or dolt config -global -set %[1]s \"EMAIL_ADDRESS\"", env.UserEmailKey))
		usage()
		return 1
	}

	err := dEnv.InitRepo(name, email)

	if err != nil {
		cli.PrintErrln(color.RedString("Failed to initialize directory as a data repo. %s", err.Error()))
		return 1
	}

	cli.Println(color.CyanString("Successfully initialized dolt data repository."))
	return 0
}

func initRepoErrToVerr(err error) errhand.VerboseError {
	switch err {
	case nil:
		return nil

	case env.ErrPreexistingDoltDir:
		bdr := errhand.BuildDError("Unable to initialize the current directory.")
		bdr.AddDetails("Directory already initialized.")
		return bdr.Build()

	case doltdb.ErrNomsIO:
		return errhand.BuildDError("fatal: failed to write empty structure").Build()

	case env.ErrStateUpdate:
		return errhand.BuildDError("fatal: failed to write initial state").Build()

	default:
		return errhand.BuildDError("fatal: " + err.Error()).Build()
	}

}
