package credcmds

import (
	"github.com/fatih/color"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var rmShortDesc = ""
var rmLongDesc = ""
var rmSynopsis = []string{}

func Rm(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, rmShortDesc, rmLongDesc, rmSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	credsDir, verr := actions.EnsureCredsDir(dEnv)

	if verr == nil {
		for _, arg := range args {
			jwkFilePath, err := dEnv.FindCreds(credsDir, arg)

			if err == nil {
				err = dEnv.FS.DeleteFile(jwkFilePath)
			}

			if err != nil {
				cli.Println(color.YellowString("failed to delete %s: %s", arg, err.Error()))
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}
