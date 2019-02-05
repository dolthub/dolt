package credcmds

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var newShortDesc = ""
var newLongDesc = ""
var newSynopsis = []string{}

func New(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, newShortDesc, newLongDesc, newSynopsis, ap)
	cli.ParseArgs(ap, args, help)

	_, _, verr := actions.NewCredsFile(dEnv)

	return commands.HandleVErrAndExitCode(verr, usage)
}
