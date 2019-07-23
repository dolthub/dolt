package credcmds

import (
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/creds"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var lsShortDesc = ""
var lsLongDesc = ""
var lsSynopsis = []string{}

func Ls(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, lsShortDesc, lsLongDesc, lsSynopsis, ap)
	cli.ParseArgs(ap, args, help)

	credsDir, verr := actions.EnsureCredsDir(dEnv)

	if verr == nil {
		dEnv.FS.Iter(credsDir, false, getJWKHandler(dEnv))
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func getJWKHandler(dEnv *env.DoltEnv) func(string, int64, bool) bool {
	return func(path string, size int64, isDir bool) (stop bool) {
		if strings.HasSuffix(path, creds.JWKFileExtension) {
			dc, err := creds.JWKCredsReadFromFile(dEnv.FS, path)

			if err == nil {
				cli.Println(dc.PubKeyBase32Str())
			} else {
				cli.Println(color.RedString("Corrupted creds file: %s", path))
			}
		}

		return false
	}
}
