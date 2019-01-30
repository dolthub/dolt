package credcmds

import (
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/creds"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"os"
	"path/filepath"
)

var rmShortDesc = ""
var rmLongDesc = ""
var rmSynopsis = []string{}

func Rm(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, rmShortDesc, rmLongDesc, rmSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	credsDir, verr := EnsureCredsDir(dEnv)

	if verr == nil {
		for _, arg := range args {
			deleted, err := deleteIfExists(dEnv.FS, credsDir, arg)

			if err != nil {
				cli.Println(color.YellowString("error: io error attempting to delete %s", arg))
				continue
			}

			if !deleted {
				maybeKid, err := creds.PubKeyStrToKID(arg)

				if err != nil {
					cli.Println(color.YellowString("error: %s is not a valid base 	32 string", arg))
				} else {
					deleted, err = deleteIfExists(dEnv.FS, credsDir, maybeKid)
					creds.GenerateCredentials()

					if err != nil {
						cli.Println(color.YellowString("error: io error attempting to delete %s", arg))
					} else if !deleted {
						cli.Println(color.YellowString("error: unknown key %s.", arg))
					}
				}
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func deleteIfExists(fs filesys.Filesys, dir, filename string) (bool, error) {
	absPath := filepath.Join(dir, filename+creds.JWKFileExtension)
	err := fs.DeleteFile(absPath)

	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}
