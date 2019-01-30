package credcmds

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/creds"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var newShortDesc = ""
var newLongDesc = ""
var newSynopsis = []string{}

func New(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, newShortDesc, newLongDesc, newSynopsis, ap)
	cli.ParseArgs(ap, args, help)

	credsDir, verr := EnsureCredsDir(dEnv)

	if verr == nil {
		var dCreds creds.DoltCreds
		dCreds, verr = GenCredsWithVErr()

		if verr == nil {
			x, err := creds.JWKCredsWriteToDir(dEnv.FS, credsDir, dCreds)
			cli.Println(x)

			if err != nil {
				verr = errhand.BuildDError("failed to create new key.").AddCause(err).Build()
			} else {
				cli.Println("Credentials created successfully.")
				cli.Println("pub key:", dCreds.PubKeyBase32Str())
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func EnsureCredsDir(dEnv *env.DoltEnv) (string, errhand.VerboseError) {
	credsPath, err := dEnv.CredsDir()
	if err != nil {
		return "", errhand.BuildDError("fatal: could not determine credentials dir").AddCause(err).Build()
	}

	err = dEnv.FS.MkDirs(credsPath)

	if err != nil {
		return "", errhand.BuildDError("fatal: failed to create credentials dir.").AddCause(err).Build()
	}

	return credsPath, nil
}

func GenCredsWithVErr() (creds.DoltCreds, errhand.VerboseError) {
	dCreds, err := creds.GenerateCredentials()

	if err != nil {
		verr := errhand.BuildDError("").Build()
		return dCreds, verr
	}

	return dCreds, nil
}
