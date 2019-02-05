package actions

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/creds"
)

func NewCredsFile(dEnv *env.DoltEnv) (string, creds.DoltCreds, errhand.VerboseError) {
	credsDir, verr := EnsureCredsDir(dEnv)
	if verr != nil {
		return "", creds.EmptyCreds, verr
	}

	dCreds, verr := GenCredsWithVErr()

	if verr != nil {
		return "", creds.EmptyCreds, verr
	}

	credsPath, err := creds.JWKCredsWriteToDir(dEnv.FS, credsDir, dCreds)

	if err != nil {
		return "", creds.EmptyCreds, errhand.BuildDError("failed to create new key.").AddCause(err).Build()
	}

	cli.Println("Credentials created successfully.")
	cli.Println("pub key:", dCreds.PubKeyBase32Str())

	return credsPath, dCreds, verr
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
