// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actions

import (
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
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
