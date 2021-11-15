// Copyright 2019 Dolthub, Inc.
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

package commands

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const (
	featureVersionFlag = "feature"
)

type VersionCmd struct {
	VersionStr string
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd VersionCmd) Name() string {
	return "version"
}

// Description returns a description of the command
func (cmd VersionCmd) Description() string {
	return "Displays the current Dolt cli version."
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd VersionCmd) RequiresRepo() bool {
	return false
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd VersionCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	return nil
}

func (cmd VersionCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(featureVersionFlag, "f", "query the feature version of this repository.")
	return ap
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd VersionCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	cli.Println("dolt version", cmd.VersionStr)

	usage := func() {}
	ap := cmd.ArgParser()
	apr := cli.ParseArgsOrDie(ap, args, usage)

	var verr errhand.VerboseError
	if apr.Contains(featureVersionFlag) {
		if !cli.CheckEnvIsValid(dEnv) {
			return 2
		}
		wr, err := dEnv.WorkingRoot(ctx)
		if err != nil {
			verr = errhand.BuildDError("could not read working root").AddCause(err).Build()
			return HandleVErrAndExitCode(verr, usage)
		}

		fv, ok, err := wr.GetFeatureVersion(ctx)
		if err != nil {
			verr = errhand.BuildDError("error reading feature version").AddCause(err).Build()
		} else if !ok {
			verr = errhand.BuildDError("the current head does not have a feature version").Build()
		} else {
			cli.Println("feature version:", fv)
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}
