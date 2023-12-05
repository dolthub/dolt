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
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/google/go-github/v57/github"
)

const (
	featureVersionFlag = "feature"
	verboseFlag        = "verbose"
)

var versionDocs = cli.CommandDocumentationContent{
	ShortDesc: "Displays the version for the Dolt binary.",
	LongDesc:  `Displays the version for the Dolt binary.`,
	Synopsis: []string{
		`[--verbose] [--feature]`,
	},
}

type VersionCmd struct {
	VersionStr string
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd VersionCmd) Name() string {
	return "version"
}

// Description returns a description of the command
func (cmd VersionCmd) Description() string {
	return versionDocs.ShortDesc
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd VersionCmd) RequiresRepo() bool {
	return false
}

func (cmd VersionCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(versionDocs, ap)
}

func (cmd VersionCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag(featureVersionFlag, "f", "display the feature version of this repository.")
	ap.SupportsFlag(verboseFlag, "v", "display verbose details, including the storage format of this repository.")
	return ap
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd VersionCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, versionDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	cli.Println("dolt version", cmd.VersionStr)

	var verr errhand.VerboseError
	// out of date check
	client := github.NewClient(nil)
	release, resp, err := client.Repositories.GetLatestRelease(ctx, "dolthub", "dolt")
	if err != nil || resp.StatusCode != 200 {
		verr = errhand.BuildDError("error: failed to get latest release").AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usage)
	}
	releaseName := strings.TrimPrefix(*release.TagName, "v")
	if cmd.VersionStr != releaseName {
		cli.Printf("Warning: you are on an old version of Dolt. The newest version is %s.\n", releaseName)
	}

	if apr.Contains(verboseFlag) {
		if dEnv.HasDoltDir() && dEnv.RSLoadErr == nil && !cli.CheckEnvIsValid(dEnv) {
			return 2
		} else if dEnv.HasDoltDir() && dEnv.RSLoadErr == nil {
			nbf := dEnv.DoltDB.Format()
			cli.Printf("database storage format: %s\n", dfunctions.GetStorageFormatDisplayString(nbf))
		}
	}

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
