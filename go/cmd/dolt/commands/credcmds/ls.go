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

package credcmds

import (
	"context"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

var lsDocs = cli.CommandDocumentationContent{
	ShortDesc: "List keypairs available for authenticating with doltremoteapi.",
	LongDesc: `Lists known public keys from keypairs for authenticating with doltremoteapi.

The currently selected keypair appears with a {{.EmphasisLeft}}*{{.EmphasisRight}} next to it.`,
	Synopsis: []string{"[-v | --verbose]"},
}

var lsVerbose = false

type LsCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd LsCmd) Name() string {
	return "ls"
}

// Description returns a description of the command
func (cmd LsCmd) Description() string {
	return lsDocs.ShortDesc
}

func (cmd LsCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(lsDocs, ap)
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd LsCmd) RequiresRepo() bool {
	return false
}

// EventType returns the type of the event to log
func (cmd LsCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CREDS_LS
}

func (cmd LsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag("verbose", "v", "Verbose output, including key id.")
	return ap
}

// Exec executes the command
func (cmd LsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, lsDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.Contains("verbose") {
		lsVerbose = true
	}

	credsDir, verr := actions.EnsureCredsDir(dEnv)

	if verr == nil {
		dEnv.FS.Iter(credsDir, false, getJWKHandler(dEnv))
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func getJWKHandler(dEnv *env.DoltEnv) func(string, int64, bool) bool {
	current, valid, _ := dEnv.UserDoltCreds()
	first := false
	return func(path string, size int64, isDir bool) (stop bool) {
		if strings.HasSuffix(path, creds.JWKFileExtension) {
			if !first {
				if lsVerbose {
					cli.Println("  public key (used on dolthub settings page)              key id (user.creds in dolt config)")
					cli.Println("  ----------------------------------------------------    ---------------------------------------------")
				}
			}
			first = true

			dc, err := creds.JWKCredsReadFromFile(dEnv.FS, path)

			if err == nil {
				str := dc.PubKeyBase32Str()
				if lsVerbose {
					str += "    " + dc.KeyIDBase32Str()
				}
				if valid && current.PubKeyBase32Str() == dc.PubKeyBase32Str() {
					cli.Println(color.GreenString("* " + str))
				} else {
					cli.Println("  " + str)
				}
			} else {
				cli.Println(color.RedString("Corrupted creds file: %s", path))
			}
		}
		return false
	}
}
