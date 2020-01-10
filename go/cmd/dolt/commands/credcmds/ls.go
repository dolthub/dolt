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

package credcmds

import (
	"context"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/creds"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var lsShortDesc = "List keypairs available for authenticating with doltremoteapi."
var lsLongDesc = `Lists known public keys from keypairs for authenticating with doltremoteapi.

The currently selected keypair appears with a '*' next to it.`
var lsSynopsis = []string{"[-v | --verbose]"}

var lsVerbose = false

func Ls(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag("verbose", "v", "Verbose output, including key id.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, lsShortDesc, lsLongDesc, lsSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

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
	current, valid, _ := dEnv.UserRPCCreds()
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
