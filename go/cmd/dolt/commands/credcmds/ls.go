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
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/creds"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"strings"
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
