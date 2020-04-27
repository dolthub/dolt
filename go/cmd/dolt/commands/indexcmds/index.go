// Copyright 2020 Liquidata, Inc.
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

package indexcmds

import (
	"strings"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
)

const (
	IndexCmdWarning = "All dolt index commands are intended for developers only. Usage of these commands are not recommended.\n"
)

var Commands = cli.NewHiddenSubCommandHandler("index", "Internal debugging commands for showing and modifying table indexes.", []cli.Command{
	CatCmd{},
	LsCmd{},
	RebuildCmd{},
})

func HandleErr(verr errhand.VerboseError, usage cli.UsagePrinter) int {
	if verr != nil {
		if msg := verr.Verbose(); strings.TrimSpace(msg) != "" {
			cli.PrintErrln(msg)
		}

		if usage != nil {
			usage()
		}

		return 1
	}

	return 0
}
