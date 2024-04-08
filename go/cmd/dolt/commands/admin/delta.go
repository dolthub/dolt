// Copyright 2024 Dolthub, Inc.
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

package admin

import (
	"context"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
)

type DeltaCmd struct {
}

func (cmd DeltaCmd) Name() string {
	return "delta"
}

// Description returns a description of the command
func (cmd DeltaCmd) Description() string {
	return "Hidden command to kick the tires with the new archive format."
}
func (cmd DeltaCmd) RequiresRepo() bool {
	return true
}
func (cmd DeltaCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd DeltaCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	return ap
}
func (cmd DeltaCmd) Hidden() bool {
	return true
}

func (cmd DeltaCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {

	db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB)
	cs := datas.ChunkStoreFromDatabase(db)
	if _, ok := cs.(*nbs.GenerationalNBS); !ok {
		cli.PrintErrln("Delta command requires a GenerationalNBS")
		return 1
	}

	err := nbs.RunExperiment(cs, func(format string, args ...interface{}) {
		cli.Printf(format, args...)
	})
	if err != nil {
		cli.PrintErrln(err)
		return 1
	}

	return 0
}
