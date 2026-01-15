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
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"

	"github.com/fatih/color"
	"github.com/dolthub/dolt/go/store/nbs/zstd"
)

type ZstdCmd struct {
}

func (cmd ZstdCmd) Name() string {
	return "zstd"
}

func (cmd ZstdCmd) Description() string {
	return "A temporary admin command for taking a dependency on gozstd and working out tooling dependencies."
}

func (cmd ZstdCmd) RequiresRepo() bool {
	return false
}

func (cmd ZstdCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd ZstdCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	return ap
}

func (cmd ZstdCmd) Hidden() bool {
	return true
}

func (cmd ZstdCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	fmt.Fprintf(color.Error, "Hello, world! compressed is %v\n", zstd.Compress(nil, []byte("Hello, world!")))

	return 0
}
