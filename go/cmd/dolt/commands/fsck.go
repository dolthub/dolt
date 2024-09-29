// Copyright 2023 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type FsckCmd struct{}

var _ cli.Command = FsckCmd{}

func (cmd FsckCmd) Description() string {
	//TODO implement me
	panic("implement me")
}

func (cmd FsckCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, _ cli.CliContext) int {
	progress := make(chan interface{}, 32)
	defer close(progress)
	fsckHandleProgress(progress)

	err := dEnv.DoltDB.FSCK(ctx, progress)
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}

	return 0
}

func fsckHandleProgress(progress chan interface{}) {
	go func() {
		for item := range progress {
			cli.Println(item)
		}
	}()
}

func (cmd FsckCmd) Docs() *cli.CommandDocumentation {
	//TODO implement me
	panic("implement Docs")
}

func (cmd FsckCmd) ArgParser() *argparser.ArgParser {
	return &argparser.ArgParser{}
}

func (cmd FsckCmd) Name() string {
	return "fsck"
}
