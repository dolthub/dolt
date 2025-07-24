// Copyright 2025 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"strconv"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/nbs"
)

type StorageCmd struct {
}

func (s StorageCmd) Name() string {
	return "storage"
}

func (s StorageCmd) Description() string {
	return "print storage information for the current database"
}

func (s StorageCmd) Exec(ctx context.Context, _ string, _ []string, dEnv *env.DoltEnv, _ cli.CliContext) int {
	abs, err := dEnv.FS.Abs("")
	if err != nil {
		cli.Println(fmt.Sprintf("Couldn't get absolute path: %v", err))
		return 1
	}

	mmapArchiveIndexesString := dEnv.Config.GetStringOrDefault(config.MmapArchiveIndexes, "false")

	mmapArchiveIndexes, err := strconv.ParseBool(mmapArchiveIndexesString)
	if err != nil {
		cli.Println(fmt.Sprintf("Couldn't parse : %v", err))
		return 1
	}

	smd, err := nbs.GetStorageMetadata(ctx, abs, &nbs.Stats{}, mmapArchiveIndexes)

	for _, artifact := range smd.GetArtifacts() {
		cli.Println(artifact.SummaryString())
	}
	return 0
}

func (s StorageCmd) Docs() *cli.CommandDocumentation {
	return &cli.CommandDocumentation{
		CommandStr: "storage",
		ShortDesc:  "print storage information for the current database",
		LongDesc:   `Admin command to get some basic insights into the storage files in this database`,
		Synopsis: []string{
			"storage",
		},
		ArgParser: s.ArgParser(),
	}
}

func (s StorageCmd) ArgParser() *argparser.ArgParser {
	return argparser.NewArgParserWithMaxArgs(s.Name(), 0)
}

var _ cli.Command = StorageCmd{}
