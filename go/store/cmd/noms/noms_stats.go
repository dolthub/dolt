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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"

	"github.com/attic-labs/kingpin"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/config"
)

func nomsStats(ctx context.Context, noms *kingpin.Application) (*kingpin.CmdClause, util.KingpinHandler) {
	stats := noms.Command("stats", "Shows stats summary for a Noms Database")
	database := stats.Arg("database", "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the database argument.").Required().String()

	return stats, func(input string) int {
		cfg := config.NewResolver()
		store, _, err := cfg.GetDatabase(ctx, *database)
		util.CheckError(err)
		defer store.Close()

		fmt.Println(store.StatsSummary())
		return 0
	}
}
