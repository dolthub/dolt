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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
	"os"

	flag "github.com/juju/gnuflag"

	"github.com/liquidata-inc/dolt/go/store/cmd/noms/util"
	"github.com/liquidata-inc/dolt/go/store/constants"
)

var nomsVersion = &util.Command{
	Run:       runVersion,
	UsageLine: "version ",
	Short:     "Display noms version",
	Long:      "version prints the Noms data version and build identifier",
	Flags:     setupVersionFlags,
	Nargs:     0,
}

func setupVersionFlags() *flag.FlagSet {
	return flag.NewFlagSet("version", flag.ExitOnError)
}

func runVersion(ctx context.Context, args []string) int {
	fmt.Fprintf(os.Stdout, "format version: %v\n", constants.NomsVersion)
	fmt.Fprintf(os.Stdout, "built from %v\n", constants.NomsGitSHA)
	return 0
}
