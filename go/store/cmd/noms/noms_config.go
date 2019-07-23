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
	"github.com/liquidata-inc/ld/dolt/go/store/cmd/noms/util"
	"github.com/liquidata-inc/ld/dolt/go/store/config"
)

var nomsConfig = &util.Command{
	Run:       runConfig,
	UsageLine: "config ",
	Short:     "Display noms config info",
	Long:      "Prints the active configuration if a .nomsconfig file is present",
	Flags:     setupConfigFlags,
	Nargs:     0,
}

func setupConfigFlags() *flag.FlagSet {
	return flag.NewFlagSet("config", flag.ExitOnError)
}

func runConfig(ctx context.Context, args []string) int {
	c, err := config.FindNomsConfig()
	if err == config.ErrNoConfig {
		fmt.Fprintf(os.Stdout, "no config active\n")
	} else {
		util.CheckError(err)
		fmt.Fprintf(os.Stdout, "%s\n", c.String())
	}
	return 0
}
