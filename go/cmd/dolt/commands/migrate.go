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

package commands

import (
	"context"
	"github.com/fatih/color"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rebase"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

const migrationPrompt = "Run dolt migrate to update this repository to the latest format"
const migrationMsg = "Migrating repository to the latest format"

type MigrateCmd struct {}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MigrateCmd) Name() string {
	return "migrate"
}

// Description returns a description of the command
func (cmd MigrateCmd) Description() string {
	return "Executes a repository migration to update to the latest format."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd MigrateCmd) CreateMarkdown(_ filesys.Filesys, _, _ string) error {
	return nil
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd MigrateCmd) Exec(ctx context.Context, _ string, _ []string, dEnv *env.DoltEnv) int {
	cli.Println(color.YellowString(migrationMsg))
	err := rebase.MigrateUniqueTags(ctx, dEnv)
	if err != nil {
		cli.PrintErrf("error migrating repository: %s", err.Error())
		return 1
	}
	return 0
}

// These subcommands will trigger a unique tags migration
func MigrationNeeded(ctx context.Context, dEnv *env.DoltEnv, args []string) bool {
	needed, err := rebase.NeedsUniqueTagMigration(ctx, dEnv)
	if err != nil {
		cli.PrintErrf("error checking for repository migration: %s", err.Error())
		// ambiguous whether we need to migrate, but we should exit
		return true
	}
	if !needed {
		return false
	}

	var subCmd string
	if len(args) > 0 {
		subCmd = args[0]
	}
	cli.PrintErrln(color.RedString("Cannot execute dolt %s, repository format is out of date.", subCmd))
	cli.Println(migrationPrompt)
	return true
}