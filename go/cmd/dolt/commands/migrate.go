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
	"errors"
	"fmt"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rebase"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

const (
	migrationPrompt = `Run "dolt migrate" to update this repository to the latest format`
	migrationMsg    = "Migrating repository to the latest format"

	migratePushFlag = "push"
	migratePullFlag = "pull"
)

type MigrateCmd struct{}

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

func (cmd MigrateCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(migratePushFlag, "", "Push all migrated branches to the remote")
	ap.SupportsFlag(migratePullFlag, "", "Update all remote refs for a migrated remote")
	return ap
}

// EventType returns the type of the event to log
func (cmd MigrateCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TYPE_UNSPECIFIED
}

// Exec executes the command
func (cmd MigrateCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, pushDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.Contains(migratePushFlag) && apr.Contains(migratePullFlag) {
		cli.PrintErrf(color.RedString("options --%s and --%s are mutually exclusive", migratePushFlag, migratePullFlag))
		return 1
	}

	var err error
	switch {
	case apr.Contains(migratePushFlag):
		err = pushMigratedRepo(ctx, dEnv, apr)
	case apr.Contains(migratePullFlag):
		err = fetchMigratedRemoteBranches(ctx, dEnv, apr)
	default:
		err = migrateLocalRepo(ctx, dEnv)
	}

	if err != nil {
		cli.PrintErrln(color.RedString(err.Error()))
		return 1
	}

	return 0
}

func migrateLocalRepo(ctx context.Context, dEnv *env.DoltEnv) error {
	localMigrationNeeded, err := rebase.NeedsUniqueTagMigration(ctx, dEnv.DoltDB)

	if err != nil {
		return err
	}

	if localMigrationNeeded {
		cli.Println(color.YellowString(migrationMsg))
		err = rebase.MigrateUniqueTags(ctx, dEnv)

		if err != nil {
			return err
		}
	} else {
		cli.Println("Repository format is up to date")
	}

	remoteName := "origin"
	remoteMigrated, err := remoteHasBeenMigrated(ctx, dEnv, remoteName)
	if err != nil {
		// if we can't check the remote, exit silently
		return nil
	}

	if !remoteMigrated {
		cli.Println(fmt.Sprintf("Remote %s has not been migrated", remoteName))
		cli.Println(fmt.Sprintf("Run 'dolt migrate --push %s' to update remote", remoteName))
	} else {
		cli.Println(fmt.Sprintf("Remote %s has been migrated", remoteName))
		cli.Println(fmt.Sprintf("Run 'dolt migrate --pull %s' to update refs", remoteName))
	}

	return nil
}

func pushMigratedRepo(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) error {
	localMigrationNeeded, err := rebase.NeedsUniqueTagMigration(ctx, dEnv.DoltDB)
	if err != nil {
		return err
	}
	if localMigrationNeeded {
		return fmt.Errorf("Local repo must be migrated before pushing, run 'dolt migrate'")
	}

	remoteName := "origin"
	if apr.NArg() > 0 {
		remoteName = apr.Arg(0)
	}

	remotes, err := dEnv.GetRemotes()
	if err != nil {
		return err
	}

	remote, remoteOK := remotes[remoteName]
	if !remoteOK {
		return fmt.Errorf("unknown remote %s", remoteName)
	}

	remoteMigrated, err := remoteHasBeenMigrated(ctx, dEnv, remoteName)
	if err != nil {
		return err
	}
	if remoteMigrated {
		return fmt.Errorf("Remote %s has been migrated\nRun 'dolt migrate --pull' to update refs", remoteName)
	} else {
		// force push all branches
		bb, err := dEnv.DoltDB.GetBranches(ctx)

		if err != nil {
			return err
		}

		for _, branch := range bb {
			refSpec, err := ref.ParseRefSpec(branch.String())
			if err != nil {
				return err
			}

			src := refSpec.SrcRef(branch)
			dest := refSpec.DestRef(src)

			remoteRef, err := getTrackingRef(dest, remote)

			if err != nil {
				return err
			}

			destDB, err := remote.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format())

			if err != nil {
				return err
			}

			cli.Println(color.BlueString(fmt.Sprintf("Pushing migrated branch %s to %s", branch.String(), remoteName)))
			mode := ref.RefUpdateMode{Force: true}
			err = pushToRemoteBranch(ctx, dEnv, mode, src, dest, remoteRef, dEnv.DoltDB, destDB, remote)

			if err != nil {
				return err
			}
			cli.Println()
		}
	}

	return nil
}

func fetchMigratedRemoteBranches(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) error {
	localMigrationNeeded, err := rebase.NeedsUniqueTagMigration(ctx, dEnv.DoltDB)
	if err != nil {
		return err
	}
	if localMigrationNeeded {
		return fmt.Errorf("Local repo must be migrated before pulling, run 'dolt migrate'")
	}

	remoteName := "origin"
	if apr.NArg() > 0 {
		remoteName = apr.Arg(0)
	}

	remoteMigrated, err := remoteHasBeenMigrated(ctx, dEnv, remoteName)
	if err != nil {
		return err
	}
	if !remoteMigrated {
		return fmt.Errorf("Remote %s has not been migrated\nRun 'dolt migrate --push %s' to push migration", remoteName, remoteName)
	}

	// force fetch all branches
	remotes, _ := dEnv.GetRemotes()
	r, refSpecs, err := getRefSpecs(apr.Args(), dEnv, remotes)

	if err == nil {
		err = fetchRefSpecs(ctx, ref.RefUpdateMode{Force: true}, dEnv, r, refSpecs)
	}

	return err
}

func remoteHasBeenMigrated(ctx context.Context, dEnv *env.DoltEnv, remoteName string) (bool, error) {
	remotes, err := dEnv.GetRemotes()

	if err != nil {
		return false, errors.New("error: failed to read remotes from config.")
	}

	remote, remoteOK := remotes[remoteName]
	if !remoteOK {
		return false, fmt.Errorf("cannot find remote %s", remoteName)
	}

	destDB, err := remote.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format())

	if err != nil {
		return false, err
	}

	needed, err := rebase.NeedsUniqueTagMigration(ctx, destDB)
	if err != nil {
		return false, err
	}

	return !needed, nil
}

// These subcommands will trigger a unique tags migration
func MigrationNeeded(ctx context.Context, dEnv *env.DoltEnv, args []string) bool {
	needed, err := rebase.NeedsUniqueTagMigration(ctx, dEnv.DoltDB)
	if err != nil {
		cli.PrintErrf(color.RedString("error checking for repository migration: %s", err.Error()))
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
	cli.PrintErrln(color.RedString("Cannot execute 'dolt %s', repository format is out of date.", subCmd))
	cli.Println(migrationPrompt)
	return true
}
