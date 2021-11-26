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

package commands

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/fatih/color"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/rebase"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
)

const (
	migrationPrompt = `Run "dolt migrate" to update this database to the latest data format`
	migrationMsg    = "Migrating database to the latest data format"

	migratePushFlag = "push"
	migratePullFlag = "pull"
)

var migrateDocs = cli.CommandDocumentationContent{
	ShortDesc: "Executes a database migration to use the latest Dolt data format.",
	LongDesc: `Migrate is a multi-purpose command to update the data format of a Dolt database. Over time, development 
on Dolt requires changes to the on-disk data format. These changes are necessary to improve Database performance amd 
correctness. Migrating to the latest format is therefore necessary for compatibility with the latest Dolt clients, and
to take advantage of the newly released Dolt features.`,

	Synopsis: []string{
		"[ --push ] [ --pull ]",
	},
}

type MigrateCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MigrateCmd) Name() string {
	return "migrate"
}

// Description returns a description of the command
func (cmd MigrateCmd) Description() string {
	return migrateDocs.ShortDesc
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd MigrateCmd) CreateMarkdown(_ io.Writer, _ string) error {
	return nil
}

func (cmd MigrateCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(migratePushFlag, "", "Push all migrated branches to the remote")
	ap.SupportsFlag(migratePullFlag, "", "Update all local tracking refs for a migrated remote")
	return ap
}

// EventType returns the type of the event to log
func (cmd MigrateCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_MIGRATE
}

// Exec executes the command
func (cmd MigrateCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, migrateDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

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

			remoteRef, err := env.GetTrackingRef(dest, remote)

			if err != nil {
				return err
			}

			destDB, err := remote.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format())

			if err != nil {
				return err
			}

			cli.Println(color.BlueString(fmt.Sprintf("Pushing migrated branch %s to %s", branch.String(), remoteName)))
			mode := ref.UpdateMode{Force: true}
			err = actions.PushToRemoteBranch(ctx, dEnv.RepoStateReader(), dEnv.TempTableFilesDir(), mode, src, dest, remoteRef, dEnv.DoltDB, destDB, remote, runProgFuncs, stopProgFuncs)

			if err != nil {
				if err == doltdb.ErrUpToDate {
					cli.Println("Everything up-to-date")
				} else if err == doltdb.ErrIsAhead || err == actions.ErrCantFF || err == datas.ErrMergeNeeded {
					cli.Printf("To %s\n", remote.Url)
					cli.Printf("! [rejected]          %s -> %s (non-fast-forward)\n", dest.String(), remoteRef.String())
					cli.Printf("error: failed to push some refs to '%s'\n", remote.Url)
					cli.Println("hint: Updates were rejected because the tip of your current branch is behind")
					cli.Println("hint: its remote counterpart. Integrate the remote changes (e.g.")
					cli.Println("hint: 'dolt pull ...') before pushing again.")
					return errhand.BuildDError("").Build()
				} else {
					status, ok := status.FromError(err)
					if ok && status.Code() == codes.PermissionDenied {
						cli.Println("hint: have you logged into DoltHub using 'dolt login'?")
						cli.Println("hint: check that user.email in 'dolt config --list' has write perms to DoltHub repo")
					}
					if rpcErr, ok := err.(*remotestorage.RpcError); ok {
						return errhand.BuildDError("error: push failed").AddCause(err).AddDetails(rpcErr.FullDetails()).Build()
					} else {
						return errhand.BuildDError("error: push failed").AddCause(err).Build()
					}
				}
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

	r, refSpecs, err := env.NewFetchOpts(apr.Args, dEnv.RepoStateReader())

	if err == nil {
		err = actions.FetchRefSpecs(ctx, dEnv.DbData(), refSpecs, r, ref.UpdateMode{Force: true}, runProgFuncs, stopProgFuncs)
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
