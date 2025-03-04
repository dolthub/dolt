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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/google/go-github/v57/github"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

const (
	featureVersionFlag      = "feature"
	verboseFlag             = "verbose"
	versionCheckFile        = "version_check.txt"
	disableVersionCheckFile = "disable_version_check.txt"
)

var versionDocs = cli.CommandDocumentationContent{
	ShortDesc: "Displays the version for the Dolt binary.",
	LongDesc: `Displays the version for the Dolt binary.

The out-of-date check can be disabled by running {{.EmphasisLeft}}dolt config --global --add versioncheck.disabled true{{.EmphasisRight}}.`,
	Synopsis: []string{
		`[--verbose] [--feature]`,
	},
}

type VersionCmd struct {
	BinaryName string
	VersionStr string
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd VersionCmd) Name() string {
	return "version"
}

// Description returns a description of the command
func (cmd VersionCmd) Description() string {
	return versionDocs.ShortDesc
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd VersionCmd) RequiresRepo() bool {
	return false
}

func (cmd VersionCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(versionDocs, ap)
}

func (cmd VersionCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag(featureVersionFlag, "f", "display the feature version of this repository.")
	ap.SupportsFlag(verboseFlag, "v", "display verbose details, including the storage format of this repository.")
	return ap
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd VersionCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, versionDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	return cmd.ExecWithArgParser(ctx, apr, usage, dEnv)
}

func (cmd VersionCmd) ExecWithArgParser(ctx context.Context, apr *argparser.ArgParseResults, usage cli.UsagePrinter, dEnv *env.DoltEnv) int {
	binName := "dolt"
	if cmd.BinaryName != "" {
		binName = cmd.BinaryName
	}
	cli.Printf("%s version %s\n", binName, cmd.VersionStr)

	versionCheckDisabled := dEnv.Config.GetStringOrDefault(config.VersionCheckDisabled, "false")
	if versionCheckDisabled == "false" {
		verr := checkAndPrintVersionOutOfDateWarning(cmd.VersionStr, dEnv)
		if verr != nil {
			// print error but don't fail
			cli.PrintErr(color.YellowString(verr.Verbose()))
		}
	}

	if apr.Contains(verboseFlag) {
		if dEnv.HasDoltDir() && dEnv.RSLoadErr == nil && !cli.CheckEnvIsValid(dEnv) {
			return 2
		} else if dEnv.HasDoltDir() && dEnv.RSLoadErr == nil {
			nbf := dEnv.DoltDB(ctx).Format()
			cli.Printf("database storage format: %s\n", dfunctions.GetStorageFormatDisplayString(nbf))
		}
	}

	if apr.Contains(featureVersionFlag) {
		if !cli.CheckEnvIsValid(dEnv) {
			return 2
		}
		wr, err := dEnv.WorkingRoot(ctx)
		if err != nil {
			verr := errhand.BuildDError("could not read working root").AddCause(err).Build()
			return HandleVErrAndExitCode(verr, usage)
		}

		fv, ok, err := wr.GetFeatureVersion(ctx)
		if err != nil {
			verr := errhand.BuildDError("error reading feature version").AddCause(err).Build()
			return HandleVErrAndExitCode(verr, usage)
		} else if !ok {
			verr := errhand.BuildDError("the current head does not have a feature version").Build()
			return HandleVErrAndExitCode(verr, usage)
		} else {
			cli.Println("feature version:", fv)
		}
	}

	return HandleVErrAndExitCode(nil, usage)
}

// checkAndPrintVersionOutOfDateWarning checks if the current version of Dolt is out of date and prints a warning if it
// is. Restricts this check to at most once per week unless the build version is ahead of the stored latest release version.
// Also prints a warning about how to disable this check once per version.
func checkAndPrintVersionOutOfDateWarning(curVersion string, dEnv *env.DoltEnv) errhand.VerboseError {
	var latestRelease string
	var verr errhand.VerboseError

	homeDir, err := dEnv.GetUserHomeDir()
	if err != nil {
		return errhand.BuildDError("error: failed to get user home directory").AddCause(err).Build()
	}
	path := filepath.Join(homeDir, dbfactory.DoltDir, versionCheckFile)

	if exists, _ := dEnv.FS.Exists(path); exists {
		vCheck, err := dEnv.FS.ReadFile(path)
		if err != nil {
			return errhand.BuildDError("error: failed to read version check file").AddCause(err).Build()
		}

		latestRelease = strings.ReplaceAll(string(vCheck), "\n", "")
		lastCheckDate, _ := dEnv.FS.LastModified(path)
		if lastCheckDate.Before(time.Now().AddDate(0, 0, -7)) {
			latestRelease, verr = getLatestDoltReleaseAndRecord(path, dEnv)
			if verr != nil {
				return verr
			}
		} else {
			if !isVersionFormattedCorrectly(latestRelease) {
				latestRelease, verr = getLatestDoltReleaseAndRecord(path, dEnv)
				if verr != nil {
					return verr
				}
			}
		}
	} else {
		latestRelease, verr = getLatestDoltReleaseAndRecord(path, dEnv)
		if verr != nil {
			return verr
		}
	}

	// If we still don't have a valid latestRelease, even after trying to query it, then skip the out of date
	// check and print a warning message. This can happen for example, if we get a 403 from GitHub when
	// querying for the latest release tag.
	if latestRelease == "" {
		cli.Print(color.YellowString("Warning: unable to query latest released Dolt version"))
		return nil
	}

	// if there were new releases in the last week, the latestRelease stored might be behind the current version built
	isOutOfDate, verr := isOutOfDate(curVersion, latestRelease)
	if verr != nil {
		return verr
	}
	if isOutOfDate {
		cli.Print(color.YellowString("Warning: you are on an old version of Dolt. The newest version is %s.\n", latestRelease))
		printDisableVersionCheckWarning(dEnv, homeDir, curVersion)
	}

	return nil
}

// getLatestDoltRelease returns the latest release of Dolt from GitHub and records the release and current date in the
// version check file.
func getLatestDoltReleaseAndRecord(path string, dEnv *env.DoltEnv) (string, errhand.VerboseError) {
	client := github.NewClient(nil)
	release, resp, err := client.Repositories.GetLatestRelease(context.Background(), "dolthub", "dolt")
	if err == nil && resp.StatusCode == 200 {
		releaseName := strings.TrimPrefix(*release.TagName, "v")

		err = dEnv.FS.WriteFile(path, []byte(releaseName), os.ModePerm)
		if err == nil {
			return releaseName, nil
		}
	}
	return "", nil
}

// isOutOfDate compares the current version of Dolt to the given latest release version and returns true if the current
// version is out of date.
func isOutOfDate(curVersion, latestRelease string) (bool, errhand.VerboseError) {
	curVersionParts := strings.Split(curVersion, ".")
	latestReleaseParts := strings.Split(latestRelease, ".")

	for i := 0; i < len(curVersionParts) && i < len(latestReleaseParts); i++ {
		curPart, err := strconv.Atoi(curVersionParts[i])
		if err != nil {
			return false, errhand.BuildDError("error: failed to parse version number").AddCause(err).Build()
		}
		latestPart, err := strconv.Atoi(latestReleaseParts[i])
		if err != nil {
			return false, errhand.BuildDError("error: failed to parse version number").AddCause(err).Build()
		}
		if latestPart > curPart {
			return true, nil
		} else if curPart > latestPart {
			return false, nil
		}
	}

	return false, nil
}

// isVersionFormattedCorrectly checks if the given version string is formatted correctly, i.e. is of the form
// major.minor.patch where each part is an integer.
func isVersionFormattedCorrectly(version string) bool {
	versionParts := strings.Split(version, ".")
	if len(versionParts) != 3 {
		return false
	}

	for _, part := range versionParts {
		if _, err := strconv.Atoi(part); err != nil {
			return false
		}
	}

	return true
}

// Prints a warning about how to disable the version out-of-date check, limited to once per version.
func printDisableVersionCheckWarning(dEnv *env.DoltEnv, homeDir, curVersion string) {
	path := filepath.Join(homeDir, dbfactory.DoltDir, disableVersionCheckFile)
	if exists, _ := dEnv.FS.Exists(path); !exists {
		cli.Println("To disable this warning, run 'dolt config --global --add versioncheck.disabled true'")
		dEnv.FS.WriteFile(path, []byte(curVersion), os.ModePerm)
	} else {
		lastDisableVersionCheckWarning, err := dEnv.FS.ReadFile(path)
		if err != nil {
			return
		}
		if string(lastDisableVersionCheckWarning) != curVersion {
			cli.Println("To disable this warning, run 'dolt config --global --add versioncheck.disabled true'")
			dEnv.FS.WriteFile(path, []byte(curVersion), os.ModePerm)
		}
	}

	return
}
