// Copyright 2020 Dolthub, Inc.
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

package cli

import (
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const VerboseFlag = "verbose"

// we are more permissive than what is documented.
var SupportedLayouts = []string{
	"2006/01/02",
	"2006/01/02T15:04:05",
	"2006/01/02T15:04:05Z07:00",

	"2006.01.02",
	"2006.01.02T15:04:05",
	"2006.01.02T15:04:05Z07:00",

	"2006-01-02",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05Z07:00",
}

// Parses a date string. Used by multiple commands.
func ParseDate(dateStr string) (time.Time, error) {
	for _, layout := range SupportedLayouts {
		t, err := time.Parse(layout, dateStr)

		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, errors.New("error: '" + dateStr + "' is not in a supported format.")
}

// Parses the author flag for the commit method.
func ParseAuthor(authorStr string) (string, string, error) {
	if len(authorStr) == 0 {
		return "", "", errors.New("Option 'author' requires a value")
	}

	reg := regexp.MustCompile("(?m)([^)]+) \\<([^)]+)") // Regex matches Name <email
	matches := reg.FindStringSubmatch(authorStr)        // This function places the original string at the beginning of matches

	// If name and email are provided
	if len(matches) != 3 {
		return "", "", errors.New("Author not formatted correctly. Use 'Name <author@example.com>' format")
	}

	name := matches[1]
	email := strings.ReplaceAll(matches[2], ">", "")

	return name, email, nil
}

const (
	AllowEmptyFlag   = "allow-empty"
	DateParam        = "date"
	CommitMessageArg = "message"
	AuthorParam      = "author"
	ForceFlag        = "force"
	DryRunFlag       = "dry-run"
	SetUpstreamFlag  = "set-upstream"
	AllFlag          = "all"
	HardResetParam   = "hard"
	SoftResetParam   = "soft"
	CheckoutCoBranch = "b"
	NoFFParam        = "no-ff"
	SquashParam      = "squash"
	AbortParam       = "abort"
	CopyFlag         = "copy"
	MoveFlag         = "move"
	DeleteFlag       = "delete"
	DeleteForceFlag  = "D"
)

const (
	SyncBackupId        = "sync"
	RestoreBackupId     = "restore"
	AddBackupId         = "add"
	RemoveBackupId      = "remove"
	RemoveBackupShortId = "rm"
)

var mergeAbortDetails = `Abort the current conflict resolution process, and try to reconstruct the pre-merge state.

If there were uncommitted working set changes present when the merge started, {{.EmphasisLeft}}dolt merge --abort{{.EmphasisRight}} will be unable to reconstruct these changes. It is therefore recommended to always commit or stash your changes before running dolt merge.
`

// Creates the argparser shared dolt commit cli and DOLT_COMMIT.
func CreateCommitArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(CommitMessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the commit message.")
	ap.SupportsFlag(AllowEmptyFlag, "", "Allow recording a commit that has the exact same data as its sole parent. This is usually a mistake, so it is disabled by default. This option bypasses that safety.")
	ap.SupportsString(DateParam, "", "date", "Specify the date used in the commit. If not specified the current system time is used.")
	ap.SupportsFlag(ForceFlag, "f", "Ignores any foreign key warnings and proceeds with the commit.")
	ap.SupportsString(AuthorParam, "", "author", "Specify an explicit author using the standard A U Thor <author@example.com> format.")
	ap.SupportsFlag(AllFlag, "a", "Adds all edited files in working to staged.")
	return ap
}

func CreateMergeArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(NoFFParam, "", "Create a merge commit even when the merge resolves as a fast-forward.")
	ap.SupportsFlag(SquashParam, "", "Merges changes to the working set without updating the commit history")
	ap.SupportsString(CommitMessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the commit message.")
	ap.SupportsFlag(AbortParam, "", mergeAbortDetails)
	return ap
}

func CreatePushArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(SetUpstreamFlag, "u", "For every branch that is up to date or successfully pushed, add upstream (tracking) reference, used by argument-less {{.EmphasisLeft}}dolt pull{{.EmphasisRight}} and other commands.")
	ap.SupportsFlag(ForceFlag, "f", "Update the remote with local history, overwriting any conflicting history in the remote.")
	return ap
}

func CreateAddArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "Working table(s) to add to the list tables staged to be committed. The abbreviation '.' can be used to add all tables."})
	ap.SupportsFlag("all", "A", "Stages any and all changes (adds, deletes, and modifications).")
	return ap
}

func CreateResetArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(HardResetParam, "", "Resets the working tables and staged tables. Any changes to tracked tables in the working tree since {{.LessThan}}commit{{.GreaterThan}} are discarded.")
	ap.SupportsFlag(SoftResetParam, "", "Does not touch the working tables, but removes all tables staged to be committed.")
	return ap
}

func CreateCleanArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(DryRunFlag, "", "Tests removing untracked tables without modifying the working set.")
	return ap
}

func CreateCheckoutArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(CheckoutCoBranch, "", "branch", "Create a new branch named {{.LessThan}}new_branch{{.GreaterThan}} and start it at {{.LessThan}}start_point{{.GreaterThan}}.")
	ap.SupportsFlag(ForceFlag, "f", "If there is any changes in working set, the force flag will wipe out the current changes and checkout the new branch.")
	return ap
}

func CreateFetchArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(ForceFlag, "f", "Update refs to remote branches with the current state of the remote, overwriting any conflicting history.")
	return ap
}

func CreateRevertArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(AuthorParam, "", "author", "Specify an explicit author using the standard A U Thor <author@example.com> format.")
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"revision",
		"The commit revisions. If multiple revisions are given, they're applied in the order given."})

	return ap
}

func CreatePullArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(SquashParam, "", "Merges changes to the working set without updating the commit history")
	ap.SupportsFlag(NoFFParam, "", "Create a merge commit even when the merge resolves as a fast-forward.")
	ap.SupportsFlag(ForceFlag, "f", "Ignores any foreign key warnings and proceeds with the commit.")

	return ap
}

func CreateBranchArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(ForceFlag, "f", "Ignores any foreign key warnings and proceeds with the commit.")
	ap.SupportsFlag(CopyFlag, "c", "Create a copy of a branch.")
	ap.SupportsFlag(MoveFlag, "m", "Move/rename a branch")
	ap.SupportsFlag(DeleteFlag, "d", "Delete a branch. The branch must be fully merged in its upstream branch.")
	ap.SupportsFlag(DeleteForceFlag, "", "Shortcut for {{.EmphasisLeft}}--delete --force{{.EmphasisRight}}.")

	return ap
}

func CreateBackupArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"region", "cloud provider region associated with this backup."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"creds-type", "credential type.  Valid options are role, env, and file.  See the help section for additional details."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"profile", "AWS profile to use."})
	ap.SupportsFlag(VerboseFlag, "v", "When printing the list of backups adds additional details.")
	ap.SupportsString(dbfactory.AWSRegionParam, "", "region", "")
	ap.SupportsValidatedString(dbfactory.AWSCredsTypeParam, "", "creds-type", "", argparser.ValidatorFromStrList(dbfactory.AWSCredsTypeParam, dbfactory.AWSCredTypes))
	ap.SupportsString(dbfactory.AWSCredsFileParam, "", "file", "AWS credentials file")
	ap.SupportsString(dbfactory.AWSCredsProfile, "", "profile", "AWS profile to use")
	return ap
}
