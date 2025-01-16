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
	"fmt"
	"regexp"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const VerboseFlag = "verbose"

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
	SyncBackupId        = "sync"
	SyncBackupUrlId     = "sync-url"
	RestoreBackupId     = "restore"
	AddBackupId         = "add"
	RemoveBackupId      = "remove"
	RemoveBackupShortId = "rm"
)

var branchForceFlagDesc = "Reset {{.LessThan}}branchname{{.GreaterThan}} to {{.LessThan}}startpoint{{.GreaterThan}}, even if {{.LessThan}}branchname{{.GreaterThan}} exists already. Without {{.EmphasisLeft}}-f{{.EmphasisRight}}, {{.EmphasisLeft}}dolt branch{{.EmphasisRight}} refuses to change an existing branch. In combination with {{.EmphasisLeft}}-d{{.EmphasisRight}} (or {{.EmphasisLeft}}--delete{{.EmphasisRight}}), allow deleting the branch irrespective of its merged status. In combination with -m (or {{.EmphasisLeft}}--move{{.EmphasisRight}}), allow renaming the branch even if the new branch name already exists, the same applies for {{.EmphasisLeft}}-c{{.EmphasisRight}} (or {{.EmphasisLeft}}--copy{{.EmphasisRight}})."

// CreateCommitArgParser creates the argparser shared dolt commit cli and DOLT_COMMIT.
func CreateCommitArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs("commit", 0)
	ap.SupportsString(MessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the commit message.")
	ap.SupportsFlag(AllowEmptyFlag, "", "Allow recording a commit that has the exact same data as its sole parent. This is usually a mistake, so it is disabled by default. This option bypasses that safety. Cannot be used with --skip-empty.")
	ap.SupportsFlag(SkipEmptyFlag, "", "Only create a commit if there are staged changes. If no changes are staged, the call to commit is a no-op. Cannot be used with --allow-empty.")
	ap.SupportsString(DateParam, "", "date", "Specify the date used in the commit. If not specified the current system time is used.")
	ap.SupportsFlag(ForceFlag, "f", "Ignores any foreign key warnings and proceeds with the commit.")
	ap.SupportsString(AuthorParam, "", "author", "Specify an explicit author using the standard A U Thor {{.LessThan}}author@example.com{{.GreaterThan}} format.")
	ap.SupportsFlag(AllFlag, "a", "Adds all existing, changed tables (but not new tables) in the working set to the staged set.")
	ap.SupportsFlag(UpperCaseAllFlag, "A", "Adds all tables and databases (including new tables) in the working set to the staged set.")
	ap.SupportsFlag(AmendFlag, "", "Amend previous commit")
	ap.SupportsOptionalString(SignFlag, "S", "key-id", "Sign the commit using GPG. If no key-id is provided the key-id is taken from 'user.signingkey' the in the configuration")
	return ap
}

func CreateConflictsResolveArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("conflicts resolve")
	ap.SupportsFlag(OursFlag, "", "For all conflicts, take the version from our branch and resolve the conflict")
	ap.SupportsFlag(TheirsFlag, "", "For all conflicts, take the version from their branch and resolve the conflict")
	return ap
}

func CreateMergeArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs("merge", 1)
	ap.TooManyArgsErrorFunc = func(receivedArgs []string) error {
		return errors.New("Error: Dolt does not support merging from multiple commits. You probably meant to checkout one and then merge from the other.")
	}
	ap.SupportsFlag(NoFFParam, "", "Create a merge commit even when the merge resolves as a fast-forward.")
	ap.SupportsFlag(SquashParam, "", "Merge changes to the working set without updating the commit history")
	ap.SupportsString(MessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the commit message.")
	ap.SupportsFlag(AbortParam, "", "Abort the in-progress merge and return the working set to the state before the merge started.")
	ap.SupportsFlag(CommitFlag, "", "Perform the merge and commit the result. This is the default option, but can be overridden with the --no-commit flag. Note that this option does not affect fast-forward merges, which don't create a new merge commit, and if any merge conflicts or constraint violations are detected, no commit will be attempted.")
	ap.SupportsFlag(NoCommitFlag, "", "Perform the merge and stop just before creating a merge commit. Note this will not prevent a fast-forward merge; use the --no-ff arg together with the --no-commit arg to prevent both fast-forwards and merge commits.")
	ap.SupportsFlag(NoEditFlag, "", "Use an auto-generated commit message when creating a merge commit. The default for interactive CLI sessions is to open an editor.")
	ap.SupportsString(AuthorParam, "", "author", "Specify an explicit author using the standard A U Thor {{.LessThan}}author@example.com{{.GreaterThan}} format.")

	return ap
}

func CreateRebaseArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs("rebase", 1)
	ap.TooManyArgsErrorFunc = func(receivedArgs []string) error {
		return errors.New("rebase takes at most one positional argument.")
	}
	ap.SupportsString(EmptyParam, "", "empty", "How to handle commits that are not empty to start, but which become empty after rebasing. Valid values are: drop (default) or keep")
	ap.SupportsFlag(AbortParam, "", "Abort an interactive rebase and return the working set to the pre-rebase state")
	ap.SupportsFlag(ContinueFlag, "", "Continue an interactive rebase after adjusting the rebase plan")
	ap.SupportsFlag(InteractiveFlag, "i", "Start an interactive rebase")
	return ap
}

func CreatePushArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("push")
	ap.SupportsString(UserFlag, "", "user", "User name to use when authenticating with the remote. Gets password from the environment variable {{.EmphasisLeft}}DOLT_REMOTE_PASSWORD{{.EmphasisRight}}.")
	ap.SupportsFlag(SetUpstreamFlag, "u", "For every branch that is up to date or successfully pushed, add upstream (tracking) reference, used by argument-less {{.EmphasisLeft}}dolt pull{{.EmphasisRight}} and other commands.")
	ap.SupportsFlag(ForceFlag, "f", "Update the remote with local history, overwriting any conflicting history in the remote.")
	ap.SupportsFlag(AllFlag, "", "Push all branches.")
	ap.SupportsFlag(SilentFlag, "", "Suppress progress information.")
	return ap
}

func CreateAddArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("add")
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "Working table(s) to add to the list tables staged to be committed. The abbreviation '.' can be used to add all tables."})
	ap.SupportsFlag(AllFlag, "A", "Stages any and all changes (adds, deletes, and modifications) except for ignored tables.")
	ap.SupportsFlag(ForceFlag, "f", "Allow adding otherwise ignored tables.")

	return ap
}

func CreateCloneArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs("clone", 2)
	ap.SupportsString(RemoteParam, "", "name", "Name of the remote to be added to the cloned database. The default is 'origin'.")
	ap.SupportsString(BranchParam, "b", "branch", "The branch to be cloned. If not specified all branches will be cloned.")
	ap.SupportsString(DepthFlag, "", "depth", "Clone a single branch and limit history to the given commit depth.")
	ap.SupportsString(dbfactory.AWSRegionParam, "", "region", "")
	ap.SupportsValidatedString(dbfactory.AWSCredsTypeParam, "", "creds-type", "", argparser.ValidatorFromStrList(dbfactory.AWSCredsTypeParam, dbfactory.AWSCredTypes))
	ap.SupportsString(dbfactory.AWSCredsFileParam, "", "file", "AWS credentials file.")
	ap.SupportsString(dbfactory.AWSCredsProfile, "", "profile", "AWS profile to use.")
	ap.SupportsString(dbfactory.OSSCredsFileParam, "", "file", "OSS credentials file.")
	ap.SupportsString(dbfactory.OSSCredsProfile, "", "profile", "OSS profile to use.")
	ap.SupportsString(UserFlag, "u", "user", "User name to use when authenticating with the remote. Gets password from the environment variable {{.EmphasisLeft}}DOLT_REMOTE_PASSWORD{{.EmphasisRight}}.")
	ap.SupportsFlag(SingleBranchFlag, "", "Clone only the history leading to the tip of a single branch, either specified by --branch or the remote's HEAD (default).")
	return ap
}

func CreateResetArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("reset")
	ap.SupportsFlag(HardResetParam, "", "Resets the working tables and staged tables. Any changes to tracked tables in the working tree since {{.LessThan}}commit{{.GreaterThan}} are discarded.")
	ap.SupportsFlag(SoftResetParam, "", "Does not touch the working tables, but removes all tables staged to be committed.")
	return ap
}

func CreateRemoteArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("remote")
	return ap
}

func CreateCleanArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("clean")
	ap.SupportsFlag(DryRunFlag, "", "Tests removing untracked tables without modifying the working set.")
	return ap
}

func CreateCheckoutArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("checkout")
	ap.SupportsString(CheckoutCreateBranch, "", "branch", "Create a new branch named {{.LessThan}}new_branch{{.GreaterThan}} and start it at {{.LessThan}}start_point{{.GreaterThan}}.")
	ap.SupportsString(CreateResetBranch, "", "branch", "Similar to '-b'. Forcibly resets the branch to {{.LessThan}}start_point{{.GreaterThan}} if it exists.")
	ap.SupportsFlag(ForceFlag, "f", "If there is any changes in working set, the force flag will wipe out the current changes and checkout the new branch.")
	ap.SupportsString(TrackFlag, "t", "", "When creating a new branch, set up 'upstream' configuration.")
	return ap
}

func CreateCherryPickArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs("cherrypick", 1)
	ap.SupportsFlag(AbortParam, "", "Abort the current conflict resolution process, and revert all changes from the in-process cherry-pick operation.")
	ap.SupportsFlag(AllowEmptyFlag, "", "Allow empty commits to be cherry-picked. "+
		"Note that use of this option only keeps commits that were initially empty. "+
		"Commits which become empty, due to a previous commit, will cause cherry-pick to fail.")
	ap.TooManyArgsErrorFunc = func(receivedArgs []string) error {
		return errors.New("cherry-picking multiple commits is not supported yet.")
	}
	return ap
}

func CreateFetchArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("fetch")
	ap.SupportsString(UserFlag, "", "user", "User name to use when authenticating with the remote. Gets password from the environment variable {{.EmphasisLeft}}DOLT_REMOTE_PASSWORD{{.EmphasisRight}}.")
	ap.SupportsFlag(PruneFlag, "p", "After fetching, remove any remote-tracking references that don't exist on the remote.")
	ap.SupportsFlag(SilentFlag, "", "Suppress progress information.")
	return ap
}

func CreateRevertArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("revert")
	ap.SupportsString(AuthorParam, "", "author", "Specify an explicit author using the standard A U Thor {{.LessThan}}author@example.com{{.GreaterThan}} format.")
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"revision",
		"The commit revisions. If multiple revisions are given, they're applied in the order given."})

	return ap
}

func CreatePullArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs("pull", 2)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"remote", "The name of the remote to pull from."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"remoteBranch", "The name of a branch on the specified remote to be merged into the current working set."})
	ap.SupportsFlag(SquashParam, "", "Merge changes to the working set without updating the commit history")
	ap.SupportsFlag(NoFFParam, "", "Create a merge commit even when the merge resolves as a fast-forward.")
	ap.SupportsFlag(ForceFlag, "f", "Update from the remote HEAD even if there are errors.")
	ap.SupportsFlag(CommitFlag, "", "Perform the merge and commit the result. This is the default option, but can be overridden with the --no-commit flag. Note that this option does not affect fast-forward merges, which don't create a new merge commit, and if any merge conflicts or constraint violations are detected, no commit will be attempted.")
	ap.SupportsFlag(NoCommitFlag, "", "Perform the merge and stop just before creating a merge commit. Note this will not prevent a fast-forward merge; use the --no-ff arg together with the --no-commit arg to prevent both fast-forwards and merge commits.")
	ap.SupportsFlag(NoEditFlag, "", "Use an auto-generated commit message when creating a merge commit. The default for interactive CLI sessions is to open an editor.")
	ap.SupportsString(UserFlag, "", "user", "User name to use when authenticating with the remote. Gets password from the environment variable {{.EmphasisLeft}}DOLT_REMOTE_PASSWORD{{.EmphasisRight}}.")
	ap.SupportsFlag(PruneFlag, "p", "After fetching, remove any remote-tracking references that don't exist on the remote.")
	ap.SupportsFlag(SilentFlag, "", "Suppress progress information.")
	return ap
}

func createTracklessBranchArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("branch")
	ap.SupportsFlag(ForceFlag, "f", branchForceFlagDesc)
	ap.SupportsFlag(CopyFlag, "c", "Create a copy of a branch.")
	ap.SupportsFlag(MoveFlag, "m", "Move/rename a branch")
	ap.SupportsFlag(DeleteFlag, "d", "Delete a branch. The branch must be fully merged in its upstream branch.")
	ap.SupportsFlag(DeleteForceFlag, "", "Shortcut for {{.EmphasisLeft}}--delete --force{{.EmphasisRight}}.")

	return ap
}

func CreateBranchArgParser() *argparser.ArgParser {
	ap := createTracklessBranchArgParser()
	ap.SupportsString(TrackFlag, "t", "", "When creating a new branch, set up 'upstream' configuration.")

	return ap
}

func CreateBranchArgParserWithNoTrackValue() *argparser.ArgParser {
	ap := createTracklessBranchArgParser()
	ap.SupportsFlag(TrackFlag, "t", "When creating a new branch, set up 'upstream' configuration.")

	return ap
}

func CreateTagArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("tag")
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"ref", "A commit ref that the tag should point at."})
	ap.SupportsString(MessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the tag message.")
	ap.SupportsFlag(VerboseFlag, "v", "list tags along with their metadata.")
	ap.SupportsFlag(DeleteFlag, "d", "Delete a tag.")
	ap.SupportsString(AuthorParam, "", "author", "Specify an explicit author using the standard A U Thor {{.LessThan}}author@example.com{{.GreaterThan}} format.")
	return ap
}

func CreateBackupArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("backup")
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"region", "cloud provider region associated with this backup."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"creds-type", "credential type.  Valid options are role, env, and file.  See the help section for additional details."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"profile", "AWS profile to use."})
	ap.SupportsFlag(VerboseFlag, "v", "When printing the list of backups adds additional details.")
	ap.SupportsFlag(ForceFlag, "f", "When restoring a backup, overwrite the contents of the existing database with the same name.")
	ap.SupportsString(dbfactory.AWSRegionParam, "", "region", "")
	ap.SupportsValidatedString(dbfactory.AWSCredsTypeParam, "", "creds-type", "", argparser.ValidatorFromStrList(dbfactory.AWSCredsTypeParam, dbfactory.AWSCredTypes))
	ap.SupportsString(dbfactory.AWSCredsFileParam, "", "file", "AWS credentials file")
	ap.SupportsString(dbfactory.AWSCredsProfile, "", "profile", "AWS profile to use")
	return ap
}

func CreateVerifyConstraintsArgParser(name string) *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(name)
	ap.SupportsFlag(AllFlag, "a", "Verifies that all rows in the database do not violate constraints instead of just rows modified or inserted in the working set.")
	ap.SupportsFlag(OutputOnlyFlag, "o", "Disables writing violated constraints to the constraint violations table.")
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table(s) to check constraints on. If omitted, checks all tables."})
	return ap
}

func CreateLogArgParser(isTableFunction bool) *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs("log")
	ap.SupportsInt(NumberFlag, "n", "num_commits", "Limit the number of commits to output.")
	ap.SupportsInt(MinParentsFlag, "", "parent_count", "The minimum number of parents a commit must have to be included in the log.")
	ap.SupportsFlag(MergesFlag, "", "Equivalent to min-parents == 2, this will limit the log to commits with 2 or more parents.")
	ap.SupportsFlag(ParentsFlag, "", "Shows all parents of each commit in the log.")
	ap.SupportsString(DecorateFlag, "", "decorate_fmt", "Shows refs next to commits. Valid options are short, full, no, and auto")
	ap.SupportsStringList(NotFlag, "", "revision", "Excludes commits from revision.")
	ap.SupportsFlag(ShowSignatureFlag, "", "Shows the signature of each commit.")
	if isTableFunction {
		ap.SupportsStringList(TablesFlag, "t", "table", "Restricts the log to commits that modified the specified tables.")
	} else {
		ap.SupportsFlag(OneLineFlag, "", "Shows logs in a compact format.")
		ap.SupportsFlag(StatFlag, "", "Shows the diffstat for each commit.")
		ap.SupportsFlag(GraphFlag, "", "Shows the commit graph.")
	}
	return ap
}

func CreateGCArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs("gc", 0)
	ap.SupportsFlag(ShallowFlag, "s", "perform a fast, but incomplete garbage collection pass")
	ap.SupportsFlag(FullFlag, "f", "perform a full garbage collection, including the old generation")
	return ap
}

func CreateCountCommitsArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs("gc", 0)
	ap.SupportsString("from", "f", "commit id", "commit to start counting from")
	ap.SupportsString("to", "t", "commit id", "commit to stop counting at")
	return ap
}

func CreateReflogArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs("reflog", 1)
	ap.SupportsFlag(AllFlag, "", "Show all refs, including hidden refs, such as DoltHub workspace refs")
	return ap
}

func CreateGlobalArgParser(name string) *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(name)
	if name == "dolt" {
		ap.SupportsString("profile", "", "profile", "The name of the profile to use when executing SQL queries. Run `dolt profile --help` for more information.")
	}
	ap.SupportsString("user", "u", "user", "Defines the local superuser (defaults to `root`). If the specified user exists, will take on permissions of that user.")
	ap.SupportsString("password", "p", "password", "Defines the password for the user. Defaults to empty string when the user is `root`.")
	ap.SupportsString("host", "", "host", "Defines the host to connect to.")
	ap.SupportsString("port", "", "port", "Defines the port to connect to.")
	ap.SupportsFlag("no-tls", "", "Disables TLS for the connection to remote databases.")
	ap.SupportsString("data-dir", "", "data-dir", "Defines a data directory whose subdirectories should all be dolt data repositories accessible as independent databases. Defaults to the current directory.")
	ap.SupportsString("doltcfg-dir", "", "doltcfg-dir", "Defines a directory that contains configuration files for dolt. Defaults to `$data-dir/.doltcfg`. Will only be created if there is a change to configuration settings.")
	ap.SupportsString("privilege-file", "", "privilege-file", "Path to a file to load and store users and grants. Defaults to `$doltcfg-dir/privileges.db`. Will only be created if there is a change to privileges.")
	ap.SupportsString("branch-control-file", "", "branch-control-file", "Path to a file to load and store branch control permissions. Defaults to `$doltcfg-dir/branch_control.db`. Will only be created if there is a change to branch control permissions.")
	ap.SupportsString("use-db", "", "use-db", "The name of the database to use when executing SQL queries. Defaults the database of the root directory, if it exists, and the first alphabetically if not.")
	ap.SupportsString("branch", "", "branch", "Name of the branch to be selected")
	return ap
}

var awsParams = []string{dbfactory.AWSRegionParam, dbfactory.AWSCredsTypeParam, dbfactory.AWSCredsFileParam, dbfactory.AWSCredsProfile}
var ossParams = []string{dbfactory.OSSCredsFileParam, dbfactory.OSSCredsProfile}

func ProcessBackupArgs(apr *argparser.ArgParseResults, scheme, backupUrl string) (map[string]string, error) {
	params := map[string]string{}

	var err error
	switch scheme {
	case dbfactory.AWSScheme:
		err = AddAWSParams(backupUrl, apr, params)
	case dbfactory.OSSScheme:
		err = AddOSSParams(backupUrl, apr, params)
	default:
		err = VerifyNoAwsParams(apr)
	}
	return params, err
}

func AddAWSParams(remoteUrl string, apr *argparser.ArgParseResults, params map[string]string) error {
	isAWS := strings.HasPrefix(remoteUrl, "aws")

	if !isAWS {
		for _, p := range awsParams {
			if _, ok := apr.GetValue(p); ok {
				return fmt.Errorf("%s param is only valid for aws cloud remotes in the format aws://dynamo-table:s3-bucket/database", p)
			}
		}
	}

	for _, p := range awsParams {
		if val, ok := apr.GetValue(p); ok {
			params[p] = val
		}
	}

	return nil
}

func AddOSSParams(remoteUrl string, apr *argparser.ArgParseResults, params map[string]string) error {
	isOSS := strings.HasPrefix(remoteUrl, "oss")

	if !isOSS {
		for _, p := range ossParams {
			if _, ok := apr.GetValue(p); ok {
				return fmt.Errorf("%s param is only valid for oss cloud remotes in the format oss://oss-bucket/database", p)
			}
		}
	}

	for _, p := range ossParams {
		if val, ok := apr.GetValue(p); ok {
			params[p] = val
		}
	}

	return nil
}

func VerifyNoAwsParams(apr *argparser.ArgParseResults) error {
	if awsParams := apr.GetValues(awsParams...); len(awsParams) > 0 {
		awsParamKeys := make([]string, 0, len(awsParams))
		for k := range awsParams {
			awsParamKeys = append(awsParamKeys, k)
		}

		keysStr := strings.Join(awsParamKeys, ",")
		return fmt.Errorf("The parameters %s, are only valid for aws remotes", keysStr)
	}

	return nil
}

// VerifyCommitArgs validates the arguments in |apr| for `dolt commit` and returns an error
// if any validation problems were encountered.
func VerifyCommitArgs(apr *argparser.ArgParseResults) error {
	if apr.Contains(AllowEmptyFlag) && apr.Contains(SkipEmptyFlag) {
		return errors.New("error: cannot use both --allow-empty and --skip-empty")
	}

	return nil
}
