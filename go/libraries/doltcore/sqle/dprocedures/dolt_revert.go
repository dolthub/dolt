// Copyright 2022 Dolthub, Inc.
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

package dprocedures

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	revertpkg "github.com/dolthub/dolt/go/libraries/doltcore/revert"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

// doltRevertSchema matches the return schema for merge and cherry-pick to report
// the created commit, data conflicts, schema conflicts, and constraint violations.
// Unlike merge and cherry-pick, revert can create multiple commits. When this
// occurs, the returned commit hash is the last reverted commit. Customers can find
// the other reverted commits by looking in the commit log.
var doltRevertSchema = []*sql.Column{
	{
		Name:     "hash",
		Type:     gmstypes.LongText,
		Nullable: true,
	},
	{
		Name:     "data_conflicts",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
	{
		Name:     "schema_conflicts",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
	{
		Name:     "constraint_violations",
		Type:     gmstypes.Int64,
		Nullable: false,
	},
}

// doltRevert is the stored procedure version for the CLI command `dolt revert`.
func doltRevert(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	commitHash, dataConflicts, schemaConflicts, constraintViolations, err := doDoltRevert(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(commitHash, int64(dataConflicts), int64(schemaConflicts), int64(constraintViolations)), nil
}

func doDoltRevert(ctx *sql.Context, args []string) (string, int, int, int, error) {
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return "", 0, 0, 0, fmt.Errorf("error: empty database name")
	}

	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return "", 0, 0, 0, err
	}

	apr, err := cli.CreateRevertArgParser().Parse(args)
	if err != nil {
		return "", 0, 0, 0, err
	}

	if apr.Contains(cli.AbortParam) && apr.Contains(cli.ContinueFlag) {
		return "", 0, 0, 0, fmt.Errorf("error: --continue and --abort are mutually exclusive")
	}

	if apr.Contains(cli.AbortParam) {
		return "", 0, 0, 0, revertpkg.AbortRevert(ctx, dbName)
	}

	authorName, authorEmail := resolveAuthor(ctx, apr)

	if apr.Contains(cli.ContinueFlag) {
		return revertpkg.ContinueRevert(ctx, dbName, authorName, authorEmail)
	}

	if apr.NArg() == 0 {
		return "", 0, 0, 0, fmt.Errorf("error: nothing specified to revert")
	}

	// Resolve all revision specs to stable commit hashes before starting any reverts.
	// Each successful revert creates a new commit and moves HEAD, so relative specs
	// like HEAD~1 would drift if resolved lazily inside each Revert() call.
	resolvedHashes, err := resolveRevisionSpecs(ctx, dbName, apr.Args)
	if err != nil {
		return "", 0, 0, 0, err
	}

	// Capture the HEAD commit before any reverts begin. This is stored in the merge state
	// so that --abort can restore the branch to its pre-series position even if some reverts
	// in the series already succeeded and advanced HEAD before a conflict was encountered.
	doltSession := dsess.DSessFromSess(ctx.Session)
	preRevertHeadCommit, err := doltSession.GetHeadCommit(ctx, dbName)
	if err != nil {
		return "", 0, 0, 0, err
	}

	var lastHash string
	for i, commitHash := range resolvedHashes {
		pendingHashes := resolvedHashes[i+1:]
		revertHash, mergeResult, err := revertpkg.Revert(ctx, commitHash, authorName, authorEmail, preRevertHeadCommit, pendingHashes)
		if err != nil {
			return "", 0, 0, 0, err
		}

		if mergeResult != nil {
			// Conflicts encountered – return counts so the caller can inform the user.
			// The remaining hashes have already been stored in the merge state by Revert(),
			// so --continue will pick them up automatically.
			return "",
				mergeResult.CountOfTablesWithDataConflicts(),
				mergeResult.CountOfTablesWithSchemaConflicts(),
				mergeResult.CountOfTablesWithConstraintViolations(),
				nil
		}

		lastHash = revertHash
	}

	return lastHash, 0, 0, 0, nil
}

// resolveRevisionSpecs resolves each revision string to a stable commit hash string before any reverts begin.
// All specs must be resolved upfront because each successive revert moves HEAD, which would cause relative
// specs like HEAD~1 to refer to different commits than originally intended.
func resolveRevisionSpecs(ctx *sql.Context, dbName string, revisionStrs []string) ([]string, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)

	ddb, ok := doltSession.GetDoltDB(ctx, dbName)
	if !ok {
		return nil, fmt.Errorf("failed to get dolt database")
	}

	headRef, err := doltSession.CWBHeadRef(ctx, dbName)
	if err != nil {
		return nil, err
	}

	hashes := make([]string, len(revisionStrs))
	for i, revStr := range revisionStrs {
		commitSpec, err := doltdb.NewCommitSpec(revStr)
		if err != nil {
			return nil, err
		}

		optCmt, err := ddb.Resolve(ctx, commitSpec, headRef)
		if err != nil {
			return nil, err
		}

		commit, ok := optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitEncountered
		}

		h, err := commit.HashOf()
		if err != nil {
			return nil, err
		}

		hashes[i] = h.String()
	}

	return hashes, nil
}

// resolveAuthor returns the author name and email from the --author flag, or falls back to the SQL session client.
func resolveAuthor(ctx *sql.Context, apr *argparser.ArgParseResults) (string, string) {
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err := cli.ParseAuthor(authorStr)
		if err == nil {
			return name, email
		}
	}
	name := ctx.Client().User
	email := fmt.Sprintf("%s@%s", ctx.Client().User, ctx.Client().Address)
	return name, email
}
