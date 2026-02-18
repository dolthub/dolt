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
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/gpg"
	"github.com/dolthub/dolt/go/store/datas"
)

// doltCommit is the stored procedure version for the CLI command `dolt commit`.
func doltCommit(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	commitHash, skipped, err := doDoltCommit(ctx, args)
	if err != nil {
		return nil, err
	}
	if skipped {
		return nil, nil
	}
	return rowToIter(commitHash), nil
}

// doltCommitHashOut is the stored procedure version for the CLI function `commit`. The first parameter is the variable
// to set the hash of.
func doltCommitHashOut(ctx *sql.Context, outHash *string, args ...string) (sql.RowIter, error) {
	commitHash, skipped, err := doDoltCommit(ctx, args)
	if err != nil {
		return nil, err
	}
	if skipped {
		return nil, nil
	}

	*outHash = commitHash
	return rowToIter(commitHash), nil
}

// doDoltCommit creates a dolt commit using the specified command line |args| provided. The response is the commit hash
// of the new commit (or the empty string if the commit was skipped), a boolean that indicates if creating the commit
// was skipped (e.g. due to --skip-empty), and an error describing any error encountered.
func doDoltCommit(ctx *sql.Context, args []string) (string, bool, error) {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return "", false, err
	}
	// Get the information for the sql context.
	dbName := ctx.GetCurrentDatabase()

	apr, err := cli.CreateCommitArgParser(true).Parse(args)
	if err != nil {
		return "", false, err
	}

	targetBranch, branchSpecified := apr.GetValue(cli.BranchParam)
	if branchSpecified {
		// Use revision-qualified database name for the target branch. This will enable you to commit
		// to branches other than the current branch.
		dbName = fmt.Sprintf("%s/%s", dbName, targetBranch)
	}

	if err := cli.VerifyCommitArgs(apr); err != nil {
		return "", false, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return "", false, fmt.Errorf("Could not load database %s", dbName)
	}

	if apr.Contains(cli.UpperCaseAllFlag) {
		roots, err = actions.StageAllTables(ctx, roots, true)
		if err != nil {
			return "", false, err
		}
		roots, err = actions.StageDatabase(ctx, roots)
		if err != nil {
			return "", false, err
		}
	} else if apr.Contains(cli.AllFlag) {
		roots, err = actions.StageModifiedAndDeletedTables(ctx, roots)
		if err != nil {
			return "", false, err
		}
		roots, err = actions.StageDatabase(ctx, roots)
		if err != nil {
			return "", false, err
		}
	}

	msg, msgOk := apr.GetValue(cli.MessageArg)
	commitStagedProps, err := dSess.NewCommitStagedProps(ctx, msg, dsess.FallbackToSQLClient)
	if err != nil {
		return "", false, err
	}

	commitStagedProps.Amend = apr.Contains(cli.AmendFlag)
	if !msgOk {
		if commitStagedProps.Amend {
			commit, err := dSess.GetHeadCommit(ctx, dbName)
			if err != nil {
				return "", false, err
			}
			commitMeta, err := commit.GetCommitMeta(ctx)
			if err != nil {
				return "", false, err
			}
			msg = commitMeta.Description
		} else {
			return "", false, fmt.Errorf("Must provide commit message.")
		}
	}

	commitStagedProps.AllowEmpty = apr.Contains(cli.AllowEmptyFlag)
	commitStagedProps.SkipEmpty = apr.Contains(cli.SkipEmptyFlag)
	commitStagedProps.SkipVerification = apr.Contains(cli.SkipVerificationFlag)

	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		commitStagedProps.Name, commitStagedProps.Email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return "", false, err
		}
	}

	// Override author date if --date flag is provided.
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		t, err := dconfig.ParseDate(commitTimeStr)
		if err != nil {
			return "", false, err
		}
		commitStagedProps.Date = &t
	}

	if apr.Contains(cli.ForceFlag) {
		commitStagedProps.Force = true
		err = ctx.SetSessionVariable(ctx, "dolt_force_transaction_commit", 1)
		if err != nil {
			return "", false, err
		}
	}

	shouldSign, err := dsess.GetBooleanSystemVar(ctx, "gpgsign")
	if err != nil {
		return "", false, fmt.Errorf("failed to get gpgsign: %w", err)
	}

	pendingCommit, err := dSess.NewPendingCommit(ctx, dbName, roots, commitStagedProps)
	if err != nil {
		return "", false, err
	}

	// Nothing to commit, and we didn't pass --allowEmpty
	if pendingCommit == nil && apr.Contains(cli.SkipEmptyFlag) {
		return "", true, nil
	} else if pendingCommit == nil {
		return "", false, errors.New("nothing to commit")
	}

	// Set up signing context if signing is requested.
	if apr.Contains(cli.SignFlag) || shouldSign {
		keyId := apr.GetValueOrDefault(cli.SignFlag, "")

		if keyId == "" {
			v, err := ctx.GetSessionVariable(ctx, "signingkey")
			if err != nil && !sql.ErrUnknownSystemVariable.Is(err) {
				return "", false, fmt.Errorf("failed to get signingkey: %w", err)
			} else if err == nil {
				keyId = v.(string)
			}
		}

		headHash, err := roots.Head.HashOf()
		if err != nil {
			return "", false, err
		}
		stagedHash, err := roots.Staged.HashOf()
		if err != nil {
			return "", false, err
		}

		pendingCommit.CommitOptions.Signing = &datas.SigningContext{
			Key:        keyId,
			SignFunc:   gpg.Sign,
			DBName:     dbName,
			HeadHash:   headHash,
			StagedHash: stagedHash,
		}
	}

	newCommit, err := dSess.DoltCommit(ctx, dbName, dSess.GetTransaction(), pendingCommit)
	if err != nil {
		return "", false, err
	}

	h, err := newCommit.HashOf()
	if err != nil {
		return "", false, err
	}

	return h.String(), false, nil
}

func getDoltArgs(ctx *sql.Context, row sql.Row, children []sql.Expression) ([]string, error) {
	args := make([]string, len(children))
	for i := range children {
		childVal, err := children[i].Eval(ctx, row)

		if err != nil {
			return nil, err
		}

		text, _, err := types.Text.Convert(ctx, childVal)

		if err != nil {
			return nil, err
		}

		args[i] = text.(string)
	}

	return args, nil
}
