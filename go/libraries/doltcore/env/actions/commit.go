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

package actions

import (
	"fmt"
	"io"
	"strings"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/datas"
)

const CommitVerificationFailedPrefix = "commit verification failed:"

var ErrCommitVerificationFailed = goerrors.NewKind(CommitVerificationFailedPrefix + " %s")

// CleanupMode controls how a commit message is cleaned before it is stored.
type CleanupMode int

const (
	// CleanupStrip strips trailing whitespace per line, drops leading and trailing blank
	// lines, and collapses runs of multiple consecutive blank lines to one. This is the
	// default. See [cleanup modes].
	//
	// [cleanup modes]: https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---cleanupltmodegt
	CleanupStrip CleanupMode = iota
	// CleanupVerbatim stores the message exactly as provided, with no whitespace changes.
	// Use this when replaying an already-stored commit whose message should not be altered.
	// See [cleanup modes].
	CleanupVerbatim
)

// cleanCommitMessage implements [CleanupStrip] on |s|.
func cleanCommitMessage(s string) string {
	var out []string
	blanks := 0
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimRight(line, " \t\r")
		if line == "" {
			blanks++
			continue
		}
		if len(out) > 0 && blanks > 0 {
			out = append(out, "")
		}
		blanks = 0
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// TODO(elianddb): expose the remaining cleanup modes via a --cleanup flag on dolt commit and
// dolt merge. See [cleanup modes].
//
// [cleanup modes]: https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---cleanupltmodegt
func applyCleanup(mode CleanupMode, msg string) string {
	if mode == CleanupStrip {
		return cleanCommitMessage(msg)
	}
	return msg
}

// CommitStagedProps contains the parameters for a staged commit operation.
type CommitStagedProps struct {
	Message string
	// CleanupMode controls how Message is cleaned before storage.
	CleanupMode      CleanupMode
	AllowEmpty       bool
	SkipEmpty        bool
	Amend            bool
	Force            bool
	SkipVerification bool
	// Author is the identity of the person who wrote the change.
	Author datas.CommitIdent
	// Committer is the identity of the person who applied the change.
	Committer datas.CommitIdent
}

const (
	// System variable name, defined here to avoid circular imports
	DoltCommitVerificationGroups = "dolt_commit_verification_groups"
)

// getCommitRunTestGroups returns the test groups to run for commit operations
// Returns empty slice if no tests should be run, ["*"] if all tests should be run,
// or specific group names if only those groups should be run
func getCommitRunTestGroups() []string {
	_, val, ok := sql.SystemVariables.GetGlobal(DoltCommitVerificationGroups)
	if !ok {
		return nil
	}
	if stringVal, ok := val.(string); ok && stringVal != "" {
		if stringVal == "*" {
			return []string{"*"}
		}
		// Split by comma and trim whitespace
		groups := strings.Split(stringVal, ",")
		for i, group := range groups {
			groups[i] = strings.TrimSpace(group)
		}
		return groups
	}
	return nil
}

// GetCommitStaged returns a new pending commit with the roots and commit properties given.
func GetCommitStaged(
	ctx *sql.Context,
	tableResolver doltdb.TableResolver,
	roots doltdb.Roots,
	ws *doltdb.WorkingSet,
	mergeParents []*doltdb.Commit,
	db *doltdb.DoltDB,
	props CommitStagedProps,
) (*doltdb.PendingCommit, error) {
	props.Message = applyCleanup(props.CleanupMode, props.Message)
	if props.Message == "" {
		return nil, datas.ErrEmptyCommitMessage
	}

	allowEmpty := ws.MergeActive() || props.AllowEmpty || props.Amend

	// HashOf returns a cached value, so comparing the staged root against HEAD
	// here avoids the full table delta walk for callers with nothing staged.
	if !allowEmpty {
		stagedHash, err := roots.Staged.HashOf()
		if err != nil {
			return nil, err
		}
		headHash, err := roots.Head.HashOf()
		if err != nil {
			return nil, err
		}
		if stagedHash == headHash {
			if props.SkipEmpty {
				return nil, nil
			}
			return nil, NothingStaged{}
		}
	}

	stagedTables, notStaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	var stagedTblNames []doltdb.TableName
	for _, td := range stagedTables {
		n := td.ToName
		if td.IsDrop() {
			n = td.FromName
		}
		stagedTblNames = append(stagedTblNames, n)
	}

	stagedSchemas, _, err := diff.GetStagedUnstagedDatabaseSchemaDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	isEmpty := len(stagedTables) == 0 && len(stagedSchemas) == 0

	if isEmpty && props.SkipEmpty {
		return nil, nil
	}
	if isEmpty && !allowEmpty {
		return nil, NothingStaged{notStaged}
	}

	if !props.Force {
		inConflict, err := doltdb.TablesWithDataConflicts(ctx, roots.Working)
		if err != nil {
			return nil, err
		}
		if len(inConflict) > 0 {
			return nil, NewTblInConflictError(inConflict)
		}
		violatesConstraints, err := doltdb.TablesWithConstraintViolations(ctx, roots.Working)
		if err != nil {
			return nil, err
		}
		if len(violatesConstraints) > 0 {
			return nil, NewTblHasConstraintViolations(violatesConstraints)
		}

		if ws.MergeActive() {
			schConflicts := ws.MergeState().TablesWithSchemaConflicts()
			if len(schConflicts) > 0 {
				return nil, NewTblSchemaConflictError(schConflicts)
			}
		}

		fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
		if err != nil {
			return nil, err
		}

		if intValue, ok := fkChecks.(int8); ok && intValue == 1 {
			roots.Staged, err = doltdb.ValidateForeignKeysOnSchemas(ctx, tableResolver, roots.Staged)
			if err != nil {
				return nil, err
			}
		}
	}

	if !props.SkipVerification {
		testGroups := getCommitRunTestGroups()
		if len(testGroups) > 0 {
			err := runCommitVerification(ctx, testGroups)
			if err != nil {
				return nil, err
			}
		}
	}

	commitMeta, err := datas.NewCommitMetaWithAuthorCommitter(props.Author, props.Committer, props.Message)
	if err != nil {
		return nil, err
	}

	return db.NewPendingCommit(ctx, roots, mergeParents, props.Amend, commitMeta)
}

// runCommitVerification runs the commit verification tests for the given test groups.
// If any tests fail, it returns ErrCommitVerificationFailed wrapping the failure details.
// Callers can use errors.Is(err, ErrCommitVerificationFailed) to detect this case.
func runCommitVerification(ctx *sql.Context, testGroups []string) error {
	type sessionInterface interface {
		sql.Session
		GenericProvider() sql.MutableDatabaseProvider
	}

	session, ok := ctx.Session.(sessionInterface)
	if !ok {
		return fmt.Errorf("session does not provide database provider interface")
	}

	provider := session.GenericProvider()
	engine := gms.NewDefault(provider)

	return runTestsUsingDtablefunctions(ctx, engine, testGroups)
}

// runTestsUsingDtablefunctions runs tests using the dtablefunctions package against the staged root
func runTestsUsingDtablefunctions(ctx *sql.Context, engine *gms.Engine, testGroups []string) error {
	if len(testGroups) == 0 {
		return nil
	}

	var allFailures []string

	for _, group := range testGroups {
		query := fmt.Sprintf("SELECT * FROM dolt_test_run('%s')", group)
		_, iter, _, err := engine.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to run dolt_test_run for group %s: %w", group, err)
		}

		for {
			row, rErr := iter.Next(ctx)
			if rErr == io.EOF {
				break
			}
			if rErr != nil {
				return fmt.Errorf("error reading test results: %w", rErr)
			}

			// Extract status (column 3)
			status := fmt.Sprintf("%v", row[3])
			if status != "PASS" {
				testName := fmt.Sprintf("%v", row[0])
				message := fmt.Sprintf("%v", row[4])
				allFailures = append(allFailures, fmt.Sprintf("%s (%s)", testName, message))
			}
		}
	}

	if len(allFailures) > 0 {
		return ErrCommitVerificationFailed.New(strings.Join(allFailures, ", "))
	}

	return nil
}
