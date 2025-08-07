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

// CommitStagedProps contains the parameters for a staged commit operation.
type CommitStagedProps struct {
	Message string
	// Date is the author date. Use [datas.CommitDateAt] for an explicit date or [datas.CommitDateNow] to resolve at commit time.
	Date             datas.CommitDate
	AllowEmpty       bool
	SkipEmpty        bool
	Amend            bool
	Force            bool
	Name             string
	Email            string
	SkipVerification bool

	// CommitterDate is the committer date. Use [datas.CommitDateAt] for an explicit date or [datas.CommitDateNow] to resolve at commit time.
	CommitterDate  datas.CommitDate
	CommitterName  string
	CommitterEmail string
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

// NewCommitStagedProps creates a new CommitStagedProps with the given author information. Committer fields are
// automatically populated with the author information.
func NewCommitStagedProps(name, email string, date datas.CommitDate, message string) CommitStagedProps {
	return CommitStagedProps{
		Message:        message,
		Date:           date,
		Name:           name,
		Email:          email,
		CommitterName:  name,
		CommitterEmail: email,
	}
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

	author := datas.CommitIdentity{Name: props.Name, Email: props.Email, Date: props.Date}
	committer := datas.CommitIdentity{Name: props.CommitterName, Email: props.CommitterEmail, Date: props.CommitterDate}
	commitMeta, err := datas.NewCommitMetaWithAuthorAndCommitter(author, committer, props.Message)
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
