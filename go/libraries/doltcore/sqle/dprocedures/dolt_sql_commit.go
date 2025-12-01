package dprocedures

import (
	"context"
	"fmt"
	"io"
	"strings"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// doltSqlCommit executes SQL statement and creates a Dolt commit in the same transaction
func doltSqlCommit(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("usage: CALL dolt_sql_commit('-m', 'message', 'sql_statement')")
	}

	// Parse parameters
	var message, sqlStatement string
	for i := 0; i < len(args); i++ {
		if args[i] == "-m" && i+1 < len(args) {
			message = args[i+1]
			i++
		} else if !strings.HasPrefix(args[i], "-") {
			sqlStatement = args[i]
		}
	}

	if message == "" || sqlStatement == "" {
		return nil, fmt.Errorf("must provide both commit message and SQL statement")
	}

	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return nil, err
	}

	// Get current session and database
	dSess := dsess.DSessFromSess(ctx.Session)
	dbName := ctx.GetCurrentDatabase()

	// Ensure execution in transaction
	// Use a new context for all operations to prevent any context cancellation during SQL execution from affecting the commit
	// We want the entire process (SQL execution + commit) to be atomic and not affected by external cancellation (best effort)
	commitCtx := ctx.WithContext(context.Background())

	// Track if we started the transaction
	transactionStarted := false
	if dSess.GetTransaction() == nil {
		if _, err := dSess.StartTransaction(commitCtx, sql.ReadWrite); err != nil {
			return nil, err
		}
		transactionStarted = true
	}

	// Define rollback function for cleanup on error
	rollbackIfNeeded := func() {
		if transactionStarted {
			// Use new context to ensure rollback can be executed even if original context is canceled
			rollbackCtx := ctx.WithContext(context.Background())
			_ = dSess.Rollback(rollbackCtx, dSess.GetTransaction())
		}
	}

	// Execute SQL statement
	// Use commitCtx directly to avoid creating additional childCtx and defer cancel(), preventing context cancellation error propagation
	err := func() error {
		// Create a new sql.Context for internal query execution
		// Use context.Background() to ensure complete isolation from external context cancellation
		// Using sql.NewContext resets context metadata (like PID), which helps avoid internal queries interfering with external query state
		innerSqlCtx := sql.NewContext(context.Background(), sql.WithSession(dSess))
		innerSqlCtx.SetCurrentDatabase(dbName)

		engine := gms.NewDefault(dSess.Provider())
		// Use innerSqlCtx to execute query
		_, iter, _, err := engine.Query(innerSqlCtx, sqlStatement)
		if err != nil {
			return fmt.Errorf("SQL execution failed: %w", err)
		}
		for {
			_, err := iter.Next(innerSqlCtx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = iter.Close(innerSqlCtx)
				return fmt.Errorf("SQL execution failed during iteration: %w", err)
			}
		}
		if err := iter.Close(innerSqlCtx); err != nil {
			return fmt.Errorf("failed to close iterator: %w", err)
		}
		return nil
	}()

	if err != nil {
		rollbackIfNeeded()
		return nil, err
	}

	// Get current roots
	roots, ok := dSess.GetRoots(commitCtx, dbName)
	if !ok {
		rollbackIfNeeded()
		return nil, fmt.Errorf("Could not load database %s", dbName)
	}

	// Create commit properties, referencing dolt_commit implementation [1](#10-0)
	csp := actions.CommitStagedProps{
		Message:    message,
		Date:       ctx.QueryTime(),
		AllowEmpty: false,
		SkipEmpty:  false,
		Amend:      false,
		Force:      false,
		Name:       ctx.Client().User,
		Email:      fmt.Sprintf("%s@%s", ctx.Client().User, ctx.Client().Address),
	}

	// Stage all tables to ensure SQL changes are included in the commit
	roots, err = actions.StageAllTables(commitCtx, roots, true)
	if err != nil {
		rollbackIfNeeded()
		return nil, fmt.Errorf("failed to stage tables: %w", err)
	}

	pendingCommit, err := dSess.NewPendingCommit(commitCtx, dbName, roots, csp)
	if err != nil {
		rollbackIfNeeded()
		return nil, fmt.Errorf("failed to create pending commit: %w", err)
	}

	if pendingCommit == nil {
		rollbackIfNeeded()
		return nil, fmt.Errorf("nothing to commit")
	}

	// Call DoltCommit (this will complete both SQL transaction commit and Dolt commit creation)
	// Key: The DoltCommit method itself contains atomic transaction commit logic [2](#10-1)
	newCommit, err := dSess.DoltCommit(commitCtx, dbName, dSess.GetTransaction(), pendingCommit)
	if err != nil {
		rollbackIfNeeded()
		return nil, fmt.Errorf("failed to create dolt commit: %w", err)
	}

	hash, err := newCommit.HashOf()
	if err != nil {
		return nil, err
	}

	return rowToIter(hash.String()), nil
}
