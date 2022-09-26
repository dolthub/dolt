//TODO: DELETE ME LATER, FOR TESTING PURPOSES ONLY

package temp_branch_control

import (
	"context"
	"fmt"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Permissions are a set of flags that denote a user's allowed functionality on a branch.
type Permissions uint64

const ( // Make these explicit after settling on which permissions to actually use
	Permissions_Admin Permissions = 1 << iota
	Permissions_Write
	Permissions_Destroy
	Permissions_Merge
	Permissions_Branch
	Permissions_All Permissions = (Permissions_Branch << 1) - 1 // Update when new permissions are added
)

// To prevent import cycles, try and rely on as few Dolt packages as possible so that this may be called from as many
// places as possible
type BranchAwareSession interface {
	sql.Session
	GetBranch() string
	GetUser() string
	GetHost() string
}

// Slice to hold nodes is temporary, will decide a better format later
type Access struct {
	Nodes   []AccessNode
	RWMutex *sync.RWMutex
}

type Creation struct {
	Nodes   []CreationNode
	RWMutex *sync.RWMutex
}

// Temporary global variables to aid with development
var StaticAccess = &Access{nil, &sync.RWMutex{}}
var StaticCreation = &Creation{nil, &sync.RWMutex{}}

// I'm using the `LikeMatcher` here as it allows for the "username%" semantics that we like, but it doesn't support
// any kind of longest-match semantics, so I'll either extend this or come up with something better
type AccessNode struct {
	Branch      expression.LikeMatcher
	User        expression.LikeMatcher
	Host        expression.LikeMatcher
	Permissions Permissions
}

type CreationNode struct {
	Branch expression.LikeMatcher
	User   expression.LikeMatcher
	Host   expression.LikeMatcher
}

func mustConstructLikeMatcher(collation sql.CollationID, pattern string) expression.LikeMatcher {
	lm, err := expression.ConstructLikeMatcher(collation, pattern, '\\')
	if err != nil {
		panic(err)
	}
	return lm
}

// Temporary init to aid development
func init() {
	StaticAccess.Nodes = append(StaticAccess.Nodes, AccessNode{
		Branch:      mustConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, `%`),
		User:        mustConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, "root"),
		Host:        mustConstructLikeMatcher(sql.Collation_utf8mb4_0900_ai_ci, "%"),
		Permissions: Permissions_All,
	})
	StaticCreation.Nodes = append(StaticCreation.Nodes, CreationNode{
		Branch: mustConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, `%`),
		User:   mustConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, "root"),
		Host:   mustConstructLikeMatcher(sql.Collation_utf8mb4_0900_ai_ci, "%"),
	})
}

// CheckAccess returns whether the given context has the correct permissions on its selected branch. In general, SQL
// statements will almost always return a *sql.Context, so any checks from the SQL path will correctly check for branch
// permissions. However, not all CLI commands use *sql.Context, and therefore will not have any user associated with
// the context. In these cases, CheckAccess will pass as we want to allow all local commands to ignore branch
// permissions.
func CheckAccess(ctx context.Context, flags Permissions) error {
	branchAwareSession := getBranchAwareSession(ctx)
	if branchAwareSession == nil {
		return nil
	}
	StaticAccess.RWMutex.RLock()
	defer StaticAccess.RWMutex.RUnlock()

	branch := branchAwareSession.GetBranch()
	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	// Won't actually loop over everything, not performant at all
	for _, node := range StaticAccess.Nodes {
		// This doesn't apply the "longest match" rule
		if node.Branch.Match(branch) && node.User.Match(user) && node.Host.Match(host) {
			if (node.Permissions&flags == flags) || (node.Permissions&Permissions_Admin > 0) {
				return nil
			}
			return fmt.Errorf("`%s`@`%s` does not have the correct permissions on branch `%s`", user, host, branch)
		}
	}
	return fmt.Errorf("`%s`@`%s` does not have the correct permissions on branch `%s`", user, host, branch)
}

// CanCreateBranch returns whether the given context can create a branch with the given name. In general, SQL statements
// will almost always return a *sql.Context, so any checks from the SQL path will be able to validate a branch's name.
// However, not all CLI commands use *sql.Context, and therefore will not have any user associated with the context. In
// these cases, CanCreateBranch will pass as we want to allow all local commands to freely create branches.
func CanCreateBranch(ctx context.Context, branchName string) error {
	branchAwareSession := getBranchAwareSession(ctx)
	if branchAwareSession == nil {
		return nil
	}
	StaticCreation.RWMutex.RLock()
	defer StaticCreation.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	// As this doesn't handle the "longest match" rule, we just iterate over all matches and see if any of them work
	matchFound := false
	for _, node := range StaticCreation.Nodes {
		if node.Branch.Match(branchName) {
			matchFound = true
			if node.User.Match(user) && node.Host.Match(host) {
				return nil
			}
		}
	}
	if matchFound {
		return fmt.Errorf("`%s`@`%s` cannot create a branch named `%s`", user, host, branchName)
	}
	return nil
}

// CanDeleteBranch returns whether the given context can delete a branch with the given name. In general, SQL statements
// will almost always return a *sql.Context, so any checks from the SQL path will be able to validate a branch's name.
// However, not all CLI commands use *sql.Context, and therefore will not have any user associated with the context. In
// these cases, CanDeleteBranch will pass as we want to allow all local commands to freely delete branches.
func CanDeleteBranch(ctx context.Context, branchName string) error {
	branchAwareSession := getBranchAwareSession(ctx)
	if branchAwareSession == nil {
		return nil
	}
	StaticAccess.RWMutex.RLock()
	defer StaticAccess.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	for _, node := range StaticAccess.Nodes {
		if node.Branch.Match(branchName) && node.User.Match(user) && node.Host.Match(host) {
			if (node.Permissions&Permissions_Destroy > 0) || (node.Permissions&Permissions_Admin > 0) {
				return nil
			}
			return fmt.Errorf("`%s`@`%s` cannot delete the branch `%s`", user, host, branchName)
		}
	}
	return fmt.Errorf("`%s`@`%s` cannot delete the branch `%s`", user, host, branchName)
}

// getBranchAwareSession returns the session contained within the context. If the context does NOT contain a session,
// then nil is returned.
func getBranchAwareSession(ctx context.Context) BranchAwareSession {
	if sqlCtx, ok := ctx.(*sql.Context); ok {
		if bas, ok := sqlCtx.Session.(BranchAwareSession); ok {
			return bas
		}
	} else if bas, ok := ctx.(BranchAwareSession); ok {
		return bas
	}
	return nil
}
