package dfunctions

import (
	"fmt"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

const DoltCommitFuncName = "dolt_commit"

type DoltCommitFunc struct {
	children []sql.Expression
}

// NewDoltCommitFunc creates a new DoltCommitFunc expression.
func NewDoltCommitFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltCommitFunc{children: args}, nil
}

const (
	allowEmptyFlag   = "allow-empty"
	dateParam        = "date"
	commitMessageArg = "message"
	forceFlag		 = "force"
)

func createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(commitMessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the commit message.")
	ap.SupportsFlag(allowEmptyFlag, "", "Allow recording a commit that has the exact same data as its sole parent. This is usually a mistake, so it is disabled by default. This option bypasses that safety.")
	ap.SupportsString(dateParam, "", "date", "Specify the date used in the commit. If not specified the current system time is used.")
	ap.SupportsFlag(forceFlag, "f", "Ignores any foreign key warnings and proceeds with the commit.")
	return ap
}


func (d DoltCommitFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	//ap := createArgParser()


	// TODO: Ask connection limit here what's up (autocommit disableb? )

	// TODO: Help and usage?

	// Get the args from the children
	//var args []string
	//for i := range d.children {
	//	str := d.children[i].String() // TODO: Do we need to eval here?
	//	args = append(args, str)
	//}
	//
	//apr := cli.ParseArgs(ap, args, nil) // TODO: Fix usage printer
	//
	//msg, msgOk := apr.GetValue(commitMessageArg)
	//if !msgOk {
	//	return nil, fmt.Errorf("Must provide commit message.")
	//}
	//
	//t := time.Now()
	//if commitTimeStr, ok := apr.GetValue(dateParam); ok {
	//	var err error
	//	t, err = commands.ParseDate(commitTimeStr)
	//
	//	if err != nil {
	//		return nil, fmt.Errorf(err.Error())
	//	}
	//}
	//
	//dbName := ctx.GetCurrentDatabase()
	//dSess := sqle.DSessFromSess(ctx.Session)

	// sessions have different dbs. Have different ways of updating. You need to to figure out that
	// interaction. Why are we not using denv instead of RepoStateWriter.

	// Might need to implement new methods to commit data. After changing methods, change actions
	// to take in a repo state writer instead of an environment.

	//db, _ := dSess.GetDoltDB(dbName)
	//
	//err := actions.CommitStaged(ctx, dEnv, actions.CommitStagedProps{
	//	Message:          msg,
	//	Date:             t,
	//	AllowEmpty:       apr.Contains(allowEmptyFlag),
	//	CheckForeignKeys: !apr.Contains(forceFlag),
	//})

	// watch how code based from sql engine through debugger all the way to dolt commit

	// give myself a couple days. Want to get in with laser focus.
	// Spend a couple of days top down. Need to better understand the code base.


	// dolt and mysql server are totally. Go-mysql-server is a query planner. The engine has a
	// way to say this has a schema.

	// core.go in go-mysql-interface (Read this!). Wants to integrate with the query engine
	// mysql layer sits on a dolt layer. Dolt implements go-mysql-layer. One capability is
	// defining user function. Way to plug in that. Defining these custom functions that hook into
	// go-mysql-server.

	// dolt layer is independent of the sql layer .

	// 1. First job is to a deep dive into a command line function. Look at what happens. See what
	// happens when you run these things. Get a feel for how these works. Understand this layer and can't do anything
	// above.

	/*
	 * How they change from above them. We'll
	 */


	// want to flush things to disk.

	return "", nil
}

func (d DoltCommitFunc) String() string {
	childrenStrings := make([]string, len(d.children))

	for _, child := range d.children {
		childrenStrings = append(childrenStrings, child.String())
	}
	return fmt.Sprintf("DOLT_COMMIT(%s)", strings.Join(childrenStrings, " "))
}

func (d DoltCommitFunc) Type() sql.Type {
	return sql.Text
}

func (d DoltCommitFunc) IsNullable() bool {
	return false
}

func (d DoltCommitFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltCommitFunc(children...)
}

func (d DoltCommitFunc) Resolved() bool {
	//for _, child := range d.children {
	//	if !child.Resolved() {
	//		return false
	//	}
	//}
	return true
}

func (d DoltCommitFunc) Children() []sql.Expression {
	return d.children
}
