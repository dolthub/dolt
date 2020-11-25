package dfunctions

import (
	"fmt"
	"errors"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"time"

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


// we are more permissive than what is documented.
var supportedLayouts = []string{
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

func parseDate(dateStr string) (time.Time, error) {
	for _, layout := range supportedLayouts {
		t, err := time.Parse(layout, dateStr)

		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, errors.New("error: '" + dateStr + "' is not in a supported format.")
}

func (d DoltCommitFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	ap := createArgParser()

	//// Get the args from the children
	var args []string
	for i := range d.children {
		str := d.children[i].String() // TODO: Do we need to eval here?
		args = append(args, str)
	}

	cli.Println(args)

	apr := cli.ParseArgs(ap, args, nil) // TODO: Fix usage printer

	//msg, msgOk := apr.GetValue(commitMessageArg)
	//if !msgOk {
	//	return nil, fmt.Errorf("Must provide commit message.")
	//}

	t := time.Now()
	if commitTimeStr, ok := apr.GetValue(dateParam); ok {
		var err error
		t, err = parseDate(commitTimeStr)

		if err != nil {
			return nil, fmt.Errorf(err.Error())
		}
	}


	dbName := ctx.GetCurrentDatabase()
	dSess := sqle.DSessFromSess(ctx.Session)

	doltdb, _ := dSess.GetDoltDB(dbName)
	rsr, _ := dSess.GetDoltDBRepoStateReader(dbName)
	rsw, _ := dSess.GetDoltDBRepoStateWriter(dbName)

	err := actions.CommitStaged(ctx, doltdb, rsr, rsw, actions.CommitStagedProps{
		Message:          "Here is a commit",
		Date:             t,
		AllowEmpty:       apr.Contains(allowEmptyFlag),
		CheckForeignKeys: !apr.Contains(forceFlag),
		Name: "John",
		Email: "John@doe.com",
	})

	if err != nil {
		return err, fmt.Errorf(err.Error())
	}

	// sessions have different dbs. Have different ways of updating. You need to to figure out that
	// interaction. Why are we not using denv instead of RepoStateWriter.

	// Might need to implement new methods to commit data. After changing methods, change actions
	// to take in a repo state writer instead of an environment.

	//db, _ := dSess.GetDoltDB(dbName)
	//

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

	return "Committed this", err
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
