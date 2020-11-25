package dfunctions

import (
	"errors"
	"fmt"
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

	msg, msgOk := apr.GetValue(commitMessageArg)
	if !msgOk {
		msg = "This is vinai's commit"
		//return nil, fmt.Errorf("Must provide commit message.")
	}

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

	msg = "This is vinai's commit"
	err := actions.CommitStaged(ctx, doltdb, rsr, rsw, actions.CommitStagedProps{
		Message:          msg,
		Date:             t,
		AllowEmpty:       apr.Contains(allowEmptyFlag),
		CheckForeignKeys: !apr.Contains(forceFlag),
		Name: "John",
		Email: "John@doe.com",
	})

	return "", err
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
