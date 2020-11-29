package dfunctions

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
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
	forceFlag        = "force"
	authorParam      = "author"
)

func createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(commitMessageArg, "m", "msg", "Use the given {{.LessThan}}msg{{.GreaterThan}} as the commit message.")
	ap.SupportsFlag(allowEmptyFlag, "", "Allow recording a commit that has the exact same data as its sole parent. This is usually a mistake, so it is disabled by default. This option bypasses that safety.")
	ap.SupportsString(dateParam, "", "date", "Specify the date used in the commit. If not specified the current system time is used.")
	ap.SupportsFlag(forceFlag, "f", "Ignores any foreign key warnings and proceeds with the commit.")
	ap.SupportsString(authorParam, "", "author", "Specify an explicit author using the standard A U Thor <author@example.com> format.")
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

func parseAuthor(authorStr string) (string, string, error) {
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

// Trims the double quotes for the param/
func trimQuotes(param string) string {
	if len(param) > 0 && param[0] == '"' {
		param = param[1:]
	}

	if len(param) > 0 && param[len(param)-1] == '"' {
		param = param[:len(param)-1]
	}

	return param
}

func (d DoltCommitFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Get the information for the sql context.
	dbName := ctx.GetCurrentDatabase()
	dSess := sqle.DSessFromSess(ctx.Session)

	doltdb, dok := dSess.GetDoltDB(dbName)

	if !dok {
		return nil, fmt.Errorf("Could not load %s", dbName)
	}

	rsr, rsrok := dSess.GetDoltDBRepoStateReader(dbName)

	if !rsrok {
		return nil, fmt.Errorf("Could not load the %s RepoStateReader", dbName)
	}

	rsw, rswok := dSess.GetDoltDBRepoStateWriter(dbName)

	if !rswok {
		return nil, fmt.Errorf("Could not load the %s RepoStateWriter", dbName)
	}

	ap := createArgParser()

	// Get the args for DOLT_COMMIT.
	args := make([]string, 1)
	for i := range d.children {
		temp := d.children[i].String()
		str := trimQuotes(temp)
		args = append(args, str)
	}

	apr := cli.ParseArgs(ap, args, nil)

	// Parse the author flag. Return an error if not.
	var name, email string
	var err error
	if authorStr, ok := apr.GetValue(authorParam); ok {
		name, email, err = parseAuthor(authorStr)
		// TODO: Set name and email in cli if not set????
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("Must specify author flag.")
	}

	// Get the commit message.
	msg, msgOk := apr.GetValue(commitMessageArg)
	if !msgOk {
		return nil, fmt.Errorf("Must provide commit message.")
	}

	// Specify the time if the date parameter is not.
	t := time.Now()
	if commitTimeStr, ok := apr.GetValue(dateParam); ok {
		var err error
		t, err = parseDate(commitTimeStr)

		if err != nil {
			return nil, fmt.Errorf(err.Error())
		}
	}

	err = actions.CommitStaged(ctx, doltdb, rsr, rsw, actions.CommitStagedProps{
		Message:          msg,
		Date:             t,
		AllowEmpty:       apr.Contains(allowEmptyFlag),
		CheckForeignKeys: !apr.Contains(forceFlag),
		Name:             name,
		Email:            email,
	})

	return "Commit Staged.", err
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
	return true
}

func (d DoltCommitFunc) Children() []sql.Expression {
	return d.children
}
