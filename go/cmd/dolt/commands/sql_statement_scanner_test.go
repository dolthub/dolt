package commands

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestScanStatements(t *testing.T) {
	type testcase struct {
		input string
		statements []string
	}

	// Some of these include malformed input (e.g. strings that aren't properly terminated)
	testcases := []testcase {
		{
			input: `insert into foo values (";;';'");`,
			statements: []string{
				`insert into foo values (";;';'")`,
			},
		},
		{
			input: `select ''';;'; select ";\;"`,
			statements: []string{
				`select ''';;'`,
				`select ";\;"`,
			},
		},
		{
			input: `select ''';;'; select ";\;`,
			statements: []string{
				`select ''';;'`,
				`select ";\;`,
			},
		},
		{
			input: `select '\\'''; select '";";'; select 1`,
			statements: []string{
				`select '\\'''`,
				`select '";";'`,
				`select 1`,
			},
		},
		{
			input: `select '\\''; select '";";'; select 1`,
			statements: []string{
				`select '\\''; select '";"`,
				`'; select 1`,
			},
		},
		{
			input: `insert into foo values(''); select 1`,
			statements: []string{
				`insert into foo values('')`,
				`select 1`,
			},
		},
		{
			input: `insert into foo values('''); select 1`,
			statements: []string{
				`insert into foo values('''); select 1`,
			},
		},
		{
			input: `insert into foo values(''''); select 1`,
			statements: []string{
				`insert into foo values('''')`,
				`select 1`,
			},
		},
		{
			input: `insert into foo values(""); select 1`,
			statements: []string{
				`insert into foo values("")`,
				`select 1`,
			},
		},
		{
			input: `insert into foo values("""); select 1`,
			statements: []string{
				`insert into foo values("""); select 1`,
			},
		},
		{
			input: `insert into foo values(""""); select 1`,
			statements: []string{
				`insert into foo values("""")`,
				`select 1`,
			},
		},
		{
			input: `select '\''; select "hell\"o"`,
			statements: []string{
				`select '\''`,
				`select "hell\"o"`,
			},
		},
		{
			input: `select * from foo; select baz from foo;
select
a from b; select 1`,
			statements: []string{
				"select * from foo",
				"select baz from foo",
				"select\na from b",
				"select 1",
			},
		},
		{
			input: `insert into foo values ('a', "b;", 'c;;""
'); update foo set baz = bar,
qux = '"hello"""' where xyzzy = ";;';'";
create table foo (a int not null default ';',
primary key (a));`,
			statements: []string{
				`insert into foo values ('a', "b;", 'c;;""
')`,
				`update foo set baz = bar,
qux = '"hello"""' where xyzzy = ";;';'"`,
				`create table foo (a int not null default ';',
primary key (a))`,
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.input, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			scanner := NewSqlStatementScanner(reader)
			var i int
			for scanner.Scan() {
				require.True(t, i < len(tt.statements))
				assert.Equal(t, tt.statements[i], strings.TrimSpace(scanner.Text()))
				i++
			}

			require.NoError(t, scanner.Err())
		})
	}
}