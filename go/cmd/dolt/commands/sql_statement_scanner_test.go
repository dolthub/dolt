// Copyright 2020 Dolthub, Inc.
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

package commands

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanStatements(t *testing.T) {
	type testcase struct {
		input      string
		statements []string
		lineNums   []int
	}

	// Some of these include malformed input (e.g. strings that aren't properly terminated)
	testcases := []testcase{
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
			input: `select ''';;'; select ";\;
;`,
			statements: []string{
				`select ''';;'`,
				`select ";\;
;`,
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
			lineNums: []int{
				1, 1, 2, 3,
			},
		},
		{
			input: "create table dumb (`hell\\`o;` int primary key);",
			statements: []string{
				"create table dumb (`hell\\`o;` int primary key)",
			},
		},
		{
			input: "create table dumb (`hell``o;` int primary key); select \n" +
				"baz from foo;\n" +
				"\n" +
				"select\n" +
				"a from b; select 1\n\n",
			statements: []string{
				"create table dumb (`hell``o;` int primary key)",
				"select \nbaz from foo",
				"select\na from b",
				"select 1",
			},
			lineNums: []int{
				1, 1, 4, 5,
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
			lineNums: []int{
				1, 2, 6,
			},
		},
		{
			input: `DELIMITER |
insert into foo values (1,2,3)|`,
			statements: []string{
				"",
				"insert into foo values (1,2,3)",
			},
			lineNums: []int{1, 2},
		},
		{
			// https://github.com/dolthub/dolt/issues/10828
			input: `-- comment asdfasdf
		delimiter //
		select current_user() //`,
			statements: []string{
				"",
				"",
				"select current_user()",
			},
			lineNums: []int{1, 2, 3},
		},
		{
			// https://github.com/dolthub/dolt/issues/8495
			input: strings.Repeat(" ", 4096) + `insert into foo values (1,2,3)`,
			statements: []string{
				"insert into foo values (1,2,3)",
			},
			lineNums: []int{1, 2},
		},
		{
			input: "DELIMITER" + strings.Repeat(" ", 4096) + `|
insert into foo values (1,2,3)|`,
			statements: []string{
				"",
				"insert into foo values (1,2,3)",
			},
			lineNums: []int{1, 2},
		},
		{
			// https://github.com/dolthub/dolt/issues/10694
			input: `-- '
-- can have intermediate comments
CALL dolt_commit('-m', 'message', '--allow-empty');
CALL dolt_checkout('main');`,
			statements: []string{
				"", "",
				"CALL dolt_commit('-m', 'message', '--allow-empty')",
				"CALL dolt_checkout('main')",
			},
			lineNums: []int{1, 2, 3, 4},
		},
		{
			input: `/* block comment with lone quote '
*/
-- can have intermediate comments
CALL dolt_commit('-m', 'message', '--allow-empty');
CALL dolt_checkout('main');`,
			statements: []string{
				`/* block comment with lone quote '
*/
-- can have intermediate comments
CALL dolt_commit('-m', 'message', '--allow-empty')`,
				"CALL dolt_checkout('main')",
			},
			lineNums: []int{1, 5},
		},
		{
			input: `select * /* -- ignore line comment inside block comment */ from xy;
select x from xy; -- select y from xy;
select * /* ignore multi-line comment with ;
comment;
comment;
*/ from foo;
select '-- ignore line comment
in quote';`,
			statements: []string{
				"select * /* -- ignore line comment inside block comment */ from xy",
				"select x from xy",
				"",
				`select * /* ignore multi-line comment with ;
comment;
comment;
*/ from foo`,
				`select '-- ignore line comment
in quote'`,
			},
			lineNums: []int{1, 2, 2, 3, 7},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.input, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			scanner := NewStreamScanner(reader)
			var i int
			for scanner.Scan() {
				require.True(t, i < len(tt.statements))
				assert.Equal(t, tt.statements[i], strings.TrimSpace(scanner.Text()))
				if tt.lineNums != nil {
					assert.Equal(t, tt.lineNums[i], scanner.state.statementStartLine)
				} else {
					assert.Equal(t, 1, scanner.state.statementStartLine)
				}
				i++
			}

			require.NoError(t, scanner.Err())
		})
	}
}

// TestEndedAtEOFFlag covers the per-Scan termination distinction that
// IsShellInputComplete relies on. A delimiter-terminated statement must
// leave EndedAtEOF false; an unterminated trailing statement must leave it
// true.
func TestEndedAtEOFFlag(t *testing.T) {
	t.Run("delimiter terminated", func(t *testing.T) {
		scanner := NewStreamScanner(strings.NewReader("select 1;"))
		require.True(t, scanner.Scan())
		assert.False(t, scanner.EndedAtEOF())
		assert.False(t, scanner.Scan())
	})
	t.Run("unterminated EOF", func(t *testing.T) {
		scanner := NewStreamScanner(strings.NewReader("select 1"))
		require.True(t, scanner.Scan())
		assert.True(t, scanner.EndedAtEOF())
		assert.False(t, scanner.Scan())
	})
	t.Run("flag resets across scans", func(t *testing.T) {
		// First statement terminates; second hits EOF unterminated.
		scanner := NewStreamScanner(strings.NewReader("select 1; select 2"))
		require.True(t, scanner.Scan())
		assert.False(t, scanner.EndedAtEOF())
		require.True(t, scanner.Scan())
		assert.True(t, scanner.EndedAtEOF())
	})
}

// TestIsShellInputComplete covers the interactive-shell completion
// predicate. Each case captures one branch of the four bug scenarios
// referenced by dolthub/dolt#10866, plus the DELIMITER and pure-comment
// edges that StreamScanner emits as empty tokens. The delimiter argument
// mirrors the shell's active DELIMITER; most cases use the default ";".
func TestIsShellInputComplete(t *testing.T) {
	testcases := []struct {
		name      string
		input     string
		delimiter string
		want      bool
	}{
		{"empty", "", ";", true},
		{"whitespace only", "   ", ";", true},
		{"only newlines", "\n\n", ";", true},
		{"unterminated single statement", "select 1", ";", false},
		{"terminated single statement", "select 1;", ";", true},
		{"terminated multi statement (#10860)", "select null; select null;", ";", true},
		{"trailing unterminated multi", "select 1; select 2", ";", false},
		{"open single quote (#10861)", "select '", ";", false},
		{"open double quote (#10861)", "select \"", ";", false},
		{"open backtick", "select `", ";", false},
		{"semicolon inside open quote (#10861)", "select ';", ";", false},
		{"closed quote then delimiter", "select 'foo';", ";", true},
		{"backslash-escaped quote leaves string open", "select '\\';", ";", false},
		{"doubled backslash closes string", "select '\\\\';", ";", true},
		{"open block comment (#10862)", "select /* ;", ";", false},
		{"closed block comment then delimiter", "select /* x */ null;", ";", true},
		{"line comment with semicolon and no newline", "select -- ;", ";", false},
		{"line comment with newline then terminator", "select -- ;\nnull;", ";", true},
		{"DELIMITER alone", "DELIMITER //", ";", true},
		{"DELIMITER then terminated", "DELIMITER //\nselect 1//", ";", true},
		{"DELIMITER then unterminated", "DELIMITER //\nselect 1", ";", false},
		{"pure line comment input", "-- hello\n", ";", true},
		// A pure block comment with no trailing terminator produces a
		// non-empty trailing token at EOF (StreamScanner emits empty tokens
		// only for pure line comments). The shell waits for the user to
		// type a delimiter, matching MySQL's behavior after a comment.
		{"pure block comment input", "/* hello */\n", ";", false},

		// Non-";" delimiter cases (DELIMITER active in the shell). The
		// predicate sees the cumulative buffer with the delimiter still
		// present (the callback strips the trailing delimiter only after
		// ishell delivers the buffer). Under a custom delimiter, internal ";"
		// must not be treated as terminators; only the custom delimiter
		// completes a statement.
		{"custom delim: trigger body still being typed is incomplete",
			"CREATE TRIGGER t BEFORE INSERT ON x FOR EACH ROW\nBEGIN\nSET NEW.v = 1;\nSET NEW.v = 2;\nEND;", "#", false},
		{"custom delim: trigger body completed by custom delimiter",
			"CREATE TRIGGER t BEFORE INSERT ON x FOR EACH ROW\nBEGIN\nSET NEW.v = 1;\nSET NEW.v = 2;\nEND; #", "#", true},
		{"custom delim: single statement terminated by custom delimiter",
			"select 1 #", "#", true},
		{"custom delim: unterminated single statement", "select 1", "#", false},
		{"custom delim: multi-char // unterminated", "select 1", "//", false},
		{"custom delim: multi-char // terminated", "select 1 //", "//", true},
		{"custom delim: empty stays complete", "", "#", true},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			got := IsShellInputComplete(tt.input, tt.delimiter)
			assert.Equal(t, tt.want, got, "input=%q delimiter=%q", tt.input, tt.delimiter)
		})
	}
}
