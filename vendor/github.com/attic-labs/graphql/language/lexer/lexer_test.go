package lexer

import (
	"reflect"
	"testing"

	"github.com/attic-labs/graphql/language/source"
)

type Test struct {
	Body     string
	Expected interface{}
}

func createSource(body string) *source.Source {
	return source.NewSource(&source.Source{Body: []byte(body)})
}

func TestLexer_GetTokenDesc(t *testing.T) {
	expected := `Name "foo"`
	tokenDescription := GetTokenDesc(Token{
		Kind:  TokenKind[NAME],
		Start: 2,
		End:   5,
		Value: "foo",
	})
	if expected != tokenDescription {
		t.Errorf("Expected %v, got %v", expected, tokenDescription)
	}

	expected = `Name`
	tokenDescription = GetTokenDesc(Token{
		Kind:  TokenKind[NAME],
		Start: 0,
		End:   0,
		Value: "",
	})
	if expected != tokenDescription {
		t.Errorf("Expected %v, got %v", expected, tokenDescription)
	}

	expected = `String "foo"`
	tokenDescription = GetTokenDesc(Token{
		Kind:  TokenKind[STRING],
		Start: 2,
		End:   5,
		Value: "foo",
	})
	if expected != tokenDescription {
		t.Errorf("Expected %v, got %v", expected, tokenDescription)
	}

	expected = `String`
	tokenDescription = GetTokenDesc(Token{
		Kind:  TokenKind[STRING],
		Start: 0,
		End:   0,
		Value: "",
	})
	if expected != tokenDescription {
		t.Errorf("Expected %v, got %v", expected, tokenDescription)
	}

}

func TestLexer_DisallowsUncommonControlCharacters(t *testing.T) {
	tests := []Test{
		{
			Body: "\u0007",
			Expected: `Syntax Error GraphQL (1:1) Invalid character "\\u0007"

1: \u0007
   ^
`,
		},
	}
	for _, test := range tests {
		_, err := Lex(createSource(test.Body))(0)
		if err == nil {
			t.Errorf("unexpected nil error\nexpected:\n%v\n\ngot:\n%v", test.Expected, err)
		}
		if err.Error() != test.Expected {
			t.Errorf("unexpected error.\nexpected:\n%v\n\ngot:\n%v", test.Expected, err.Error())
		}
	}
}

func TestLexer_AcceptsBOMHeader(t *testing.T) {
	tests := []Test{
		{
			Body: "\uFEFF foo",
			Expected: Token{
				Kind:  TokenKind[NAME],
				Start: 2,
				End:   5,
				Value: "foo",
			},
		},
	}
	for _, test := range tests {
		token, err := Lex(&source.Source{Body: []byte(test.Body)})(0)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(token, test.Expected) {
			t.Errorf("unexpected token, expected: %v, got: %v", test.Expected, token)
		}
	}
}

func TestLexer_SkipsWhiteSpace(t *testing.T) {
	tests := []Test{
		{
			Body: `

    foo

`,
			Expected: Token{
				Kind:  TokenKind[NAME],
				Start: 6,
				End:   9,
				Value: "foo",
			},
		},
		{
			Body: `
    #comment
    foo#comment
`,
			Expected: Token{
				Kind:  TokenKind[NAME],
				Start: 18,
				End:   21,
				Value: "foo",
			},
		},
		{
			Body: `,,,foo,,,`,
			Expected: Token{
				Kind:  TokenKind[NAME],
				Start: 3,
				End:   6,
				Value: "foo",
			},
		},
		{
			Body: ``,
			Expected: Token{
				Kind:  TokenKind[EOF],
				Start: 0,
				End:   0,
				Value: "",
			},
		},
	}
	for _, test := range tests {
		token, err := Lex(&source.Source{Body: []byte(test.Body)})(0)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(token, test.Expected) {
			t.Errorf("unexpected token, expected: %v, got: %v, body: %s", test.Expected, token, test.Body)
		}
	}
}

func TestLexer_ErrorsRespectWhitespace(t *testing.T) {
	body := `

    ?

`
	_, err := Lex(createSource(body))(0)
	expected := "Syntax Error GraphQL (3:5) Unexpected character \"?\".\n\n2: \n3:     ?\n       ^\n4: \n"
	if err == nil {
		t.Fatalf("unexpected nil error\nexpected:\n%v\n\ngot:\n%v", expected, err)
	}
	if err.Error() != expected {
		t.Fatalf("unexpected error.\nexpected:\n%v\n\ngot:\n%v", expected, err.Error())
	}
}

func TestLexer_LexesNames(t *testing.T) {
	tests := []Test{
		{
			Body: "simple",
			Expected: Token{
				Kind:  TokenKind[NAME],
				Start: 0,
				End:   6,
				Value: "simple",
			},
		},
		{
			Body: "Capital",
			Expected: Token{
				Kind:  TokenKind[NAME],
				Start: 0,
				End:   7,
				Value: "Capital",
			},
		},
	}
	for _, test := range tests {
		token, err := Lex(&source.Source{Body: []byte(test.Body)})(0)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(token, test.Expected) {
			t.Errorf("unexpected token, expected: %v, got: %v", test.Expected, token)
		}
	}
}

func TestLexer_LexesStrings(t *testing.T) {
	tests := []Test{
		{
			Body: "\"simple\"",
			Expected: Token{
				Kind:  TokenKind[STRING],
				Start: 0,
				End:   8,
				Value: "simple",
			},
		},
		{
			Body: "\" white space \"",
			Expected: Token{
				Kind:  TokenKind[STRING],
				Start: 0,
				End:   15,
				Value: " white space ",
			},
		},
		{
			Body: "\"quote \\\"\"",
			Expected: Token{
				Kind:  TokenKind[STRING],
				Start: 0,
				End:   10,
				Value: `quote "`,
			},
		},
		{
			Body: "\"escaped \\n\\r\\b\\t\\f\"",
			Expected: Token{
				Kind:  TokenKind[STRING],
				Start: 0,
				End:   20,
				Value: "escaped \n\r\b\t\f",
			},
		},
		{
			Body: "\"slashes \\\\ \\/\"",
			Expected: Token{
				Kind:  TokenKind[STRING],
				Start: 0,
				End:   15,
				Value: "slashes \\ /",
			},
		},
		{
			Body: "\"unicode \\u1234\\u5678\\u90AB\\uCDEF\"",
			Expected: Token{
				Kind:  TokenKind[STRING],
				Start: 0,
				End:   34,
				Value: "unicode \u1234\u5678\u90AB\uCDEF",
			},
		},
		{
			Body: "\"unicode фы世界\"",
			Expected: Token{
				Kind:  TokenKind[STRING],
				Start: 0,
				End:   20,
				Value: "unicode фы世界",
			},
		},
		{
			Body: "\"фы世界\"",
			Expected: Token{
				Kind:  TokenKind[STRING],
				Start: 0,
				End:   12,
				Value: "фы世界",
			},
		},
		{
			Body: "\"Has a фы世界 multi-byte character.\"",
			Expected: Token{
				Kind:  TokenKind[STRING],
				Start: 0,
				End:   40,
				Value: "Has a фы世界 multi-byte character.",
			},
		},
	}
	for _, test := range tests {
		token, err := Lex(&source.Source{Body: []byte(test.Body)})(0)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(token, test.Expected) {
			t.Errorf("unexpected token, expected: %v, got: %v", test.Expected, token)
		}
	}
}

func TestLexer_ReportsUsefulStringErrors(t *testing.T) {
	tests := []Test{
		{
			Body: "\"",
			Expected: `Syntax Error GraphQL (1:2) Unterminated string.

1: "
    ^
`,
		},
		{
			Body: "\"no end quote",
			Expected: `Syntax Error GraphQL (1:14) Unterminated string.

1: "no end quote
                ^
`,
		},
		{
			Body: "\"contains unescaped \u0007 control char\"",
			Expected: `Syntax Error GraphQL (1:21) Invalid character within String: "\\u0007".

1: "contains unescaped \u0007 control char"
                       ^
`,
		},
		{
			Body: "\"null-byte is not \u0000 end of file\"",
			Expected: `Syntax Error GraphQL (1:19) Invalid character within String: "\\u0000".

1: "null-byte is not \u0000 end of file"
                     ^
`,
		},
		{
			Body: "\"multi\nline\"",
			Expected: `Syntax Error GraphQL (1:7) Unterminated string.

1: "multi
         ^
2: line"
`,
		},
		{
			Body: "\"multi\rline\"",
			Expected: `Syntax Error GraphQL (1:7) Unterminated string.

1: "multi
         ^
2: line"
`,
		},
		{
			Body: "\"bad \\z esc\"",
			Expected: `Syntax Error GraphQL (1:7) Invalid character escape sequence: \\z.

1: "bad \z esc"
         ^
`,
		},
		{
			Body: "\"bad \\x esc\"",
			Expected: `Syntax Error GraphQL (1:7) Invalid character escape sequence: \\x.

1: "bad \x esc"
         ^
`,
		},
		{
			Body: "\"bad \\u1 esc\"",
			Expected: `Syntax Error GraphQL (1:7) Invalid character escape sequence: \u1 es

1: "bad \u1 esc"
         ^
`,
		},
		{
			Body: "\"bad \\u0XX1 esc\"",
			Expected: `Syntax Error GraphQL (1:7) Invalid character escape sequence: \u0XX1

1: "bad \u0XX1 esc"
         ^
`,
		},
		{
			Body: "\"bad \\uXXXX esc\"",
			Expected: `Syntax Error GraphQL (1:7) Invalid character escape sequence: \uXXXX

1: "bad \uXXXX esc"
         ^
`,
		},
		{
			Body: "\"bad \\uFXXX esc\"",
			Expected: `Syntax Error GraphQL (1:7) Invalid character escape sequence: \uFXXX

1: "bad \uFXXX esc"
         ^
`,
		},
		{
			Body: "\"bad \\uXXXF esc\"",
			Expected: `Syntax Error GraphQL (1:7) Invalid character escape sequence: \uXXXF

1: "bad \uXXXF esc"
         ^
`,
		},
		{
			Body: "\"bad \\u123",
			Expected: `Syntax Error GraphQL (1:7) Invalid character escape sequence: \u123

1: "bad \u123
         ^
`,
		},
		{
			// some unicode chars take more than one column of text
			// current implementation does not handle this
			Body: "\"bфы世ыы𠱸d \\uXXXF esc\"",
			Expected: `Syntax Error GraphQL (1:12) Invalid character escape sequence: \uXXXF

1: "bфы世ыы𠱸d \uXXXF esc"
              ^
`,
		},
	}
	for _, test := range tests {
		_, err := Lex(createSource(test.Body))(0)
		if err == nil {
			t.Errorf("unexpected nil error\nexpected:\n%v\n\ngot:\n%v", test.Expected, err)
		}

		if err.Error() != test.Expected {
			t.Errorf("unexpected error.\nexpected:\n%v\n\ngot:\n%v", test.Expected, err.Error())
		}
	}
}

func TestLexer_LexesNumbers(t *testing.T) {
	tests := []Test{
		{
			Body: "4",
			Expected: Token{
				Kind:  TokenKind[INT],
				Start: 0,
				End:   1,
				Value: "4",
			},
		},
		{
			Body: "4.123",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   5,
				Value: "4.123",
			},
		},
		{
			Body: "-4",
			Expected: Token{
				Kind:  TokenKind[INT],
				Start: 0,
				End:   2,
				Value: "-4",
			},
		},
		{
			Body: "9",
			Expected: Token{
				Kind:  TokenKind[INT],
				Start: 0,
				End:   1,
				Value: "9",
			},
		},
		{
			Body: "0",
			Expected: Token{
				Kind:  TokenKind[INT],
				Start: 0,
				End:   1,
				Value: "0",
			},
		},
		{
			Body: "-4.123",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   6,
				Value: "-4.123",
			},
		},
		{
			Body: "0.123",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   5,
				Value: "0.123",
			},
		},
		{
			Body: "123e4",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   5,
				Value: "123e4",
			},
		},
		{
			Body: "123E4",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   5,
				Value: "123E4",
			},
		},
		{
			Body: "123e-4",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   6,
				Value: "123e-4",
			},
		},
		{
			Body: "123e+4",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   6,
				Value: "123e+4",
			},
		},
		{
			Body: "-1.123e4",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   8,
				Value: "-1.123e4",
			},
		},
		{
			Body: "-1.123E4",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   8,
				Value: "-1.123E4",
			},
		},
		{
			Body: "-1.123e-4",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   9,
				Value: "-1.123e-4",
			},
		},
		{
			Body: "-1.123e+4",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   9,
				Value: "-1.123e+4",
			},
		},
		{
			Body: "-1.123e4567",
			Expected: Token{
				Kind:  TokenKind[FLOAT],
				Start: 0,
				End:   11,
				Value: "-1.123e4567",
			},
		},
	}
	for _, test := range tests {
		token, err := Lex(createSource(test.Body))(0)
		if err != nil {
			t.Errorf("unexpected error: %v, test: %s", err, test)
		}
		if !reflect.DeepEqual(token, test.Expected) {
			t.Errorf("unexpected token, expected: %v, got: %v, test: %v", test.Expected, token, test)
		}
	}
}

func TestLexer_ReportsUsefulNumberErrors(t *testing.T) {
	tests := []Test{
		{
			Body: "00",
			Expected: `Syntax Error GraphQL (1:2) Invalid number, unexpected digit after 0: "0".

1: 00
    ^
`,
		},
		{
			Body: "+1",
			Expected: `Syntax Error GraphQL (1:1) Unexpected character "+".

1: +1
   ^
`,
		},
		{
			Body: "1.",
			Expected: `Syntax Error GraphQL (1:3) Invalid number, expected digit but got: <EOF>.

1: 1.
     ^
`,
		},
		{
			Body: ".123",
			Expected: `Syntax Error GraphQL (1:1) Unexpected character ".".

1: .123
   ^
`,
		},
		{
			Body: "1.A",
			Expected: `Syntax Error GraphQL (1:3) Invalid number, expected digit but got: "A".

1: 1.A
     ^
`,
		},
		{
			Body: "-A",
			Expected: `Syntax Error GraphQL (1:2) Invalid number, expected digit but got: "A".

1: -A
    ^
`,
		},
		{
			Body: "1.0e",

			Expected: `Syntax Error GraphQL (1:5) Invalid number, expected digit but got: <EOF>.

1: 1.0e
       ^
`,
		},
		{
			Body: "1.0eA",
			Expected: `Syntax Error GraphQL (1:5) Invalid number, expected digit but got: "A".

1: 1.0eA
       ^
`,
		},
	}
	for _, test := range tests {
		_, err := Lex(createSource(test.Body))(0)
		if err == nil {
			t.Errorf("unexpected nil error\nexpected:\n%v\n\ngot:\n%v", test.Expected, err)
		}
		if err.Error() != test.Expected {
			t.Errorf("unexpected error.\nexpected:\n%v\n\ngot:\n%v", test.Expected, err.Error())
		}
	}
}

func TestLexer_LexesPunctuation(t *testing.T) {
	tests := []Test{
		{
			Body: "!",
			Expected: Token{
				Kind:  TokenKind[BANG],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: "$",
			Expected: Token{
				Kind:  TokenKind[DOLLAR],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: "(",
			Expected: Token{
				Kind:  TokenKind[PAREN_L],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: ")",
			Expected: Token{
				Kind:  TokenKind[PAREN_R],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: "...",
			Expected: Token{
				Kind:  TokenKind[SPREAD],
				Start: 0,
				End:   3,
				Value: "",
			},
		},
		{
			Body: ":",
			Expected: Token{
				Kind:  TokenKind[COLON],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: "=",
			Expected: Token{
				Kind:  TokenKind[EQUALS],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: "@",
			Expected: Token{
				Kind:  TokenKind[AT],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: "[",
			Expected: Token{
				Kind:  TokenKind[BRACKET_L],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: "]",
			Expected: Token{
				Kind:  TokenKind[BRACKET_R],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: "{",
			Expected: Token{
				Kind:  TokenKind[BRACE_L],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: "|",
			Expected: Token{
				Kind:  TokenKind[PIPE],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
		{
			Body: "}",
			Expected: Token{
				Kind:  TokenKind[BRACE_R],
				Start: 0,
				End:   1,
				Value: "",
			},
		},
	}
	for _, test := range tests {
		token, err := Lex(createSource(test.Body))(0)
		if err != nil {
			t.Errorf("unexpected error :%v, test: %v", err, test)
		}
		if !reflect.DeepEqual(token, test.Expected) {
			t.Errorf("unexpected token, expected: %v, got: %v, test: %v", test.Expected, token, test)
		}
	}
}

func TestLexer_ReportsUsefulUnknownCharacterError(t *testing.T) {
	tests := []Test{
		{
			Body: "..",
			Expected: `Syntax Error GraphQL (1:1) Unexpected character ".".

1: ..
   ^
`,
		},
		{
			Body: "?",
			Expected: `Syntax Error GraphQL (1:1) Unexpected character "?".

1: ?
   ^
`,
		},
		{
			Body: "\u203B",
			Expected: `Syntax Error GraphQL (1:1) Unexpected character "\\u203B".

1: ※
   ^
`,
		},
		{
			Body: "\u203b",
			Expected: `Syntax Error GraphQL (1:1) Unexpected character "\\u203B".

1: ※
   ^
`,
		},
		{
			Body: "ф",
			Expected: `Syntax Error GraphQL (1:1) Unexpected character "\\u0444".

1: ф
   ^
`,
		},
	}
	for _, test := range tests {
		_, err := Lex(createSource(test.Body))(0)
		if err == nil {
			t.Errorf("unexpected nil error\nexpected:\n%v\n\ngot:\n%v", test.Expected, err)
		}
		if err.Error() != test.Expected {
			t.Errorf("unexpected error.\nexpected:\n%v\n\ngot:\n%v", test.Expected, err.Error())
		}
	}
}

func TestLexer_ReportsUsefulInformationForDashesInNames(t *testing.T) {
	q := "a-b"
	lexer := Lex(createSource(q))
	firstToken, err := lexer(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	firstTokenExpected := Token{
		Kind:  TokenKind[NAME],
		Start: 0,
		End:   1,
		Value: "a",
	}
	if !reflect.DeepEqual(firstToken, firstTokenExpected) {
		t.Fatalf("unexpected token, expected: %v, got: %v", firstTokenExpected, firstToken)
	}
	errExpected := `Syntax Error GraphQL (1:3) Invalid number, expected digit but got: "b".

1: a-b
     ^
`
	token, err := lexer(0)
	if err == nil {
		t.Fatalf("unexpected nil error: %v", err)
	}
	if err.Error() != errExpected {
		t.Fatalf("unexpected error, token:%v\nexpected:\n%v\n\ngot:\n%v", token, errExpected, err.Error())
	}
}
