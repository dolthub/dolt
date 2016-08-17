// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"
	"text/scanner"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

type scannerResult struct {
	tok  int
	text string
}

type parseResult struct {
	query string
	ex    expr
}

func TestQueryScanner(t *testing.T) {
	assert := assert.New(t)

	s := NewQueryScanner(`9 (99.9) "99.9" and or http://localhost:8000/cli-tour::yo <= >= < > = _`)

	scannerResults := []scannerResult{
		scannerResult{tok: scanner.Int, text: "9"},
		scannerResult{tok: int('('), text: "("},
		scannerResult{tok: scanner.Float, text: "99.9"},
		scannerResult{tok: int(')'), text: ")"},
		scannerResult{tok: scanner.String, text: `"99.9"`},
		scannerResult{tok: scanner.Ident, text: "and"},
		scannerResult{tok: scanner.Ident, text: "or"},
		scannerResult{tok: scanner.Ident, text: "http://localhost:8000/cli-tour::yo"},
		scannerResult{tok: scanner.Ident, text: "<="},
		scannerResult{tok: scanner.Ident, text: ">="},
		scannerResult{tok: scanner.Ident, text: "<"},
		scannerResult{tok: scanner.Ident, text: ">"},
		scannerResult{tok: int('='), text: "="},
		scannerResult{tok: int('_'), text: "_"},
	}

	for _, sr := range scannerResults {
		tok := s.Scan()
		assert.Equal(sr.tok, int(tok), "expected text: %s, found: %s, pos: %s", sr.text, s.TokenText(), s.Pos())
		assert.Equal(sr.text, s.TokenText())
	}
	tok := s.Scan()
	assert.Equal(scanner.EOF, int(tok))
}

func TestPeek(t *testing.T) {
	assert := assert.New(t)

	s := NewQueryScanner(`_ < "one"`)
	scannerResults := []scannerResult{
		scannerResult{tok: int('_'), text: "_"},
		scannerResult{tok: scanner.Ident, text: "<"},
		scannerResult{tok: scanner.String, text: `"one"`},
		scannerResult{tok: scanner.EOF, text: ""},
	}

	for _, sr := range scannerResults {
		assert.Equal(sr.tok, int(s.Peek()))
		assert.Equal(sr.text, s.TokenText())
		assert.Equal(sr.tok, int(s.Scan()))
		assert.Equal(sr.text, s.TokenText())
	}
}

func TestParsing(t *testing.T) {
	assert := assert.New(t)

	re1 := compExpr{"_", equals, types.Number(2015)}
	re2 := compExpr{"_", gte, types.Number(2020)}
	re3 := compExpr{"_", lte, types.Number(2022)}
	re4 := compExpr{"_", lt, types.Number(2030)}

	queries := []parseResult{
		parseResult{`_ = 2015`, re1},
		parseResult{`(_ = 2015 )`, re1},
		parseResult{`(((_ = 2015 ) ))`, re1},
		parseResult{`_ = 2015 or _ >= 2020`, logExpr{or, re1, re2}},
		parseResult{`(_ = 2015) or _ >= 2020`, logExpr{or, re1, re2}},
		parseResult{`_ = 2015 or (_ >= 2020)`, logExpr{or, re1, re2}},
		parseResult{`(_ = 2015 or _ >= 2020)`, logExpr{or, re1, re2}},
		parseResult{`(_ = 2015 or _ >= 2020) and _ <= 2022`, logExpr{and, logExpr{or, re1, re2}, re3}},
		parseResult{`_ = 2015 or _ >= 2020 and _ <= 2022`, logExpr{or, re1, logExpr{and, re2, re3}}},
		parseResult{`_ = 2015 or _ >= 2020 and _ <= 2022 or _ < 2030`, logExpr{or, re1, logExpr{and, re2, logExpr{or, re3, re4}}}},
		parseResult{`(_ = 2015 or _ >= 2020) and (_ <= 2022 or _ < 2030)`, logExpr{and, logExpr{or, re1, re2}, logExpr{or, re3, re4}}},
	}

	for _, pr := range queries {
		expr, err := parseQuery(pr.query)
		assert.NoError(err)
		assert.Equal(pr.ex, expr, "bad query: %s", pr.query)
	}

	badQueries := []string{
		`sdfsd = 2015`,
		`_ = "unfinished string`,
		`_ and 2015`,
		`_ < `,
		`_ < 2015 and ()`,
		`_ < 2015 an _ > 2016`,
		`(_ < 2015) what`,
		`(_< 2015`,
	}

	for _, q := range badQueries {
		expr, err := parseQuery(q)
		assert.Error(err)
		assert.Nil(expr)
	}
}
