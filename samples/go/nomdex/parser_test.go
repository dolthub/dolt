// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"
	"text/scanner"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
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

	s := NewQueryScanner(`9 (99.9) -9 0x7F "99.9" and or http://localhost:8000/cli-tour::yo <= >= < > = _ !=`)

	scannerResults := []scannerResult{
		{tok: scanner.Int, text: "9"},
		{tok: int('('), text: "("},
		{tok: scanner.Float, text: "99.9"},
		{tok: int(')'), text: ")"},
		{tok: '-', text: "-"},
		{tok: scanner.Int, text: "9"},
		{tok: scanner.Int, text: "0x7F"},
		{tok: scanner.String, text: `"99.9"`},
		{tok: scanner.Ident, text: "and"},
		{tok: scanner.Ident, text: "or"},
		{tok: scanner.Ident, text: "http://localhost:8000/cli-tour::yo"},
		{tok: scanner.Ident, text: "<="},
		{tok: scanner.Ident, text: ">="},
		{tok: scanner.Ident, text: "<"},
		{tok: scanner.Ident, text: ">"},
		{tok: int('='), text: "="},
		{tok: int('_'), text: "_"},
		{tok: scanner.Ident, text: "!="},
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
		{tok: int('_'), text: "_"},
		{tok: scanner.Ident, text: "<"},
		{tok: scanner.String, text: `"one"`},
		{tok: scanner.EOF, text: ""},
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

	re1 := compExpr{"index1", equals, types.Number(2015)}
	re2 := compExpr{"index1", gte, types.Number(2020)}
	re3 := compExpr{"index1", lte, types.Number(2022)}
	re4 := compExpr{"index1", lt, types.Number(-2030)}
	re5 := compExpr{"index1", ne, types.Number(3.5)}
	re6 := compExpr{"index1", ne, types.Number(-3500.4536632)}
	re7 := compExpr{"index1", ne, types.String("whassup")}

	queries := []parseResult{
		{`index1 = 2015`, re1},
		{`(index1 = 2015 )`, re1},
		{`(((index1 = 2015 ) ))`, re1},
		{`index1 = 2015 or index1 >= 2020`, logExpr{or, re1, re2, "index1"}},
		{`(index1 = 2015) or index1 >= 2020`, logExpr{or, re1, re2, "index1"}},
		{`index1 = 2015 or (index1 >= 2020)`, logExpr{or, re1, re2, "index1"}},
		{`(index1 = 2015 or index1 >= 2020)`, logExpr{or, re1, re2, "index1"}},
		{`(index1 = 2015 or index1 >= 2020) and index1 <= 2022`, logExpr{and, logExpr{or, re1, re2, "index1"}, re3, "index1"}},
		{`index1 = 2015 or index1 >= 2020 and index1 <= 2022`, logExpr{or, re1, logExpr{and, re2, re3, "index1"}, "index1"}},
		{`index1 = 2015 or index1 >= 2020 and index1 <= 2022 or index1 < -2030`, logExpr{or, re1, logExpr{and, re2, logExpr{or, re3, re4, "index1"}, "index1"}, "index1"}},
		{`(index1 = 2015 or index1 >= 2020) and (index1 <= 2022 or index1 < -2030)`, logExpr{and, logExpr{or, re1, re2, "index1"}, logExpr{or, re3, re4, "index1"}, "index1"}},
		{`index1 != 3.5`, re5},
		{`index1 != -3500.4536632`, re6},
		{`index1 != "whassup"`, re7},
	}

	db := datas.NewDatabase(chunks.NewMemoryStore())
	db, err := db.Commit("index1", datas.NewCommit(types.NewMap(types.String("one"), types.NewSet(types.String("two"))), types.NewSet(), types.EmptyStruct))
	assert.NoError(err)

	im := &indexManager{db: db, indexes: map[string]types.Map{}}
	for _, pr := range queries {
		expr, err := parseQuery(pr.query, im)
		assert.NoError(err)
		assert.Equal(pr.ex, expr, "bad query: %s", pr.query)
	}

	badQueries := []string{
		`sdfsd = 2015`,
		`index1 = "unfinished string`,
		`index1 and 2015`,
		`index1 < `,
		`index1 < 2015 and ()`,
		`index1 < 2015 an index1 > 2016`,
		`(index1 < 2015) what`,
		`(index1< 2015`,
		`(badIndexName < 2015)`,
	}

	im1 := &indexManager{db: db, indexes: map[string]types.Map{}}
	for _, q := range badQueries {
		expr, err := parseQuery(q, im1)
		assert.Error(err)
		assert.Nil(expr)
	}
}
