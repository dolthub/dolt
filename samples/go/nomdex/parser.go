// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
)

/**** Query language BNF
  query := expr
  expr := expr boolOp compExpr | group
  compExpr := indexToken compOp value
  group := '(' expr ')' | compExpr
  boolOp := 'and' | 'or'
  compOp := '=' | '<' | '<=' | '>' | '>=' | !=
  value := "<string>" | number
  number := '-' digits | digits
  digits := int | float

*/

type compOp string
type boolOp string

type indexManager struct {
	db      datas.Database
	indexes map[string]types.Map
}

const (
	equals compOp = "="
	gt     compOp = ">"
	gte    compOp = ">="
	lt     compOp = "<"
	lte    compOp = "<="
	ne     compOp = "!="
	openP         = "("
	closeP        = ")"
	and    boolOp = "and"
	or     boolOp = "or"
)

var (
	compOps = []compOp{equals, gt, gte, lt, lte, ne}
	boolOps = []boolOp{and, or}
)

type qScanner struct {
	s           scanner.Scanner
	peekedToken rune
	peekedText  string
	peeked      bool
}

func (qs *qScanner) Scan() rune {
	var r rune
	if qs.peeked {
		r = qs.peekedToken
		qs.peeked = false
	} else {
		r = qs.s.Scan()
	}
	return r
}

func (qs *qScanner) Peek() rune {
	var r rune

	if !qs.peeked {
		qs.peekedToken = qs.s.Scan()
		qs.peekedText = qs.s.TokenText()
		qs.peeked = true
	}
	r = qs.peekedToken
	return r
}

func (qs *qScanner) TokenText() string {
	var text string
	if qs.peeked {
		text = qs.peekedText
	} else {
		text = qs.s.TokenText()
	}
	return text
}

func (qs *qScanner) Pos() scanner.Position {
	return qs.s.Pos()
}

func parseQuery(q string, im *indexManager) (expr, error) {
	s := NewQueryScanner(q)
	var expr expr
	err := d.Try(func() {
		expr = s.parseExpr(0, im)
	})
	return expr, err
}

func NewQueryScanner(query string) *qScanner {
	isIdentRune := func(r rune, i int) bool {
		identChars := ":/.>=-"
		startIdentChars := "!><"
		if i == 0 {
			return unicode.IsLetter(r) || strings.ContainsRune(startIdentChars, r)
		}
		return unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune(identChars, r)
	}

	errorFunc := func(s *scanner.Scanner, msg string) {
		d.PanicIfError(fmt.Errorf("%s, pos: %s\n", msg, s.Pos()))
	}

	var s scanner.Scanner
	s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanStrings | scanner.SkipComments
	s.Init(strings.NewReader(query))
	s.IsIdentRune = isIdentRune
	s.Error = errorFunc
	qs := qScanner{s: s}
	return &qs
}

func (s *qScanner) parseExpr(level int, im *indexManager) expr {
	tok := s.Scan()
	switch tok {
	case '(':
		expr1 := s.parseExpr(level+1, im)
		tok := s.Scan()
		if tok != ')' {
			d.PanicIfError(fmt.Errorf("missing ending paren for expr"))
		} else {
			tok = s.Peek()
			if tok == ')' {
				return expr1
			}
			tok = s.Scan()
			text := s.TokenText()
			switch {
			case tok == scanner.Ident && isBoolOp(text):
				op := boolOp(text)
				expr2 := s.parseExpr(level+1, im)
				return logExpr{op: op, expr1: expr1, expr2: expr2, idxName: idxNameIfSame(expr1, expr2)}
			case tok == scanner.EOF:
				return expr1
			default:
				d.PanicIfError(fmt.Errorf("extra text found at end of expr, tok: %d, text: %s", int(tok), s.TokenText()))
			}
		}
	case scanner.Ident:
		err := openIndex(s.TokenText(), im)
		d.PanicIfError(err)
		expr1 := s.parseCompExpr(level+1, s.TokenText(), im)
		tok := s.Peek()
		switch tok {
		case ')':
			return expr1
		case rune(scanner.Ident):
			tok = s.Scan()
			text := s.TokenText()
			if isBoolOp(text) {
				op := boolOp(text)
				expr2 := s.parseExpr(level+1, im)
				return logExpr{op: op, expr1: expr1, expr2: expr2, idxName: idxNameIfSame(expr1, expr2)}
			} else {
				d.PanicIfError(fmt.Errorf("expected boolean op, found: %s, level: %d", text, level))
			}
		case rune(scanner.EOF):
			return expr1
		default:
			_ = s.Scan()
		}
	default:
		d.PanicIfError(fmt.Errorf("unexpected token in expr: %s, %d", s.TokenText(), tok))
	}
	return logExpr{}
}

func (s *qScanner) parseCompExpr(level int, indexName string, im *indexManager) compExpr {

	s.Scan()
	text := s.TokenText()
	if !isCompOp(text) {
		d.PanicIfError(fmt.Errorf("expected relop token but found: '%s'", text))
	}
	op := compOp(text)
	value := s.parseValExpr()
	return compExpr{indexName, op, value}
}

func (s *qScanner) parseValExpr() types.Value {
	tok := s.Scan()
	text := s.TokenText()
	isNeg := false
	if tok == '-' {
		isNeg = true
		tok = s.Scan()
		text = s.TokenText()
	}
	switch tok {
	case scanner.String:
		if isNeg {
			d.PanicIfError(fmt.Errorf("expected number after '-', found string: %s", text))
		}
		return valueFromString(text)
	case scanner.Float:
		f, _ := strconv.ParseFloat(text, 64)
		if isNeg {
			f = -f
		}
		return types.Number(f)
	case scanner.Int:
		i, _ := strconv.ParseInt(text, 10, 64)
		if isNeg {
			i = -i
		}
		return types.Number(i)
	}
	d.PanicIfError(fmt.Errorf("expected value token, found: '%s'", text))
	return nil // for compiler
}

func valueFromString(t string) types.Value {
	l := len(t)
	if l < 2 && t[0] == '"' && t[l-1] == '"' {
		d.PanicIfError(fmt.Errorf("Unable to get value from token: %s", t))
	}
	return types.String(t[1 : l-1])
}

func isCompOp(s string) bool {
	for _, op := range compOps {
		if s == string(op) {
			return true
		}
	}
	return false
}

func isBoolOp(s string) bool {
	for _, op := range boolOps {
		if s == string(op) {
			return true
		}
	}
	return false
}

func idxNameIfSame(expr1, expr2 expr) string {
	if expr1.indexName() == expr2.indexName() {
		return expr1.indexName()
	}
	return ""
}
