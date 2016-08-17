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
	"github.com/attic-labs/noms/go/types"
)

/**** Query language BNF
  query := expr
  expr := expr boolop relExpr | group
  relExpr := index relOp value
  group := '(' expr ')' | relExpr
  boolOp := 'and' | 'or'
  relOp := '=' | '<' | '<=' | '>' | '>='
  value := "<string>" | int | float
*/

type compOp string
type boolOp string

const (
	equals compOp = "="
	gt     compOp = ">"
	gte    compOp = ">="
	lt     compOp = "<"
	lte    compOp = "<="
	openP         = "("
	closeP        = ")"
	and    boolOp = "and"
	or     boolOp = "or"
)

var (
	reloperators  = []compOp{equals, gt, gte, lt, lte}
	booloperators = []boolOp{and, or}
	indexPath     = ""
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

func parseQuery(q string) (expr, error) {
	s := NewQueryScanner(q)
	var expr expr
	err := d.Try(func() {
		expr = s.parseExpr(0)
	})
	return expr, err
}

func NewQueryScanner(query string) *qScanner {
	isIdentRune := func(r rune, i int) bool {
		identChars := ":/.>=-"
		startIdentChars := "><"
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

func (s *qScanner) parseExpr(level int) expr {
	tok := s.Scan()
	switch tok {
	case '(':
		expr := s.parseExpr(level + 1)
		tok := s.Scan()
		if tok != ')' {
			d.PanicIfError(fmt.Errorf("missing ending paren for expr"))
		} else {
			tok = s.Peek()
			if tok == ')' {
				return expr
			}
			tok = s.Scan()
			text := s.TokenText()
			switch {
			case tok == scanner.Ident && isBoolOp(text):
				op := boolOp(text)
				expr2 := s.parseExpr(level + 1)
				return logExpr{op, expr, expr2}
			case tok == scanner.EOF:
				return expr
			default:
				d.PanicIfError(fmt.Errorf("extra text found at end of expr, tok: %d, text: %s", int(tok), s.TokenText()))
			}
		}
	case '_':
		rexpr := s.parseCompExpr(level+1, s.TokenText())
		tok := s.Peek()
		switch tok {
		case ')':
			return rexpr
		case rune(scanner.Ident):
			tok = s.Scan()
			text := s.TokenText()
			if isBoolOp(text) {
				op := boolOp(text)
				expr2 := s.parseExpr(level + 1)
				return logExpr{op, rexpr, expr2}
			} else {
				d.PanicIfError(fmt.Errorf("expected boolean op, found: %s, level: %d", text, level))
			}
		case rune(scanner.EOF):
			return rexpr
		default:
			tok = s.Scan()
		}
	default:
		d.PanicIfError(fmt.Errorf("unexpected token in expr: %s, %d", s.TokenText(), tok))
	}
	return logExpr{}
}

func (s *qScanner) parseCompExpr(level int, indexPath string) compExpr {
	tok := s.Scan()
	text := s.TokenText()
	if !isCompOp(text) {
		d.PanicIfError(fmt.Errorf("expected relop token but found: '%s'", text))
	}
	op := compOp(text)
	tok = s.Scan()
	text = s.TokenText()
	switch tok {
	case scanner.String:
		return compExpr{indexPath, op, valueFromString(text)}
	case scanner.Float:
		f, _ := strconv.ParseFloat(text, 64)
		return compExpr{indexPath, op, types.Number(f)}
	case scanner.Int:
		f, _ := strconv.ParseInt(text, 10, 64)
		return compExpr{indexPath, op, types.Number(f)}
	}
	d.PanicIfError(fmt.Errorf("expected value token, found: '%s'", text))
	return compExpr{}
}

func valueFromString(t string) types.Value {
	l := len(t)
	if l < 2 && t[0] == '"' && t[l-1] == '"' {
		d.PanicIfError(fmt.Errorf("Unable to get value from token: %s", t))
	}
	return types.String(t[1 : l-1])
}

func isCompOp(s string) bool {
	for _, op := range reloperators {
		if s == string(op) {
			return true
		}
	}
	return false
}

func isBoolOp(s string) bool {
	for _, op := range booloperators {
		if s == string(op) {
			return true
		}
	}
	return false
}
