// Copyright 2019 Liquidata, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nomdl

import (
	"fmt"
	"text/scanner"
)

type lexer struct {
	scanner   *scanner.Scanner
	peekToken rune
}

func (lex *lexer) next() rune {
	if lex.peekToken != 0 {
		tok := lex.peekToken
		lex.peekToken = 0
		return tok
	}

	return lex.scanner.Scan()
}

func (lex *lexer) peek() rune {
	if lex.peekToken != 0 {
		return lex.peekToken
	}
	tok := lex.scanner.Scan()
	lex.peekToken = tok
	return tok
}

func (lex *lexer) pos() scanner.Position {
	if lex.peekToken != 0 {
		panic("Cannot use pos after peek")
	}
	return lex.scanner.Pos()
}

func (lex *lexer) tokenText() string {
	if lex.peekToken != 0 {
		panic("Cannot use tokenText after peek")
	}
	return lex.scanner.TokenText()
}

func (lex *lexer) eat(expected rune) rune {
	tok := lex.next()
	lex.check(expected, tok)
	return tok
}

func (lex *lexer) eatIf(expected rune) bool {
	tok := lex.peek()
	if tok == expected {
		lex.next()
		return true
	}
	return false
}

func (lex *lexer) check(expected, actual rune) {
	if actual != expected {
		lex.tokenMismatch(expected, actual)
	}
}

func (lex *lexer) tokenMismatch(expected, actual rune) {
	raiseSyntaxError(fmt.Sprintf("Unexpected token %s, expected %s", scanner.TokenString(actual), scanner.TokenString(expected)), lex.pos())
}

func (lex *lexer) unexpectedToken(actual rune) {
	raiseSyntaxError(fmt.Sprintf("Unexpected token %s", scanner.TokenString(actual)), lex.pos())
}

func raiseSyntaxError(msg string, pos scanner.Position) {
	panic(syntaxError{
		msg: msg,
		pos: pos,
	})
}

type syntaxError struct {
	msg string
	pos scanner.Position
}

func (e syntaxError) Error() string {
	return fmt.Sprintf("%s, %s", e.msg, e.pos)
}

func catchSyntaxError(f func()) (errRes error) {
	defer func() {
		if err := recover(); err != nil {
			if err, ok := err.(syntaxError); ok {
				errRes = err
				return
			}
			panic(err)
		}
	}()

	f()
	return
}
