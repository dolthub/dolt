// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nomdl

import (
	"io"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

// Parser provides ways to parse Noms types.
type Parser struct {
	lex *lexer
}

// ParserOptions allows passing options into New.
type ParserOptions struct {
	// Filename is the name of the file we are currently parsing.
	Filename string
}

// New creates a new Parser.
func New(r io.Reader, options ParserOptions) *Parser {
	s := scanner.Scanner{}
	s.Filename = options.Filename
	s.Mode = scanner.ScanIdents | scanner.ScanComments | scanner.SkipComments
	s.Init(r)
	lex := lexer{scanner: &s}
	return &Parser{&lex}
}

// ParseType parses a string describing a Noms type.
func ParseType(code string) (typ *types.Type, err error) {
	p := New(strings.NewReader(code), ParserOptions{})
	err = catchSyntaxError(func() {
		typ = p.parseType()
		p.ensureAtEnd()
	})
	return
}

// MustParseType parses a string describing a Noms type and panics if there
// is an error.
func MustParseType(code string) *types.Type {
	typ, err := ParseType(code)
	d.PanicIfError(err)
	return typ
}

func (p *Parser) ensureAtEnd() {
	p.lex.eat(scanner.EOF)
}

// Type :
//   TypeWithoutUnion (`|` TypeWithoutUnion)*
//
// TypeWithoutUnion :
//   `Blob`
//   `Bool`
//   `Number`
//   `String`
//   `Type`
//   `Value`
//   CycleType
//   ListType
//   MapType
//   RefType
//   SetType
//   StructType
//
// CycleType :
//   `Cycle` `<` uint32 `>`
//
// ListType :
//   `List` `<` Type? `>`
//
// MapType :
//   `Map` `<` (Type `,` Type)? `>`
//
// RefType :
//   `Set` `<` Type `>`
//
// SetType :
//   `Set` `<` Type? `>`
//
// StructType :
//   `struct` StructName? `{` StructFields? `}`
//
// StructFields :
//   StructField
//   StructField `,` StructFields?
//
// StructName :
//   Ident
//
// StructField :
//   StructFieldName `:` Type
//
// StructFieldName :
//   Ident

func (p *Parser) parseType() *types.Type {
	t := p.parseTypeWithoutUnion()
	tok := p.lex.peek()
	if tok != '|' {
		return t
	}
	unionTypes := []*types.Type{t}

	for {
		tok = p.lex.peek()
		if tok == '|' {
			p.lex.next()
		} else {
			break
		}
		unionTypes = append(unionTypes, p.parseTypeWithoutUnion())
	}
	return types.MakeUnionType(unionTypes...)
}

func (p *Parser) parseTypeWithoutUnion() *types.Type {
	tok := p.lex.next()
	switch tok {
	case scanner.Ident:
		switch p.lex.tokenText() {
		case "Bool":
			return types.BoolType
		case "Blob":
			return types.BlobType
		case "Number":
			return types.NumberType
		case "String":
			return types.StringType
		case "Type":
			return types.TypeType
		case "Value":
			return types.ValueType
		case "struct":
			return p.parseStructType()
		case "Map":
			return p.parseMapType()
		case "List":
			elemType := p.parseSingleElemType(true)
			return types.MakeListType(elemType)
		case "Set":
			elemType := p.parseSingleElemType(true)
			return types.MakeSetType(elemType)
		case "Ref":
			elemType := p.parseSingleElemType(false)
			return types.MakeRefType(elemType)
		case "Cycle":
			return p.parseCycleType()
		}
	}
	p.lex.unexpectedToken(tok)
	return nil
}

func (p *Parser) parseStructType() *types.Type {
	tok := p.lex.next()
	name := ""
	if tok == scanner.Ident {
		name = p.lex.tokenText()
		p.lex.eat('{')
	} else {
		p.lex.check('{', tok)
	}
	fields := types.FieldMap{}

	for p.lex.peek() != '}' {
		p.lex.eat(scanner.Ident)

		fieldName := p.lex.tokenText()
		p.lex.eat(':')
		typ := p.parseType()
		fields[fieldName] = typ

		if p.lex.eatIf(',') {
			continue
		}

		break
	}
	p.lex.eat('}')
	return types.MakeStructTypeFromFields(name, fields)
}

func (p *Parser) parseSingleElemType(allowEmptyUnion bool) *types.Type {
	p.lex.eat('<')
	if allowEmptyUnion && p.lex.eatIf('>') {
		return types.MakeUnionType()
	}
	elemType := p.parseType()
	p.lex.eat('>')
	return elemType
}

func (p *Parser) parseCycleType() *types.Type {
	p.lex.eat('<')
	tok := p.lex.eat(scanner.Int)
	s, err := strconv.ParseUint(p.lex.tokenText(), 10, 32)
	if err != nil {
		p.lex.unexpectedToken(tok)
		return nil
	}
	p.lex.eat('>')
	return types.MakeCycleType(uint32(s))
}

func (p *Parser) parseMapType() *types.Type {
	var keyType, valueType *types.Type
	p.lex.eat('<')

	if p.lex.eatIf('>') {
		keyType = types.MakeUnionType()
		valueType = keyType
	} else {
		keyType = p.parseType()
		p.lex.eat(',')
		valueType = p.parseType()
		p.lex.eat('>')
	}
	return types.MakeMapType(keyType, valueType)
}
