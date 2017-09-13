// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nomdl

import (
	"bytes"
	"fmt"
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
	vrw types.ValueReadWriter
}

// ParserOptions allows passing options into New.
type ParserOptions struct {
	// Filename is the name of the file we are currently parsing.
	Filename string
}

// New creates a new Parser.
func New(vrw types.ValueReadWriter, r io.Reader, options ParserOptions) *Parser {
	s := scanner.Scanner{}
	s.Init(r)
	s.Filename = options.Filename
	s.Mode = scanner.ScanIdents | scanner.ScanComments | scanner.SkipComments | scanner.ScanFloats | scanner.ScanStrings // | scanner.ScanRawStrings
	s.Error = func(s *scanner.Scanner, msg string) {}
	lex := lexer{scanner: &s}
	return &Parser{&lex, vrw}
}

// ParseType parses a string describing a Noms type.
func ParseType(code string) (typ *types.Type, err error) {
	p := New(nil, strings.NewReader(code), ParserOptions{})
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

// Parse parses a string describing a Noms value.
func Parse(vrw types.ValueReadWriter, code string) (v types.Value, err error) {
	p := New(vrw, strings.NewReader(code), ParserOptions{})
	err = catchSyntaxError(func() {
		v = p.parseValue()
		p.ensureAtEnd()
	})
	return
}

// MustParse parses a string describing a Noms value and panics if there
// is an error.
func MustParse(vrw types.ValueReadWriter, code string) types.Value {
	v, err := Parse(vrw, code)
	d.PanicIfError(err)
	return v
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
//   `Cycle` `<` StructName `>`
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
//   `Struct` StructName? `{` StructTypeFields? `}`
//
// StructTypeFields :
//   StructTypeField
//   StructTypeField `,` StructTypeFields?
//
// StructName :
//   Ident
//
// StructTypeField :
//   StructFieldName `?`? `:` Type
//
// StructFieldName :
//   Ident

func (p *Parser) parseType() *types.Type {
	tok := p.lex.eat(scanner.Ident)
	return p.parseTypeWithToken(tok, p.lex.tokenText())
}

func (p *Parser) parseTypeWithToken(tok rune, tokenText string) *types.Type {
	t := p.parseSingleTypeWithToken(tok, tokenText)
	tok = p.lex.peek()
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
		unionTypes = append(unionTypes, p.parseSingleType())
	}
	return types.MakeUnionType(unionTypes...)
}

func (p *Parser) parseSingleType() *types.Type {
	tok := p.lex.eat(scanner.Ident)
	return p.parseSingleTypeWithToken(tok, p.lex.tokenText())
}

func (p *Parser) parseSingleTypeWithToken(tok rune, tokenText string) *types.Type {
	switch tokenText {
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
	case "Struct":
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
	fields := []types.StructField{}

	for p.lex.peek() != '}' {
		p.lex.eat(scanner.Ident)

		fieldName := p.lex.tokenText()
		optional := p.lex.eatIf('?')
		p.lex.eat(':')
		typ := p.parseType()
		fields = append(fields, types.StructField{
			Name:     fieldName,
			Type:     typ,
			Optional: optional,
		})

		if p.lex.eatIf(',') {
			continue
		}

		break
	}
	p.lex.eat('}')
	return types.MakeStructType(name, fields...)
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
	p.lex.eat(scanner.Ident)
	name := p.lex.tokenText()
	p.lex.eat('>')
	return types.MakeCycleType(name)
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

// Value :
//   Type
//   Bool
//   Number
//   String
//   List
//   Set
//   Map
//   Struct
//
// Bool :
//   `true`
//   `false`
//
// Number :
//   ...
//
// String :
//   ...
//
// List :
//   `[` Values? `]`
//
// Values :
//   Value
//   Value `,` Values?
//
// Set :
//   `set` `{` Values? `}`
//
// Map :
//   `map` `{` MapEntries? `}`
//
// MapEntries :
//   MapEntry
//   MapEntry `,` MapEntries?
//
// MapEntry :
//   Value `:` Value
//
// Struct :
//   `struct` StructName? `{` StructFields? `}`
//
// StructFields :
//   StructField
//   StructField `,` StructFields?
//
// StructField :
//   StructFieldName `:` Value
func (p *Parser) parseValue() types.Value {
	tok := p.lex.next()
	switch tok {
	case scanner.Ident:
		switch tokenText := p.lex.tokenText(); tokenText {
		case "true":
			return types.Bool(true)
		case "false":
			return types.Bool(false)
		case "set":
			return p.parseSet()
		case "map":
			return p.parseMap()
		case "struct":
			return p.parseStruct()
		case "blob":
			return p.parseBlob()
		default:
			return p.parseTypeWithToken(tok, tokenText)
		}
	case scanner.Float, scanner.Int:
		f := p.parseFloat()
		return types.Number(f)
	case '-':
		if !p.lex.eatIf(scanner.Float) {
			p.lex.eat(scanner.Int)
		}
		n := p.parseFloat()
		return types.Number(-float64(n))
	case '+':
		if !p.lex.eatIf(scanner.Float) {
			p.lex.eat(scanner.Int)
		}
		return p.parseFloat()
	case '[':
		return p.parseList()
	case scanner.String:
		s := p.lex.tokenText()
		s2, err := strconv.Unquote(s)
		if err != nil {
			raiseSyntaxError(fmt.Sprintf("Invalid string %s", s), p.lex.pos())
		}
		return types.String(s2)
	}

	p.lex.unexpectedToken(tok)

	panic("unreachable")
}

func (p *Parser) parseFloat() types.Number {
	s := p.lex.tokenText()
	f, _ := strconv.ParseFloat(s, 64)
	return types.Number(f)
}

func (p *Parser) parseList() types.List {
	// already swallowed '['
	le := types.NewList(p.vrw).Edit()

	for p.lex.peek() != ']' {
		v := p.parseValue()
		le.Append(v)

		if p.lex.eatIf(',') {
			continue
		}

		break
	}
	p.lex.eat(']')
	return le.List()
}

func (p *Parser) parseSet() types.Set {
	// already swallowed 'set'
	p.lex.eat('{')
	se := types.NewSet(p.vrw).Edit()

	for p.lex.peek() != '}' {
		v := p.parseValue()
		se.Insert(v)

		if p.lex.eatIf(',') {
			continue
		}

		break
	}
	p.lex.eat('}')
	return se.Set()
}

func (p *Parser) parseMap() types.Map {
	// already swallowed 'map'
	p.lex.eat('{')
	me := types.NewMap(p.vrw).Edit()

	for p.lex.peek() != '}' {
		key := p.parseValue()

		p.lex.eat(':')
		value := p.parseValue()
		me.Set(key, value)

		if p.lex.eatIf(',') {
			continue
		}

		break
	}
	p.lex.eat('}')
	return me.Map()
}

func (p *Parser) blobString(s string) []byte {
	raise := func() {
		raiseSyntaxError(fmt.Sprintf("Invalid blob \"%s\"", s), p.lex.pos())
	}

	if len(s)%2 != 0 {
		raise()
	}

	var buff bytes.Buffer
	for i := 0; i < len(s); i += 2 {
		n, err := strconv.ParseUint(s[i:i+2], 16, 8)
		if err != nil {
			raise()
		}
		buff.WriteByte(uint8(n))
	}
	return buff.Bytes()
}

func (p *Parser) parseBlob() types.Blob {
	// already swallowed 'blob'
	p.lex.eat('{')
	var buff bytes.Buffer

	for p.lex.peek() != '}' {
		tok := p.lex.next()
		switch tok {
		case scanner.Ident, scanner.Int:
			s := p.lex.tokenText()
			buff.Write(p.blobString(s))
		default:
			p.lex.unexpectedToken(tok)
		}

	}
	p.lex.eat('}')
	return types.NewBlob(p.vrw, bytes.NewReader(buff.Bytes()))
}

func (p *Parser) parseStruct() types.Struct {
	// already swallowed 'struct'
	tok := p.lex.next()
	name := ""
	if tok == scanner.Ident {
		name = p.lex.tokenText()
		p.lex.eat('{')
	} else {
		p.lex.check('{', tok)
	}
	data := types.StructData{}

	for p.lex.peek() != '}' {
		p.lex.eat(scanner.Ident)

		fieldName := p.lex.tokenText()
		p.lex.eat(':')
		v := p.parseValue()
		data[fieldName] = v

		if p.lex.eatIf(',') {
			continue
		}

		break
	}
	p.lex.eat('}')
	return types.NewStruct(name, data)
}
