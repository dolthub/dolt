// Copyright 2019 Dolthub, Inc.
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
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/types"
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
	var typeErr error
	err = catchSyntaxError(func() {
		typ, typeErr = p.parseType()
		p.ensureAtEnd()
	})

	if err == nil && typeErr != nil {
		return nil, typeErr
	}

	return typ, err
}

// MustParseType parses a string describing a Noms type and panics if there
// is an error.
func MustParseType(code string) *types.Type {
	typ, err := ParseType(code)
	d.PanicIfError(err)
	return typ
}

// Parse parses a string describing a Noms value.
func Parse(ctx context.Context, vrw types.ValueReadWriter, code string) (v types.Value, err error) {
	p := New(vrw, strings.NewReader(code), ParserOptions{})
	var parseErr error
	err = catchSyntaxError(func() {
		v, parseErr = p.parseValue(ctx)
		p.ensureAtEnd()
	})

	if err == nil && parseErr != nil {
		return nil, parseErr
	}

	return v, err
}

// MustParse parses a string describing a Noms value and panics if there
// is an error.
func MustParse(ctx context.Context, vrw types.ValueReadWriter, code string) types.Value {
	v, err := Parse(ctx, vrw, code)
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
//   `Float`
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

func (p *Parser) parseType() (*types.Type, error) {
	tok := p.lex.eat(scanner.Ident)
	return p.parseTypeWithToken(tok, p.lex.tokenText())
}

func (p *Parser) parseTypeWithToken(tok rune, tokenText string) (*types.Type, error) {
	t, err := p.parseSingleTypeWithToken(tok, tokenText)

	if err != nil {
		return nil, err
	}

	tok = p.lex.peek()
	if tok != '|' {
		return t, nil
	}
	unionTypes := []*types.Type{t}

	for {
		tok = p.lex.peek()
		if tok == '|' {
			p.lex.next()
		} else {
			break
		}
		st, err := p.parseSingleType()

		if err != nil {
			return nil, err
		}

		unionTypes = append(unionTypes, st)
	}
	return types.MakeUnionType(unionTypes...)
}

func (p *Parser) parseSingleType() (*types.Type, error) {
	tok := p.lex.eat(scanner.Ident)
	return p.parseSingleTypeWithToken(tok, p.lex.tokenText())
}

func (p *Parser) parseSingleTypeWithToken(tok rune, tokenText string) (*types.Type, error) {
	switch tokenText {
	case "Bool":
		return types.PrimitiveTypeMap[types.BoolKind], nil
	case "Blob":
		return types.PrimitiveTypeMap[types.BlobKind], nil
	case "Float":
		return types.PrimitiveTypeMap[types.FloatKind], nil
	case "String":
		return types.PrimitiveTypeMap[types.StringKind], nil
	case "Type":
		return types.PrimitiveTypeMap[types.TypeKind], nil
	case "Value":
		return types.PrimitiveTypeMap[types.ValueKind], nil
	case "Tuple":
		f := types.Format_Default
		if p.vrw != nil {
			f = p.vrw.Format()
		}
		return types.TypeOf(types.EmptyTuple(f))
	case "Struct":
		return p.parseStructType()
	case "Map":
		return p.parseMapType()
	case "List":
		elemType, err := p.parseSingleElemType(true)

		if err != nil {
			return nil, err
		}

		return types.MakeListType(elemType)
	case "Set":
		elemType, err := p.parseSingleElemType(true)

		if err != nil {
			return nil, err
		}

		return types.MakeSetType(elemType)
	case "Ref":
		elemType, err := p.parseSingleElemType(false)

		if err != nil {
			return nil, err
		}

		return types.MakeRefType(elemType)
	case "Cycle":
		return p.parseCycleType(), nil
	}

	p.lex.unexpectedToken(tok)
	return nil, types.ErrUnknownType
}

func (p *Parser) parseStructType() (*types.Type, error) {
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
		typ, err := p.parseType()

		if err != nil {
			return nil, err
		}

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

func (p *Parser) parseSingleElemType(allowEmptyUnion bool) (*types.Type, error) {
	p.lex.eat('<')
	if allowEmptyUnion && p.lex.eatIf('>') {
		return types.MakeUnionType()
	}
	elemType, err := p.parseType()

	if err != nil {
		return nil, err
	}

	p.lex.eat('>')
	return elemType, nil
}

func (p *Parser) parseCycleType() *types.Type {
	p.lex.eat('<')
	p.lex.eat(scanner.Ident)
	name := p.lex.tokenText()
	p.lex.eat('>')
	return types.MakeCycleType(name)
}

func (p *Parser) parseMapType() (*types.Type, error) {
	var keyType, valueType *types.Type
	p.lex.eat('<')

	if p.lex.eatIf('>') {
		var err error
		keyType, err = types.MakeUnionType()

		if err != nil {
			return nil, err
		}

		valueType = keyType
	} else {
		var err error
		keyType, err = p.parseType()

		if err != nil {
			return nil, err
		}

		p.lex.eat(',')
		valueType, err = p.parseType()

		if err != nil {
			return nil, err
		}

		p.lex.eat('>')
	}
	return types.MakeMapType(keyType, valueType)
}

// Value :
//   Type
//   Bool
//   Float
//   String
//   List
//   Set
//   Map
//   Struct
//   Tuple
//
// Bool :
//   `true`
//   `false`
//
// Float :
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
func (p *Parser) parseValue(ctx context.Context) (types.Value, error) {
	tok := p.lex.next()
	switch tok {
	case scanner.Ident:
		switch tokenText := p.lex.tokenText(); tokenText {
		case "true":
			return types.Bool(true), nil
		case "false":
			return types.Bool(false), nil
		case "set":
			return p.parseSet(ctx)
		case "map":
			return p.parseMap(ctx)
		case "struct":
			return p.parseStruct(ctx)
		case "blob":
			return p.parseBlob(ctx)
		default:
			return p.parseTypeWithToken(tok, tokenText)
		}
	case scanner.Float, scanner.Int:
		f := p.parseFloat()
		return types.Float(f), nil
	case '-':
		if !p.lex.eatIf(scanner.Float) {
			p.lex.eat(scanner.Int)
		}
		n := p.parseFloat()
		return types.Float(-float64(n)), nil
	case '+':
		if !p.lex.eatIf(scanner.Float) {
			p.lex.eat(scanner.Int)
		}
		return p.parseFloat(), nil
	case '[':
		return p.parseList(ctx)
	case scanner.String:
		s := p.lex.tokenText()
		s2, err := strconv.Unquote(s)
		if err != nil {
			raiseSyntaxError(fmt.Sprintf("Invalid string %s", s), p.lex.pos())
		}
		return types.String(s2), nil
	}

	p.lex.unexpectedToken(tok)

	panic("unreachable")
}

func (p *Parser) parseFloat() types.Float {
	s := p.lex.tokenText()
	f, _ := strconv.ParseFloat(s, 64)
	return types.Float(f)
}

func (p *Parser) parseList(ctx context.Context) (types.List, error) {
	// already swallowed '['
	l, err := types.NewList(ctx, p.vrw)

	if err != nil {
		return types.EmptyList, err
	}

	le := l.Edit()

	for p.lex.peek() != ']' {
		v, err := p.parseValue(ctx)

		if err != nil {
			return types.EmptyList, err
		}
		le.Append(v)

		if p.lex.eatIf(',') {
			continue
		}

		break
	}
	p.lex.eat(']')
	return le.List(ctx)
}

func (p *Parser) parseSet(ctx context.Context) (types.Set, error) {
	// already swallowed 'set'
	p.lex.eat('{')
	s, err := types.NewSet(ctx, p.vrw)

	if err != nil {
		return types.EmptySet, err
	}

	se := s.Edit()

	for p.lex.peek() != '}' {
		v, err := p.parseValue(ctx)

		if err != nil {
			return types.EmptySet, err
		}

		se, err = se.Insert(v)

		if err != nil {
			return types.EmptySet, err
		}

		if p.lex.eatIf(',') {
			continue
		}

		break
	}
	p.lex.eat('}')
	return se.Set(ctx)
}

func (p *Parser) parseMap(ctx context.Context) (types.Map, error) {
	// already swallowed 'map'
	p.lex.eat('{')
	m, err := types.NewMap(ctx, p.vrw)

	if err != nil {
		return types.EmptyMap, err
	}

	me := m.Edit()

	for p.lex.peek() != '}' {
		key, err := p.parseValue(ctx)

		if err != nil {
			return types.EmptyMap, err
		}

		p.lex.eat(':')
		value, err := p.parseValue(ctx)
		if err != nil {
			return types.EmptyMap, err
		}

		me = me.Set(key, value)

		if p.lex.eatIf(',') {
			continue
		}

		break
	}
	p.lex.eat('}')
	return me.Map(ctx)
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

func (p *Parser) parseBlob(ctx context.Context) (types.Blob, error) {
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
	return types.NewBlob(ctx, p.vrw, bytes.NewReader(buff.Bytes()))
}

func (p *Parser) parseStruct(ctx context.Context) (types.Struct, error) {
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
		v, err := p.parseValue(ctx)

		if err != nil {
			return types.Struct{}, err
		}

		data[fieldName] = v

		if p.lex.eatIf(',') {
			continue
		}

		break
	}
	p.lex.eat('}')
	return types.NewStruct(p.vrw.Format(), name, data)
}
