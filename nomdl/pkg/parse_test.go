package pkg

import (
	"fmt"
	"strings"
	"testing"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
)

const (
	structTmpl = "struct %s { %s %s }"
)

func TestParserTestSuite(t *testing.T) {
	suite.Run(t, &ParserTestSuite{})
	suite.Run(t, &ParsedResultTestSuite{})
}

type ParserTestSuite struct {
	suite.Suite
}

func (suite *ParserTestSuite) parsePanics(test, msg string) {
	suite.Panics(func() { runParser("", strings.NewReader(test)) }, msg)
}

func (suite *ParserTestSuite) parseNotPanics(test string) {
	suite.NotPanics(func() { runParser("", strings.NewReader(test)) }, test)
}

func (suite *ParserTestSuite) TestAlias() {
	importTmpl := `alias Noms = import "%s"`
	ref := "sha1-ffffffff"
	path := `some/path/\"quotes\"/path`

	pkg := runParser("", strings.NewReader(fmt.Sprintf(importTmpl, ref)))
	suite.Equal(ref, pkg.Aliases["Noms"])

	pkg = runParser("", strings.NewReader(fmt.Sprintf(importTmpl, path)))
	suite.Equal(path, pkg.Aliases["Noms"])
}

func (suite *ParserTestSuite) TestBadStructParse() {
	noFields := "struct str { }"
	suite.parsePanics(noFields, "Struct must have fields.")

	noName := "struct { a :Bool }"
	suite.parsePanics(noName, "Struct must have name.")

	badName := "struct *ff { a :Bool }"
	suite.parsePanics(badName, "Struct must have legal name.")

	dupName := "struct str { a :Bool a :Bool }"
	suite.parsePanics(dupName, "Fields must have unique names.")

	optionalAsTypeName := "struct S { x: optional }"
	suite.parsePanics(optionalAsTypeName, "optional requires a type after it")

	optionalAsTypeName2 := "struct S { x: optional y: T }"
	suite.parsePanics(optionalAsTypeName2, "optional requires a type after it")
}

func (suite *ParserTestSuite) TestStructParse() {
	oneLine := "struct str { a :Bool b : Blob c: Blob }"
	suite.parseNotPanics(oneLine)

	noSpace := "struct str{a:Bool}"
	suite.parseNotPanics(noSpace)

	multiLine := "\nstruct str {\na :Bool\n}"
	suite.parseNotPanics(multiLine)

	for k, v := range types.KindToString {
		if types.IsPrimitiveKind(k) {
			suite.parseNotPanics(fmt.Sprintf(structTmpl, "s", "a :"+v, ""))
			suite.parseNotPanics(fmt.Sprintf(structTmpl, "s", "a: optional "+v, ""))
		}
	}

	optional := "struct str { a: optional Bool b: optional Blob c: optional Blob }"
	suite.parseNotPanics(optional)

	optionalNoSpace := "struct str{a:optional Bool}"
	suite.parseNotPanics(optionalNoSpace)
}

func (suite *ParserTestSuite) TestComment() {
	comments := []string{
		"/* Yo\n*/struct str { a :Bool }",
		"struct str { a :Bool }\n/* Yo*/",
		"/* Yo\n * is my name */\nstruct str { a :Bool }",
		"/* Yo *//* is my name */struct str { a :Bool }",
		"struct /*Yo*/ s { a :Bool }",
		"struct s /*Yo*/ { a :Bool }",
		"struct s { /*Yo*/ a :Bool }",
		"struct s { a /*Yo*/ :Bool }",
		"struct s { a :/*Yo*/ Bool }",
		"struct s { a :Bool/*Yo*/}",
		"// Yo\nstruct str { a :Bool }",
		"struct str { a :Bool }\n// Yo",
		"\n  // Yo   \t\nstruct str { a :Bool }\n   /*More Yo*/",
		`// Yo //
		// Yo Again
		struct str { a :Bool }`,
		`struct /* // up in here */s {
			a :Bool//Field a
		}`,
		`struct s {
			a :Bool //Field a
			// Not a field
		}
		/* More talk */
		struct t { b :Bool }
		struct t2 { b:optional/* x */Bool }
		struct t3 { b:/* x */optional Bool }`,
	}
	for _, c := range comments {
		suite.parseNotPanics(c)
	}
}

func (suite *ParserTestSuite) TestBadComment() {
	comments := []string{
		"st/* Yo */ruct str { a :Bool }",
		"struct str { a :Bool }\n* Yo*/",
		"/* Yo *\nstruct str { a :Bool }",
		"struct str // OOps { a :Bool }",
		"struct str { a :Bool }\n/ Yo",
	}
	for _, c := range comments {
		suite.parsePanics(c, c)
	}
}

type ParsedResultTestSuite struct {
	suite.Suite

	prim               testField
	primOptional       testField
	compound           testField
	compoundOfCompound testField
	namedType          testField
	namespacedType     testField
	mapOfNamedType     testField
}

func (suite *ParsedResultTestSuite) SetupTest() {
	suite.prim = newTestField("a", types.NumberType, false, "")
	suite.primOptional = newTestField("b", types.NumberType, true, "")
	suite.compound = newTestField("set", types.MakeSetType(types.StringType), false, "")
	suite.compoundOfCompound = newTestField("listOfSet", types.MakeListType(types.MakeSetType(types.StringType)), false, "")
	suite.namedType = newTestField("otherStruct", makeUnresolvedType("", "Other"), false, "Other")
	suite.namespacedType = newTestField("namespacedStruct", makeUnresolvedType("Elsewhere", "Other"), false, "Elsewhere.Other")
	suite.mapOfNamedType = newTestField("mapOfStructToOther", types.MakeMapType(makeUnresolvedType("", "Struct"), makeUnresolvedType("Elsewhere", "Other")), false, "Map<Struct, Elsewhere.Other>")
}

type structTestCase struct {
	Name   string
	Fields []testField
}

func makeStructTestCase(n string, fields ...testField) structTestCase {
	return structTestCase{Name: n, Fields: fields}
}

func (s structTestCase) String() string {
	fieldsSource := ""
	for _, f := range s.Fields {
		fieldsSource += f.String() + "\n"
	}
	return fmt.Sprintf("struct %s { %s }", s.Name, fieldsSource)
}

type testField struct {
	types.Field
	S string
}

func newTestField(name string, t *types.Type, optional bool, s string) testField {
	return testField{Field: types.Field{Name: name, T: t, Optional: optional}, S: s}
}

func (t testField) String() string {
	s := t.Name + ": "
	if t.Optional {
		s += "optional "
	}
	if t.S != "" {
		return s + t.S
	}
	return s + t.T.Describe()
}

func (suite *ParsedResultTestSuite) parseAndCheckStructs(structs ...structTestCase) {
	source := ""
	expectedTypes := make([]*types.Type, len(structs))
	for i, s := range structs {
		source += s.String() + "\n"
		fields := make([]types.Field, len(s.Fields))
		for i, f := range s.Fields {
			fields[i] = f.Field
		}
		expectedTypes[i] = types.MakeStructType(s.Name, fields)
	}
	suite.assertTypes(source, expectedTypes...)
}

func (suite *ParsedResultTestSuite) assertTypes(source string, ts ...*types.Type) {
	err := d.Try(func() {
		i := runParser("", strings.NewReader(source))
		for idx, t := range i.Types {
			suite.True(t.Equals(ts[idx]))
		}
	})
	suite.NoError(err, source)
}

func (suite *ParsedResultTestSuite) TestPrimitiveField() {
	suite.parseAndCheckStructs(makeStructTestCase("Simple", suite.prim))
}

func (suite *ParsedResultTestSuite) TestPrimitiveOptionalField() {
	suite.parseAndCheckStructs(makeStructTestCase("SimpleOptional", suite.primOptional))
}

func (suite *ParsedResultTestSuite) TestCommentNextToName() {
	n := "WithComment"
	s := fmt.Sprintf("struct %s { /* Oy! */%s }", n, suite.primOptional)
	suite.assertTypes(s, types.MakeStructType(n, []types.Field{suite.primOptional.Field}))
}

func (suite *ParsedResultTestSuite) TestCommentAmongFields() {
	n := "WithComment"
	s := fmt.Sprintf("struct %s { %s \n// Nope\n%s }", n, suite.prim, suite.primOptional)
	suite.assertTypes(s, types.MakeStructType(n, []types.Field{suite.prim.Field, suite.primOptional.Field}))
}

func (suite *ParsedResultTestSuite) TestCompoundField() {
	suite.parseAndCheckStructs(makeStructTestCase("Compound", suite.compound))
}

func (suite *ParsedResultTestSuite) TestCompoundOfCompoundField() {
	suite.parseAndCheckStructs(makeStructTestCase("CofC", suite.compoundOfCompound))
}

func (suite *ParsedResultTestSuite) TestNamedTypeField() {
	suite.parseAndCheckStructs(makeStructTestCase("Named", suite.namedType))
}

func (suite *ParsedResultTestSuite) TestNamespacedTypeField() {
	suite.parseAndCheckStructs(makeStructTestCase("Namespaced", suite.namespacedType))
}

func (suite *ParsedResultTestSuite) TestMapOfNamedTypeField() {
	suite.parseAndCheckStructs(makeStructTestCase("MapStruct", suite.mapOfNamedType))
}

func (suite *ParsedResultTestSuite) TestMultipleFields() {
	suite.parseAndCheckStructs(makeStructTestCase("Multi",
		suite.prim,
		suite.primOptional,
		suite.namedType,
		suite.namespacedType,
		suite.compound,
		suite.compoundOfCompound,
	))
}

func (suite *ParsedResultTestSuite) TestMultipleStructs() {
	defns := []structTestCase{
		makeStructTestCase("Simple", suite.prim),
		makeStructTestCase("Optional", suite.primOptional),
		makeStructTestCase("Compound", suite.compound),
		makeStructTestCase("Multi",
			suite.prim,
			suite.primOptional,
			suite.namespacedType,
			suite.compound,
		),
	}
	suite.parseAndCheckStructs(defns...)
}
