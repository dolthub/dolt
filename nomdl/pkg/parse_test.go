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
		}
	}
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
		struct t { b :Bool }`,
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
	prim2              testField
	compound           testField
	compoundOfCompound testField
	namedType          testField
	namespacedType     testField
	mapOfNamedType     testField
}

func (suite *ParsedResultTestSuite) SetupTest() {
	suite.prim = newTestField("a", types.NumberType, "")
	suite.prim2 = newTestField("b", types.StringType, "")
	suite.compound = newTestField("set", types.MakeSetType(types.StringType), "")
	suite.compoundOfCompound = newTestField("listOfSet", types.MakeListType(types.MakeSetType(types.StringType)), "")
	suite.namedType = newTestField("otherStruct", makeUnresolvedType("", "Other"), "Other")
	suite.namespacedType = newTestField("namespacedStruct", makeUnresolvedType("Elsewhere", "Other"), "Elsewhere.Other")
	suite.mapOfNamedType = newTestField("mapOfStructToOther", types.MakeMapType(makeUnresolvedType("", "Struct"), makeUnresolvedType("Elsewhere", "Other")), "Map<Struct, Elsewhere.Other>")
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
	Name string
	Type *types.Type
	S    string
}

func newTestField(name string, t *types.Type, s string) testField {
	return testField{Name: name, Type: t, S: s}
}

func (t testField) String() string {
	s := t.Name + ": "
	if t.S != "" {
		return s + t.S
	}
	return s + t.Type.Describe()
}

func (suite *ParsedResultTestSuite) parseAndCheckStructs(structs ...structTestCase) {
	source := ""
	expectedTypes := make([]*types.Type, len(structs))
	for i, s := range structs {
		source += s.String() + "\n"
		fields := make(types.TypeMap, len(s.Fields))
		for _, f := range s.Fields {
			fields[f.Name] = f.Type
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

func (suite *ParsedResultTestSuite) TestCommentNextToName() {
	n := "WithComment"
	s := fmt.Sprintf("struct %s { /* Oy! */%s }", n, suite.prim2)
	suite.assertTypes(s, types.MakeStructType(n, types.TypeMap{
		suite.prim2.Name: suite.prim2.Type,
	}))
}

func (suite *ParsedResultTestSuite) TestCommentAmongFields() {
	n := "WithComment"
	s := fmt.Sprintf("struct %s { %s \n// Nope\n%s }", n, suite.prim, suite.prim2)
	suite.assertTypes(s, types.MakeStructType(n, types.TypeMap{
		suite.prim.Name:  suite.prim.Type,
		suite.prim2.Name: suite.prim2.Type,
	}))
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
		suite.prim2,
		suite.namedType,
		suite.namespacedType,
		suite.compound,
		suite.compoundOfCompound,
	))
}

func (suite *ParsedResultTestSuite) TestMultipleStructs() {
	defns := []structTestCase{
		makeStructTestCase("Simple", suite.prim),
		makeStructTestCase("Simple2", suite.prim2),
		makeStructTestCase("Compound", suite.compound),
		makeStructTestCase("Multi",
			suite.prim,
			suite.prim2,
			suite.namespacedType,
			suite.compound,
		),
	}
	suite.parseAndCheckStructs(defns...)
}
