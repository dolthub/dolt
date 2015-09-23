package parse

import (
	"fmt"
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/types"
)

const (
	union      = "union { bool :Bool t2 :Blob }"
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
	suite.Panics(func() { ParsePackage("", strings.NewReader(test)) }, msg)
}

func (suite *ParserTestSuite) parseNotPanics(test string) {
	suite.NotPanics(func() { ParsePackage("", strings.NewReader(test)) }, test)
}

func (suite *ParserTestSuite) TestAlias() {
	importTmpl := `alias Noms = import "%s"`
	ref := "sha1-ffffffff"
	path := `some/path/\"quotes\"/path`

	pkg := ParsePackage("", strings.NewReader(fmt.Sprintf(importTmpl, ref)))
	suite.Equal(ref, pkg.Aliases["Noms"])

	pkg = ParsePackage("", strings.NewReader(fmt.Sprintf(importTmpl, path)))
	suite.Equal(path, pkg.Aliases["Noms"])
}

func (suite *ParserTestSuite) TestUsing() {
	usingDecls := `
using Map(String, Simple)
using List(Noms.Commit)
`
	pkg := ParsePackage("", strings.NewReader(usingDecls))
	suite.Len(pkg.UsingDeclarations, 2)

	suite.Equal(types.MapKind, pkg.UsingDeclarations[0].Desc.Kind())
	suite.EqualValues([]TypeRef{makePrimitiveTypeRef("String"), makeTypeRef("", "Simple")},
		pkg.UsingDeclarations[0].Desc.(CompoundDesc).ElemTypes)

	suite.Equal(types.ListKind, pkg.UsingDeclarations[1].Desc.Kind())
	suite.EqualValues([]TypeRef{makeTypeRef("Noms", "Commit")},
		pkg.UsingDeclarations[1].Desc.(CompoundDesc).ElemTypes)
}

func (suite *ParserTestSuite) TestBadUsing() {
	suite.Panics(func() { ParsePackage("", strings.NewReader("using Blob")) }, "Can't 'use' a primitive.")
	suite.Panics(func() { ParsePackage("", strings.NewReader("using Noms.Commit")) }, "Can't 'use' a type from another package.")
	suite.Panics(func() { ParsePackage("", strings.NewReader("using f@(k")) }, "Can't 'use' illegal identifier.")
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

	dupNameInUnion := "struct s { union { a: Bool a :Int32 } }"
	suite.parsePanics(dupNameInUnion, "union choices must have unique names.")

	dupNameInNamedUnion := "struct s { u :union { a: Bool a :Int32 } }"
	suite.parsePanics(dupNameInNamedUnion, "union choices must have unique names.")

	twoAnonUnion := fmt.Sprintf(structTmpl, "str", union, union)
	suite.parsePanics(twoAnonUnion, "Can't have two anonymous unions.")
}

func (suite *ParserTestSuite) TestStructParse() {
	oneLine := "struct str { a :Bool b : Blob c: Blob }"
	suite.parseNotPanics(oneLine)

	noSpace := "struct str{a:Bool}"
	suite.parseNotPanics(noSpace)

	multiLine := "\nstruct str {\na :Bool\n}"
	suite.parseNotPanics(multiLine)

	anonUnion := fmt.Sprintf(structTmpl, "str", "a :Bool\n", union)
	suite.parseNotPanics(anonUnion)

	namedUnions := fmt.Sprintf(structTmpl, "str", "a :Bool\nun1 :"+union, "un2 :"+union)
	suite.parseNotPanics(namedUnions)

	for k := range primitiveToDesc {
		suite.parseNotPanics(fmt.Sprintf(structTmpl, "s", "a :"+k, ""))
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

func (suite *ParserTestSuite) TestEnum() {
	enumTmpl := `enum %s { %s }`
	name := "Enum"
	ids := []string{"e1", "e2", "e4"}
	enum := fmt.Sprintf(enumTmpl, name, strings.Join(ids, "\n"))

	pkg := ParsePackage("", strings.NewReader(enum))
	suite.Equal(name, pkg.NamedTypes[name].Name)
	suite.EqualValues(ids, pkg.NamedTypes[name].Desc.(EnumDesc).IDs)
}

type ParsedResultTestSuite struct {
	suite.Suite

	primField               testField
	compoundField           testField
	compoundOfCompoundField testField
	mapOfNamedTypeField     testField
	namedTypeField          testField
	namespacedTypeField     testField
	union                   UnionDesc
}

func (suite *ParsedResultTestSuite) SetupTest() {
	suite.primField = testField{"a", makePrimitiveTypeRef("Int64")}
	suite.compoundField = testField{"set", makeCompoundTypeRef(types.SetKind, []TypeRef{makePrimitiveTypeRef("String")})}
	suite.compoundOfCompoundField = testField{
		"listOfSet",
		makeCompoundTypeRef(types.ListKind, []TypeRef{
			makeCompoundTypeRef(types.SetKind, []TypeRef{makePrimitiveTypeRef("String")})})}
	suite.mapOfNamedTypeField = testField{
		"mapOfStructToOther",
		makeCompoundTypeRef(types.MapKind, []TypeRef{
			makeTypeRef("", "Struct"),
			makeTypeRef("Elsewhere", "Other"),
		}),
	}
	suite.namedTypeField = testField{"otherStruct", makeTypeRef("", "Other")}
	suite.namespacedTypeField = testField{"namespacedStruct", makeTypeRef("Elsewhere", "Other")}
	suite.union = UnionDesc{[]Field{
		Field{"a", makePrimitiveTypeRef("Int32")},
		Field{"n", makeTypeRef("NN", "Other")},
		Field{"c", makePrimitiveTypeRef("UInt32")},
	}}
}

type structTestCase struct {
	Name   string
	Union  *UnionDesc
	Fields []testField
}

func makeStructTestCase(n string, u *UnionDesc, fields ...testField) structTestCase {
	return structTestCase{n, u, fields}
}

func (s structTestCase) toText() string {
	return fmt.Sprintf(structTmpl, s.Name, s.fieldsToString(), s.unionToString())
}

func (s structTestCase) fieldsToString() (out string) {
	for _, f := range s.Fields {
		out += f.Name + " :" + f.D.describe() + "\n"
	}
	return
}

func (s structTestCase) unionToString() string {
	if s.Union == nil {
		return ""
	}
	return s.Union.describe()
}

type testField struct {
	Name string
	D    describable
}

func (t testField) toField() Field {
	return Field{t.Name, t.D.(TypeRef)}
}

type describable interface {
	describe() string
}

func (suite *ParsedResultTestSuite) checkStruct(pkg Package, s structTestCase) {
	typ := pkg.NamedTypes[s.Name]
	typFields := typ.Desc.(StructDesc).Fields
	typUnion := typ.Desc.(StructDesc).Union

	suite.Equal(s.Name, typ.Name)
	suite.Len(typFields, len(s.Fields))
	for i, f := range s.Fields {
		// Named unions are syntactic sugar for a struct Field that points to an anonymous struct containing an anonymous union.
		// So, if the field in the test input was a union...
		if desc, ok := f.D.(*UnionDesc); ok {
			// ...make sure the names are the same...
			suite.Equal(f.Name, typFields[i].Name)
			if tfd, ok := typFields[i].T.Desc.(StructDesc); ok {
				// ...and that the IR has a TypeRef of StructKind with no fields, but an anonymous union.
				suite.Len(tfd.Fields, 0)
				suite.NotNil(tfd.Union)
				suite.EqualValues(desc, tfd.Union)
			} else {
				suite.Fail("Named unions must be parsed as anonymous structs containing an anonymous union.", "%#v", typFields[i])
			}
		} else {
			suite.EqualValues(s.Fields[i].toField(), typFields[i])
		}
	}
	if s.Union != nil && suite.NotNil(typUnion) {
		suite.Len(typUnion.Choices, len(s.Union.Choices))
		for i := range s.Union.Choices {
			suite.EqualValues(s.Union.Choices[i], typUnion.Choices[i])
		}
	} else {
		suite.EqualValues(s.Union, typUnion, "If s.Union is nil, so should typUnion be.")
	}

}

func (suite *ParsedResultTestSuite) parseAndCheckStructs(structs ...structTestCase) {
	pkgDef := ""
	for _, s := range structs {
		pkgDef += s.toText() + "\n"
	}
	err := d.Try(func() {
		pkg := ParsePackage("", strings.NewReader(pkgDef))
		for _, s := range structs {
			suite.checkStruct(pkg, s)
		}
	})
	suite.NoError(err, pkgDef)
}

func (suite *ParsedResultTestSuite) TestPrimitiveField() {
	suite.parseAndCheckStructs(makeStructTestCase("Simple", nil, suite.primField))
}

func (suite *ParsedResultTestSuite) TestAnonUnion() {
	suite.parseAndCheckStructs(makeStructTestCase("WithAnon", &suite.union, suite.primField))
}

func (suite *ParsedResultTestSuite) TestAnonUnionFirst() {
	anonUnionFirst := makeStructTestCase("WithAnonFirst", &suite.union, suite.primField)

	pkgDef := fmt.Sprintf(structTmpl, anonUnionFirst.Name, anonUnionFirst.unionToString(), anonUnionFirst.fieldsToString())
	err := d.Try(func() {
		pkg := ParsePackage("", strings.NewReader(pkgDef))
		suite.checkStruct(pkg, anonUnionFirst)
	})
	suite.NoError(err, pkgDef)
}

func (suite *ParsedResultTestSuite) TestCommentNextToName() {
	withComment := makeStructTestCase("WithComment", &suite.union, suite.primField)

	pkgDef := fmt.Sprintf(structTmpl, "/* Oy! */"+withComment.Name, withComment.unionToString(), withComment.fieldsToString())
	err := d.Try(func() {
		pkg := ParsePackage("", strings.NewReader(pkgDef))
		suite.checkStruct(pkg, withComment)
	})
	suite.NoError(err, pkgDef)
}

func (suite *ParsedResultTestSuite) TestCommentAmongFields() {
	withComment := makeStructTestCase("WithComment", &suite.union, suite.primField)

	pkgDef := fmt.Sprintf(structTmpl, withComment.Name, withComment.fieldsToString()+"\n// Nope\n", withComment.unionToString())
	err := d.Try(func() {
		pkg := ParsePackage("", strings.NewReader(pkgDef))
		suite.checkStruct(pkg, withComment)
	})
	suite.NoError(err, pkgDef)
}

func (suite *ParsedResultTestSuite) TestCompoundField() {
	suite.parseAndCheckStructs(makeStructTestCase("Compound", &suite.union, suite.compoundField))
}

func (suite *ParsedResultTestSuite) TestCompoundOfCompoundField() {
	suite.parseAndCheckStructs(makeStructTestCase("CofC", &suite.union, suite.compoundOfCompoundField))
}

func (suite *ParsedResultTestSuite) TestNamedTypeField() {
	suite.parseAndCheckStructs(makeStructTestCase("Named", &suite.union, suite.namedTypeField))
}

func (suite *ParsedResultTestSuite) TestNamespacedTypeField() {
	suite.parseAndCheckStructs(makeStructTestCase("Namespaced", &suite.union, suite.namespacedTypeField))
}

func (suite *ParsedResultTestSuite) TestMapOfNamedTypeField() {
	suite.parseAndCheckStructs(makeStructTestCase("MapStruct", &suite.union, suite.mapOfNamedTypeField))
}

func (suite *ParsedResultTestSuite) TestMultipleFields() {
	suite.parseAndCheckStructs(makeStructTestCase("Multi", &suite.union,
		suite.primField,
		suite.namedTypeField,
		suite.namespacedTypeField,
		suite.compoundField,
		suite.compoundOfCompoundField,
		testField{"namedUnion", &suite.union},
	))
}

func (suite *ParsedResultTestSuite) TestNamedAndAnonUnion() {
	suite.parseAndCheckStructs(makeStructTestCase("NamedAndAnon", &suite.union,
		testField{"namedUnion", &suite.union},
	))
}

func (suite *ParsedResultTestSuite) TestNamedUnionOnly() {
	suite.parseAndCheckStructs(makeStructTestCase("NamedUnionOnly", nil,
		testField{"namedUnion", &suite.union},
	))
}

func (suite *ParsedResultTestSuite) TestTwoNamedAndAnonUnion() {
	suite.parseAndCheckStructs(makeStructTestCase("TwoNamedAndAnon", &suite.union,
		testField{"namedUnion1", &suite.union},
		testField{"namedUnion2", &suite.union},
	))
}

func (suite *ParsedResultTestSuite) TestMultipleStructs() {
	defns := []structTestCase{
		makeStructTestCase("Simple", nil, suite.primField),
		makeStructTestCase("Compound", nil, suite.compoundField),
		makeStructTestCase("CompoundWithUnion", &suite.union, suite.compoundField),
		makeStructTestCase("TwoNamedAndAnon", &suite.union,
			testField{"namedUnion1", &suite.union},
			testField{"namedUnion2", &suite.union},
		),
		makeStructTestCase("Multi", &suite.union,
			suite.primField,
			suite.namespacedTypeField,
			suite.compoundField,
			testField{"namedUnion", &suite.union},
		),
	}
	suite.parseAndCheckStructs(defns...)
}
