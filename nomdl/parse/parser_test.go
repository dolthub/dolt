package parse

import (
	"fmt"
	"strings"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/d"
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

	suite.Equal(MapKind, pkg.UsingDeclarations[0].Desc.Kind())
	suite.EqualValues([]TypeRef{makePrimitiveTypeRef("String"), makeTypeRef("", "Simple")},
		pkg.UsingDeclarations[0].Desc.(CompoundDesc).ElemTypes)

	suite.Equal(ListKind, pkg.UsingDeclarations[1].Desc.Kind())
	suite.EqualValues([]TypeRef{makeTypeRef("Noms", "Commit")},
		pkg.UsingDeclarations[1].Desc.(CompoundDesc).ElemTypes)
}

func (suite *ParserTestSuite) TestBadUsing() {
	suite.Panics(func() { ParsePackage("", strings.NewReader("using Blob")) }, "Can't 'use' a primitive.")
	suite.Panics(func() { ParsePackage("", strings.NewReader("using Noms.Commit")) }, "Can't 'use' a type from another package.")
	suite.Panics(func() { ParsePackage("", strings.NewReader("using f@(k")) }, "Can't 'use' illegal identifier.")
}

func (suite *ParserTestSuite) TestBadStructParse() {
	panics := func(test, msg string) {
		suite.Panics(func() { ParsePackage("", strings.NewReader(test)) }, msg)
	}

	noFields := "struct str { }"
	panics(noFields, "Struct must have fields.")

	noName := "struct { a :Bool }"
	panics(noName, "Struct must have name.")

	badName := "struct *ff { a :Bool }"
	panics(badName, "Struct must have legal name.")

	dupName := "struct str { a :Bool a :Bool }"
	panics(dupName, "Fields must have unique names.")

	dupNameInUnion := "struct s { union { a: Bool a :Int32 } }"
	panics(dupNameInUnion, "union choices must have unique names.")

	dupNameInNamedUnion := "struct s { u :union { a: Bool a :Int32 } }"
	panics(dupNameInNamedUnion, "union choices must have unique names.")

	twoAnonUnion := fmt.Sprintf(structTmpl, "str", union, union)
	panics(twoAnonUnion, "Can't have two anonymous unions.")

}

func (suite *ParserTestSuite) TestStructParse() {

	notPanics := func(test string) {
		suite.NotPanics(func() { ParsePackage("", strings.NewReader(test)) }, test)
	}

	oneLine := "struct str { a :Bool b : Blob c: Blob }"
	notPanics(oneLine)

	noSpace := "struct str{a:Bool}"
	notPanics(noSpace)

	multiLine := "struct str {\na :Bool\n}"
	notPanics(multiLine)

	anonUnion := fmt.Sprintf(structTmpl, "str", "a :Bool\n", union)
	notPanics(anonUnion)

	namedUnions := fmt.Sprintf(structTmpl, "str", "a :Bool\nun1 :"+union, "un2 :"+union)
	notPanics(namedUnions)

	for k := range primitiveToDesc {
		notPanics(fmt.Sprintf(structTmpl, "s", "a :"+k, ""))
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

	primField               Field
	compoundField           Field
	compoundOfCompoundField Field
	mapOfNamedTypeField     Field
	namedTypeField          Field
	namespacedTypeField     Field
	union                   UnionDesc
}

func (suite *ParsedResultTestSuite) SetupTest() {
	suite.primField = Field{"a", makePrimitiveTypeRef("Int64")}
	suite.compoundField = Field{"set", makeCompoundTypeRef(SetKind, []TypeRef{makePrimitiveTypeRef("String")})}
	suite.compoundOfCompoundField = Field{
		"listOfSet",
		makeCompoundTypeRef(ListKind, []TypeRef{
			makeCompoundTypeRef(SetKind, []TypeRef{makePrimitiveTypeRef("String")})})}
	suite.mapOfNamedTypeField = Field{
		"mapOfStructToOther",
		makeCompoundTypeRef(MapKind, []TypeRef{
			makeTypeRef("", "Struct"),
			makeTypeRef("Elsewhere", "Other"),
		}),
	}
	suite.namedTypeField = Field{"otherStruct", makeTypeRef("", "Other")}
	suite.namespacedTypeField = Field{"namespacedStruct", makeTypeRef("Elsewhere", "Other")}
	suite.union = UnionDesc{[]Field{
		Field{"a", makePrimitiveTypeRef("Int32")},
		Field{"n", makeTypeRef("NN", "Other")},
		Field{"c", makePrimitiveTypeRef("UInt32")},
	}}
}

func (suite *ParsedResultTestSuite) toText(s TypeRef) string {
	suite.Equal(StructKind, s.Desc.Kind())
	desc := s.Desc.(StructDesc)
	return fmt.Sprintf(structTmpl, s.Name, desc.fieldsToString(), desc.unionToString())
}

func (s StructDesc) fieldsToString() (out string) {
	for _, f := range s.Fields {
		out += f.Name + " :" + f.T.describe() + "\n"
	}
	return
}

func (s StructDesc) unionToString() string {
	if s.Union == nil {
		return ""
	}
	return s.Union.describe()
}

func (suite *ParsedResultTestSuite) checkStruct(pkg Package, str TypeRef) {
	suite.Equal(StructKind, str.Desc.Kind())
	strFields := str.Desc.(StructDesc).Fields
	strUnion := str.Desc.(StructDesc).Union

	typ := pkg.NamedTypes[str.Name]
	typFields := typ.Desc.(StructDesc).Fields
	typUnion := typ.Desc.(StructDesc).Union

	suite.Equal(str.Name, typ.Name)
	suite.Len(typFields, len(strFields))
	for i, f := range strFields {
		// Named unions are syntactic sugar for a struct Field that points to an anonymous struct containing an anonymous union.
		// So, if the field in the input was of UnionKind...
		if f.T.Desc != nil && f.T.Desc.Kind() == UnionKind {
			// ...make sure the names are the same...
			suite.Equal(f.Name, typFields[i].Name)
			if tfd, ok := typFields[i].T.Desc.(StructDesc); ok {
				// ...and that the IR has a TypeRef of StructKind with no fields, but an anonymous union.
				suite.Len(tfd.Fields, 0)
				suite.NotNil(tfd.Union)
				suite.EqualValues(f.T.Desc, tfd.Union)
			} else {
				suite.Fail("Named unions must be parsed as anonymous structs containing an anonymous union.", "%#v", typFields[i])
			}
		} else {
			suite.EqualValues(f, typFields[i])
		}
	}
	if strUnion != nil && suite.NotNil(typUnion) {
		suite.Len(typUnion.Choices, len(strUnion.Choices))
		for i := range strUnion.Choices {
			suite.EqualValues(strUnion.Choices[i], typUnion.Choices[i])
		}
	} else {
		suite.EqualValues(strUnion, typUnion, "If strUnion is nil, so should typUnion be.")
	}

}

func (suite *ParsedResultTestSuite) parseAndCheckStructs(structs ...TypeRef) {
	pkgDef := ""
	for _, s := range structs {
		pkgDef += suite.toText(s) + "\n"
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
	suite.parseAndCheckStructs(makeStructTypeRef("Simple", []Field{suite.primField}, nil))
}

func (suite *ParsedResultTestSuite) TestAnonUnion() {
	suite.parseAndCheckStructs(makeStructTypeRef("WithAnon", []Field{suite.primField}, &suite.union))
}

func (suite *ParsedResultTestSuite) TestAnonUnionFirst() {
	anonUnionFirstRef := makeStructTypeRef("WithAnonFirst", []Field{suite.primField}, &suite.union)

	pkgDef := fmt.Sprintf(structTmpl, anonUnionFirstRef.Name, anonUnionFirstRef.Desc.(StructDesc).unionToString(), anonUnionFirstRef.Desc.(StructDesc).fieldsToString())
	err := d.Try(func() {
		pkg := ParsePackage("", strings.NewReader(pkgDef))
		suite.checkStruct(pkg, anonUnionFirstRef)
	})
	suite.NoError(err, pkgDef)
}

func (suite *ParsedResultTestSuite) TestCompoundField() {
	suite.parseAndCheckStructs(makeStructTypeRef("Compound", []Field{suite.compoundField}, &suite.union))
}

func (suite *ParsedResultTestSuite) TestCompoundOfCompoundField() {
	suite.parseAndCheckStructs(makeStructTypeRef("CofC", []Field{suite.compoundOfCompoundField}, &suite.union))
}

func (suite *ParsedResultTestSuite) TestNamedTypeField() {
	suite.parseAndCheckStructs(makeStructTypeRef("Named", []Field{suite.namedTypeField}, &suite.union))
}

func (suite *ParsedResultTestSuite) TestNamespacedTypeField() {
	suite.parseAndCheckStructs(makeStructTypeRef("Namespaced", []Field{suite.namespacedTypeField}, &suite.union))
}

func (suite *ParsedResultTestSuite) TestMapOfNamedTypeField() {
	suite.parseAndCheckStructs(makeStructTypeRef("MapStruct", []Field{suite.mapOfNamedTypeField}, &suite.union))
}

func (suite *ParsedResultTestSuite) TestMultipleFields() {
	suite.parseAndCheckStructs(makeStructTypeRef("Multi", []Field{
		Field{suite.primField.Name, suite.primField.T},
		Field{suite.namedTypeField.Name, suite.namedTypeField.T},
		Field{suite.namespacedTypeField.Name, suite.namespacedTypeField.T},
		Field{suite.compoundField.Name, suite.compoundField.T},
		Field{suite.compoundOfCompoundField.Name, suite.compoundOfCompoundField.T},
		Field{"namedUnion", TypeRef{Desc: &suite.union}},
	}, &suite.union))
}

func (suite *ParsedResultTestSuite) TestNamedAndAnonUnion() {
	suite.parseAndCheckStructs(makeStructTypeRef("NamedAndAnon", []Field{
		Field{"namedUnion", TypeRef{Desc: &suite.union}},
	}, &suite.union))
}

func (suite *ParsedResultTestSuite) TestNamedUnionOnly() {
	suite.parseAndCheckStructs(makeStructTypeRef("NamedUnionOnly", []Field{
		Field{"namedUnion", TypeRef{Desc: &suite.union}},
	}, nil))
}

func (suite *ParsedResultTestSuite) TestTwoNamedAndAnonUnion() {
	suite.parseAndCheckStructs(makeStructTypeRef("TwoNamedAndAnon", []Field{
		Field{"namedUnion1", TypeRef{Desc: &suite.union}},
		Field{"namedUnion2", TypeRef{Desc: &suite.union}},
	}, &suite.union))
}

func (suite *ParsedResultTestSuite) TestMultipleStructs() {
	defns := []TypeRef{
		makeStructTypeRef("Simple", []Field{suite.primField}, nil),
		makeStructTypeRef("Compound", []Field{suite.compoundField}, nil),
		makeStructTypeRef("CompoundWithUnion", []Field{suite.compoundField}, &suite.union),
		makeStructTypeRef("TwoNamedAndAnon", []Field{
			Field{"namedUnion1", TypeRef{Desc: &suite.union}},
			Field{"namedUnion2", TypeRef{Desc: &suite.union}},
		}, &suite.union),
		makeStructTypeRef("Multi", []Field{
			Field{suite.primField.Name, suite.primField.T},
			Field{suite.namespacedTypeField.Name, suite.namespacedTypeField.T},
			Field{suite.compoundField.Name, suite.compoundField.T},
			Field{"namedUnion", TypeRef{Desc: &suite.union}},
		}, &suite.union),
	}
	suite.parseAndCheckStructs(defns...)
}
