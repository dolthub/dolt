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

func (suite *ParserTestSuite) TestStruct() {
	toText := func(s TypeRef) string {
		suite.Equal(StructKind, s.Desc.Kind())
		desc := s.Desc.(StructDesc)
		return fmt.Sprintf(structTmpl, s.Name, desc.fieldsToString(), desc.unionToString())
	}

	checkStruct := func(pkg Package, str TypeRef) {
		suite.Equal(StructKind, str.Desc.Kind())
		strFields := str.Desc.(StructDesc).Fields
		strUnion := str.Desc.(StructDesc).Union

		typ := pkg.NamedTypes[str.Name]
		typFields := typ.Desc.(StructDesc).Fields
		typUnion := typ.Desc.(StructDesc).Union

		suite.Equal(str.Name, typ.Name)
		suite.Len(typFields, len(strFields))
		for i := range typFields {
			suite.EqualValues(strFields[i], typFields[i])
		}
		if strUnion != nil && suite.NotNil(typUnion) {
			suite.Len(typUnion.Choices, len(strUnion.Choices))
			for i := range typUnion.Choices {
				suite.EqualValues(strUnion.Choices[i], typUnion.Choices[i])
			}
		} else {
			suite.EqualValues(strUnion, typUnion, "If strUnion is nil, so should typUnion be.")
		}
	}

	primField := Field{"a", makePrimitiveTypeRef("Int64")}
	compoundField := Field{"set", makeCompoundTypeRef(SetKind, []TypeRef{makePrimitiveTypeRef("String")})}
	compoundOfCompoundField := Field{
		"listOfSet",
		makeCompoundTypeRef(ListKind, []TypeRef{
			makeCompoundTypeRef(SetKind, []TypeRef{makePrimitiveTypeRef("String")})})}
	mapOfNamedTypeField := Field{
		"mapOfStructToOther",
		makeCompoundTypeRef(MapKind, []TypeRef{
			makeTypeRef("", "Struct"),
			makeTypeRef("Elsewhere", "Other"),
		}),
	}
	namedTypeField := Field{"otherStruct", makeTypeRef("", "Other")}
	namespacedTypeField := Field{"otherStruct", makeTypeRef("Elsewhere", "Other")}
	union := UnionDesc{[]Field{
		Field{"a", makePrimitiveTypeRef("Int32")},
		Field{"n", makeTypeRef("NN", "Other")},
		Field{"c", makePrimitiveTypeRef("UInt32")}}}

	simpleRef := makeStructTypeRef("Simple", []Field{primField}, nil)
	withAnonUnionRef := makeStructTypeRef("WithAnon", []Field{primField}, &union)
	anonUnionFirstRef := makeStructTypeRef("WithAnonFirst", []Field{primField}, &union)
	compoundRef := makeStructTypeRef("Compound", []Field{compoundField}, &union)
	compoundOfCompoundRef := makeStructTypeRef("CofC", []Field{compoundOfCompoundField}, &union)
	namedRef := makeStructTypeRef("Named", []Field{namedTypeField}, &union)
	namespacedRef := makeStructTypeRef("Namespaced", []Field{namespacedTypeField}, &union)
	mapFieldRef := makeStructTypeRef("MapStruct", []Field{mapOfNamedTypeField}, &union)
	multiRef := makeStructTypeRef("Multi", []Field{
		primField,
		namedTypeField,
		namespacedTypeField,
		compoundField,
		compoundOfCompoundField,
		Field{"namedUnion", TypeRef{Desc: &union}},
	}, &union)
	withNamedUnionRef := makeStructTypeRef("NamedAndAnon", []Field{
		Field{"namedUnion", TypeRef{Desc: &union}},
	}, &union)
	onlyNamedUnionRef := makeStructTypeRef("NamedUnionOnly", []Field{
		Field{"namedUnion", TypeRef{Desc: &union}},
	}, nil)
	multiNamedUnionRef := makeStructTypeRef("TwoNamedAndAnon", []Field{
		Field{"namedUnion1", TypeRef{Desc: &union}},
		Field{"namedUnion2", TypeRef{Desc: &union}},
	}, &union)

	defns := []string{
		toText(simpleRef),
		toText(withAnonUnionRef),
		/* Put anon union first*/
		fmt.Sprintf(structTmpl, anonUnionFirstRef.Name, anonUnionFirstRef.Desc.(StructDesc).unionToString(), anonUnionFirstRef.Desc.(StructDesc).fieldsToString()),
		toText(compoundRef),
		toText(compoundOfCompoundRef),
		toText(namedRef),
		toText(namespacedRef),
		toText(mapFieldRef),
		toText(multiRef),
		toText(withNamedUnionRef),
		toText(onlyNamedUnionRef),
		toText(multiNamedUnionRef),
	}

	pkgDef := strings.Join(defns, "\n")
	err := d.Try(func() {
		pkg := ParsePackage("", strings.NewReader(pkgDef))
		checkStruct(pkg, simpleRef)
	})
	suite.NoError(err, pkgDef)
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
