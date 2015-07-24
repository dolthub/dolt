package nomgen

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
)

type NomgenTestSuite struct {
	suite.Suite
	ng   NG
	path string
}

func (suite *NomgenTestSuite) SetupTest() {
	dir, err := ioutil.TempDir("", "nomgen")
	suite.NoError(err)
	suite.path = path.Join(dir, "types.go")
	suite.ng = New(suite.path)
}

func (suite *NomgenTestSuite) TearDownTest() {
	os.Remove(suite.path)
}

func (suite *NomgenTestSuite) TestListSmokeTest() {
	suite.ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.ListDef"),
		types.NewString("elem"), types.NewString("int32")))
	suite.ng.WriteGo("test")
}

func (suite *NomgenTestSuite) TestSetSmokeTest() {
	suite.ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.SetDef"),
		types.NewString("elem"), types.NewString("int32")))
	suite.ng.WriteGo("test")
}

func (suite *NomgenTestSuite) TestMapSmokeTest() {
	suite.ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.MapDef"),
		types.NewString("key"), types.NewString("int32"),
		types.NewString("value"), types.NewString("bool")))

	suite.ng.WriteGo("test")
}

func (suite *NomgenTestSuite) TestStructSmokeTest() {
	suite.ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("MyStruct"),
		types.NewString("key"), types.NewString("int32"),
		types.NewString("value"), types.NewString("bool")))
	suite.ng.WriteGo("test")
}
