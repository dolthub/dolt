package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/test_util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
)

func TestNomsShow(t *testing.T) {
	suite.Run(t, &nomsShowTestSuite{})
}

type nomsShowTestSuite struct {
	test_util.ClientTestSuite
}

const (
	res1 = "struct Commit {\n  parents: Set<Ref<Parent<0>>>\n  value: Value\n}({\n  parents: {},\n  value: sha1-7cecea5a8e3bcf30b35984eb7f592d98fa994a9a,\n})\n"
	res2 = "\"test string\"\n"
	res3 = "struct Commit {\n  parents: Set<Ref<Parent<0>>>\n  value: Value\n}({\n  parents: {\n    sha1-a8b8eb235f19275ada45cf3eab3eab184b181b3b,\n  },\n  value: sha1-b326291d2a568f39fa863ad63408711e676bc05b,\n})\n"
	res4 = "List<Value>([\n  \"elem1\",\n  2,\n  \"elem3\",\n])\n"
	res5 = "struct Commit {\n  parents: Set<Ref<Parent<0>>>\n  value: Value\n}({\n  parents: {\n    sha1-7324f750bc0a61920fc1fbc5c7607aed455de74e,\n  },\n  value: sha1-7cecea5a8e3bcf30b35984eb7f592d98fa994a9a,\n})\n"
)

func writeTestData(ds dataset.Dataset, value types.Value) types.Ref {
	r1 := ds.Store().WriteValue(value)
	ds, err := ds.Commit(r1)
	d.Chk.NoError(err)
	err = ds.Store().Close()
	d.Chk.NoError(err)

	return r1
}

func (s *nomsShowTestSuite) TestNomsShow() {
	datasetName := "dsTest"
	spec := fmt.Sprintf("ldb:%s:%s", s.LdbDir, datasetName)
	sp, err := flags.ParseDatasetSpec(spec)
	d.Chk.NoError(err)
	ds, err := sp.Dataset()
	d.Chk.NoError(err)

	s1 := types.NewString("test string")
	r := writeTestData(ds, s1)
	s.Equal(res1, s.Run(main, []string{spec}))

	spec1 := fmt.Sprintf("ldb:%s:%s", s.LdbDir, r.TargetRef().String())
	s.Equal(res2, s.Run(main, []string{spec1}))

	ds, err = sp.Dataset()
	list := types.NewList(types.NewString("elem1"), types.Number(2), types.NewString("elem3"))
	r = writeTestData(ds, list)
	s.Equal(res3, s.Run(main, []string{spec}))

	spec1 = fmt.Sprintf("ldb:%s:%s", s.LdbDir, r.TargetRef().String())
	s.Equal(res4, s.Run(main, []string{spec1}))

	ds, err = sp.Dataset()
	_ = writeTestData(ds, s1)
	s.Equal(res5, s.Run(main, []string{spec}))
}
