package main

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

func TestCSVImporter(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	util.ClientTestSuite
}

func (s *testSuite) TestCSVImporter() {
	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)

	_, err = input.WriteString("a,b\n")
	d.Chk.NoError(err)
	for i := 0; i < 100; i++ {
		_, err = input.WriteString(fmt.Sprintf("a%d,%d\n", i, i))
		d.Chk.NoError(err)
	}
	_, err = input.Seek(0, 0)
	d.Chk.NoError(err)

	out := s.Run(main, []string{"-ds", "csv", input.Name()})
	s.Equal("", out)

	cs := chunks.NewLevelDBStore(s.LdbDir, 1, false)
	ds := dataset.NewDataset(datas.NewDataStore(cs), "csv")
	defer ds.Close()

	l := ds.Head().Value().(types.List)
	s.Equal(uint64(100), l.Len())

	i := uint64(0)
	l.IterAll(func(v types.Value, j uint64) {
		s.Equal(i, j)
		st := types.ReadValue(v.(types.Ref).TargetRef(), cs).(types.Struct)
		s.Equal(types.NewString(fmt.Sprintf("a%d", i)), st.Get("a"))
		s.Equal(types.NewString(fmt.Sprintf("%d", i)), st.Get("b"))
		i++
	})
}

func (s *testSuite) TestCSVImporterWithPipe() {
	oldDelimiter := delimiter
	newDelimiter := "|"
	delimiter = &newDelimiter
	defer func() { delimiter = oldDelimiter }()

	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)

	_, err = input.WriteString("a|b\n1|2\n")
	d.Chk.NoError(err)
	out := s.Run(main, []string{"-ds", "csv", input.Name()})
	s.Equal("", out)

	cs := chunks.NewLevelDBStore(s.LdbDir, 1, false)
	ds := dataset.NewDataset(datas.NewDataStore(cs), "csv")
	defer ds.Close()

	l := ds.Head().Value().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := types.ReadValue(v.(types.Ref).TargetRef(), cs).(types.Struct)
	s.Equal(types.NewString("1"), st.Get("a"))
	s.Equal(types.NewString("2"), st.Get("b"))
}

func (s *testSuite) TestCSVImporterWithExternalHeader() {
	oldHeader := header
	newHeader := "x,y"
	header = &newHeader
	defer func() { header = oldHeader }()

	input, err := ioutil.TempFile(s.TempDir, "")
	d.Chk.NoError(err)

	_, err = input.WriteString("7,8\n")
	d.Chk.NoError(err)
	out := s.Run(main, []string{"-ds", "csv", input.Name()})
	s.Equal("", out)

	cs := chunks.NewLevelDBStore(s.LdbDir, 1, false)
	ds := dataset.NewDataset(datas.NewDataStore(cs), "csv")
	defer ds.Close()

	l := ds.Head().Value().(types.List)
	s.Equal(uint64(1), l.Len())
	v := l.Get(0)
	st := types.ReadValue(v.(types.Ref).TargetRef(), cs).(types.Struct)
	s.Equal(types.NewString("7"), st.Get("x"))
	s.Equal(types.NewString("8"), st.Get("y"))
}
