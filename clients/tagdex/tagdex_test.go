package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

func TestTagdex(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	util.ClientTestSuite
}

func createRefOfRemotePhoto(id int, tag string, cs chunks.ChunkSink) RefOfRemotePhoto {
	p := RemotePhotoDef{
		Id:          fmt.Sprintf("%d", id),
		Title:       "title" + tag,
		Url:         fmt.Sprintf("http://test.com/images/%s-%d.jpg", tag, id),
		Geoposition: GeopositionDef{Latitude: 50, Longitude: 50},
		Sizes:       MapOfSizeToStringDef{SizeDef{1, 2}: "1x2"},
		Tags:        map[string]bool{tag: true},
	}.New()
	return NewRefOfRemotePhoto(types.WriteValue(p, cs))
}

func (s *testSuite) TestTagdex() {
	cs := chunks.NewLevelDBStore(s.LdbDir, 1, false)
	inputDs := dataset.NewDataset(datas.NewDataStore(cs), "input-test")

	// Build the set
	set := NewSetOfRefOfRemotePhoto().Insert(
		createRefOfRemotePhoto(0, "nyc", cs),
		createRefOfRemotePhoto(1, "sf", cs),
		createRefOfRemotePhoto(2, "cat", cs),
		createRefOfRemotePhoto(3, "dog", cs),
		createRefOfRemotePhoto(4, "cat", cs), // One more cat. Cats rule!
	)
	inputRef := types.WriteValue(set, cs)
	refVal := types.NewRef(inputRef)

	var ok bool
	inputDs, ok = inputDs.Commit(refVal)
	s.True(ok)
	inputDs.Close()

	out := s.Run(main, []string{"-in", "input-test", "-out", "tagdex-test"})
	s.Contains(out, "Indexed 5 photos from 77 values")

	cs = chunks.NewLevelDBStore(s.LdbDir, 1, false)
	ds := dataset.NewDataset(datas.NewDataStore(cs), "tagdex-test")

	m := ds.Head().Value().(MapOfStringToSetOfRefOfRemotePhoto)

	s.Equal(uint64(4), m.Len())
	s.Equal(uint64(1), m.Get("nyc").Len())
	s.Equal(uint64(1), m.Get("sf").Len())
	s.Equal(uint64(2), m.Get("cat").Len())
	s.Equal(uint64(1), m.Get("dog").Len())

	s.Equal("titlenyc", m.Get("nyc").Any().TargetValue(cs).Title())
	s.Equal("1", m.Get("sf").Any().TargetValue(cs).Id())
}
