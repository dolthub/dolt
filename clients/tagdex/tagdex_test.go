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

func createRefOfRemotePhoto(id int, tag string, cs chunks.ChunkStore) RefOfRemotePhoto {
	p := RemotePhotoDef{
		Id:          fmt.Sprintf("%d", id),
		Title:       "title" + tag,
		Url:         fmt.Sprintf("http://test.com/images/%s-%d.jpg", tag, id),
		Geoposition: GeopositionDef{Latitude: 50, Longitude: 50},
		Sizes:       MapOfSizeToStringDef{SizeDef{1, 2}: "1x2"},
		Tags:        map[string]bool{tag: true},
	}.New(cs)
	return NewRefOfRemotePhoto(types.WriteValue(p, cs))
}

func (s *testSuite) TestTagdex() {
	cs := chunks.NewLevelDBStore(s.LdbDir, 1, false)
	inputDs := dataset.NewDataset(datas.NewDataStore(cs), "input-test")

	fakePhotos := map[string]int{
		"cat": 2,
		"dog": 1,
		"nyc": 1,
		"sf":  1,
		"syd": 100,
	}

	// Build the set
	set := NewSetOfRefOfRemotePhoto(cs)
	for name, num := range fakePhotos {
		for i := 0; i < num; i++ {
			set = set.Insert(createRefOfRemotePhoto(i, name, cs))
		}
	}
	inputRef := types.WriteValue(set, cs)
	refVal := types.NewRef(inputRef)

	var err error
	inputDs, err = inputDs.Commit(refVal)
	s.NoError(err)
	inputDs.Close()

	out := s.Run(main, []string{"-in", "input-test", "-out", "tagdex-test"})
	s.Contains(out, "Indexed 105 photos from 1583 values")

	cs = chunks.NewLevelDBStore(s.LdbDir, 1, false)
	ds := dataset.NewDataset(datas.NewDataStore(cs), "tagdex-test")

	m := ds.Head().Value().(MapOfStringToSetOfRefOfRemotePhoto)

	s.Equal(uint64(len(fakePhotos)), m.Len())
	for name, num := range fakePhotos {
		s.Equal(uint64(num), m.Get(name).Len())
	}

	s.Equal("titlenyc", m.Get("nyc").First().TargetValue(cs).Title())
}
