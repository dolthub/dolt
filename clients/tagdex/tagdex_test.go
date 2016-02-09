package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/stretchr/testify/suite"
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
		Geoposition: GeopositionDef{Latitude: 50, Longitude: 50},
		Sizes:       MapOfSizeToStringDef{SizeDef{1, 2}: fmt.Sprintf("http://test.com/images/%s-%d.jpg", tag, id)},
		Tags:        map[string]bool{tag: true},
	}.New()
	return NewRefOfRemotePhoto(types.WriteValue(p, cs))
}

func (s *testSuite) TestTagdex() {
	sn := "storeName"
	cs := chunks.NewLevelDBStore(s.LdbDir, sn, 1, false)
	inputDs := dataset.NewDataset(datas.NewDataStore(cs), "input-test")

	fakePhotos := map[string]int{
		"cat": 2,
		"dog": 1,
		"nyc": 1,
		"sf":  1,
		"syd": 100,
	}

	// Build the set
	set := NewSetOfRefOfRemotePhoto()
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
	inputDs.Store().Close()

	out := s.Run(main, []string{"-store", sn, "-in", "input-test", "-out", "tagdex-test"})
	s.Contains(out, "Indexed 105 photos")

	cs = chunks.NewLevelDBStore(s.LdbDir, sn, 1, false)
	ds := dataset.NewDataset(datas.NewDataStore(cs), "tagdex-test")

	m := ds.Head().Value().(MapOfStringToSetOfRefOfRemotePhoto)

	s.Equal(uint64(len(fakePhotos)), m.Len())
	for name, num := range fakePhotos {
		s.Equal(uint64(num), m.Get(name).Len())
	}

	s.Equal("titlenyc", m.Get("nyc").First().TargetValue(cs).Title())
}
