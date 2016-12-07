// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestBasics(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	clienttest.ClientTestSuite
}

func (s *testSuite) TestWin() {
	type Face struct {
		Name       string
		X, Y, W, H int
	}

	type Date struct {
		NsSinceEpoch int
	}

	type Photo struct {
		Id    string
		Title string
		Tags  types.Set
		Faces types.Set
		Sizes map[struct {
			Width  int
			Height int
		}]string
		DateTaken     Date
		DatePublished Date
		DateUpdated   Date
	}

	type PhotoGroup struct {
		Id     string
		Cover  Photo
		Photos []Photo
	}

	getTags := func(n int) types.Set {
		s := types.NewSet()
		for i := 0; i < n; i++ {
			s = s.Insert(types.String(fmt.Sprintf("tag%d", i)))
		}
		return s
	}

	getFaces := func(n int) types.Set {
		set := types.NewSet()
		for i := 0; i < n; i++ {
			v, err := marshal.Marshal(Face{
				fmt.Sprintf("harry%d", i),
				i, i, n, n,
			})
			s.NoError(err)
			set = set.Insert(v)
		}
		return set
	}

	getPhoto := func(n int) Photo {
		return Photo{
			Id:    fmt.Sprintf("photo%d", n),
			Title: fmt.Sprintf("photo %d", n),
			Tags:  getTags(n),
			Sizes: map[struct{ Width, Height int }]string{
				{100, 100}: "100.jpg"},
			DateTaken:     Date{n * 10},
			DatePublished: Date{n*10 + 1},
			DateUpdated:   Date{n*10 + 2},
			Faces:         getFaces(n),
		}
	}

	getPhotoGroup := func(n int) PhotoGroup {
		return PhotoGroup{
			Id:    fmt.Sprintf("pg%d", n),
			Cover: getPhoto(n),
		}
	}

	groups := []PhotoGroup{}
	for i := 0; i < 5; i++ {
		groups = append(groups, getPhotoGroup(i))
	}

	sp, err := spec.ForDataset(fmt.Sprintf("ldb:%s::test", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	v, err := marshal.Marshal(groups)
	s.NoError(err)
	_, err = sp.GetDatabase().CommitValue(sp.GetDataset(), v)
	s.NoError(err)

	_, _ = s.MustRun(main, []string{"--out-ds", "idx", "--db", s.LdbDir, "test"})

	sp, err = spec.ForDataset(fmt.Sprintf("%s::idx", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	var idx struct {
		ByDate       map[int]types.Set
		ByTag        map[string]map[int]types.Set
		ByFace       map[string]map[int]types.Set
		TagsByCount  map[int]types.Set
		FacesByCount map[int]types.Set
	}
	marshal.Unmarshal(sp.GetDataset().HeadValue(), &idx)

	s.Equal(5, len(idx.ByDate))
	for i := 0; i < 5; i++ {
		k := -i * 10
		if k == 0 {
			k = -1
		}
		s.Equal(uint64(1), idx.ByDate[k].Len())
		p := idx.ByDate[k].First().(types.Struct).Get("cover").(types.Struct)
		s.Equal(fmt.Sprintf("photo %d", i), string(p.Get("title").(types.String)))
	}

	s.Equal(4, len(idx.ByTag))
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("tag%d", i)
		v := idx.ByTag[k]
		s.Equal(4-i, len(v))
	}

	s.Equal(4, len(idx.ByFace))
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("harry%d", i)
		v := idx.ByFace[k]
		s.Equal(4-i, len(v))
	}

	s.Equal(4, len(idx.TagsByCount))
	for i := 0; i < 4; i++ {
		tags := idx.TagsByCount[-4+i]
		s.Equal(1, int(tags.Len()))
		k := fmt.Sprintf("tag%d", i)
		s.True(tags.Has(types.String(k)))
	}

	s.Equal(4, len(idx.FacesByCount))
	for i := 0; i < 4; i++ {
		tags := idx.FacesByCount[-4+i]
		s.Equal(1, int(tags.Len()))
		k := fmt.Sprintf("harry%d", i)
		s.True(tags.Has(types.String(k)))
	}
}
