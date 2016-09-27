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
	sp := fmt.Sprintf("ldb:%s::test", s.LdbDir)
	db, ds, _ := spec.GetDataset(sp)

	type Date struct {
		NsSinceEpoch int
	}

	type Photo struct {
		Title string
		Tags  types.Set
		Sizes map[struct {
			Width  int
			Height int
		}]string
		DateTaken     Date
		DatePublished Date
		DateUpdated   Date
	}

	getTags := func(n int) types.Set {
		s := types.NewSet()
		for i := 0; i < n; i++ {
			s = s.Insert(types.String(fmt.Sprintf("tag%d", i)))
		}
		return s
	}

	getPhoto := func(n int) Photo {
		return Photo{
			Title: fmt.Sprintf("photo %d", n),
			Tags:  getTags(n),
			Sizes: map[struct{ Width, Height int }]string{
				{100, 100}: "100.jpg"},
			DateTaken:     Date{n * 10},
			DatePublished: Date{n*10 + 1},
			DateUpdated:   Date{n*10 + 2},
		}
	}

	photos := []Photo{}
	for i := 0; i < 5; i++ {
		photos = append(photos, getPhoto(i))
	}

	v, err := marshal.Marshal(photos)
	s.NoError(err)
	ds, err = db.CommitValue(ds, v)
	s.NoError(err)
	db.Close()

	_, _ = s.MustRun(main, []string{"--out-ds", "idx", "--db", s.LdbDir, "test"})

	db, ds, _ = spec.GetDataset(fmt.Sprintf("%s::idx", s.LdbDir))
	var idx struct {
		ByDate map[int]types.Set
		ByTag  map[string]map[int]types.Set
	}
	marshal.Unmarshal(ds.HeadValue(), &idx)

	s.Equal(5, len(idx.ByDate))
	for i := 0; i < 5; i++ {
		s.Equal(uint64(1), idx.ByDate[-i*10].Len())
		p := idx.ByDate[-i*10].First().(types.Struct)
		s.Equal(fmt.Sprintf("photo %d", i), string(p.Get("title").(types.String)))
	}

	s.Equal(4, len(idx.ByTag))
	for i := 1; i < 5; i++ {
		k := fmt.Sprintf("tag%d", i)
		v := idx.ByTag[k]
		s.Equal(4-i, len(v))
	}
}
