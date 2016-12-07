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

func (s *testSuite) TestBasic() {
	sp, err := spec.ForDataset(fmt.Sprintf("ldb:%s::test", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	data := types.NewSet(
		// first group
		marshal.MustMarshal(Photo{
			Id:        "42",
			DateTaken: Date{NsSinceEpoch: float64(42 * 1e9)},
		}),
		marshal.MustMarshal(Photo{
			Id:        "43",
			DateTaken: Date{NsSinceEpoch: float64(43 * 1e9)},
		}),
		marshal.MustMarshal(Photo{
			Id:        "44",
			DateTaken: Date{NsSinceEpoch: float64(40 * 1e9)},
		}),
		marshal.MustMarshal(Photo{
			Id:        "45",
			DateTaken: Date{NsSinceEpoch: float64(48 * 1e9)},
		}),

		// second group
		marshal.MustMarshal(Photo{
			Id:        "46",
			DateTaken: Date{NsSinceEpoch: float64(54 * 1e9)},
		}),
		marshal.MustMarshal(Photo{
			Id:        "47",
			DateTaken: Date{NsSinceEpoch: float64(55 * 1e9)},
		}),

		// No dupes
		marshal.MustMarshal(Photo{
			Id:        "48",
			DateTaken: Date{NsSinceEpoch: float64(61 * 1e9)},
		}),

		// If the DateTaken is zero, it should end up in its own group
		marshal.MustMarshal(Photo{
			Id:        "49",
			DateTaken: Date{NsSinceEpoch: float64(0)},
		}),
		marshal.MustMarshal(Photo{
			Id:        "50",
			DateTaken: Date{NsSinceEpoch: float64(0)},
		}),

		// If the DateTaken is not present, it should end up in its own group
		types.NewStruct("Photo", types.StructData{
			"id": types.String("51"),
		}),
		types.NewStruct("Photo", types.StructData{
			"id": types.String("52"),
		}),
	)

	sp.GetDatabase().CommitValue(sp.GetDataset(), data)
	s.MustRun(main, []string{"--out-ds", "dedupd", "--db", s.LdbDir, "test.value"})
	sp, err = spec.ForDataset(fmt.Sprintf("%s::dedupd", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	var result struct {
		Groups []struct {
			Cover  Photo
			Photos []Photo
		}
	}
	err = marshal.Unmarshal(sp.GetDataset().HeadValue(), &result)
	s.NoError(err)

	expectedGroups := map[string]map[string]bool{
		"44": map[string]bool{"45": true, "43": true, "42": true},
		"46": map[string]bool{"47": true},
		"48": nil,
		"49": nil,
		"50": nil,
		"51": nil,
		"52": nil,
	}

	for _, g := range result.Groups {
		exp, ok := expectedGroups[g.Cover.Id]
		s.True(ok, "Group cover %s not expected", g.Cover.Id)
		for _, p := range g.Photos {
			if _, ok = exp[p.Id]; ok {
				delete(exp, p.Id)
			} else {
				s.Fail("Photo %s not expected in group %s", p.Id, g.Cover.Id)
			}
		}
		s.Equal(0, len(exp), "Some expected photos not found in group %s: %+v", g.Cover.Id, exp)
		delete(expectedGroups, g.Cover.Id)
	}

	s.Equal(0, len(expectedGroups), "Some expected groups not found in result: %+v", expectedGroups)
}
