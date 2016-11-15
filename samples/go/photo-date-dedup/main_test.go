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

		// No dupes, so it doen't end up in a group
		marshal.MustMarshal(Photo{
			Id:        "48",
			DateTaken: Date{NsSinceEpoch: float64(61 * 1e9)},
		}),

		// Zero date taken, so it doesn't end up in a group
		marshal.MustMarshal(Photo{
			Id:        "49",
			DateTaken: Date{NsSinceEpoch: float64(0)},
		}),

		// No date taken, so it doens't end up in a group
		types.NewStruct("Photo", types.StructData{
			"Id": types.String("50"),
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

	s.Equal(2, len(result.Groups))

	s.Equal("44", result.Groups[0].Cover.Id)
	s.Equal(3, len(result.Groups[0].Photos))
	s.Equal("45", result.Groups[0].Photos[0].Id)
	s.Equal("43", result.Groups[0].Photos[1].Id)
	s.Equal("42", result.Groups[0].Photos[2].Id)

	s.Equal("46", result.Groups[1].Cover.Id)
	s.Equal(1, len(result.Groups[1].Photos))
	s.Equal("47", result.Groups[1].Photos[0].Id)
}
