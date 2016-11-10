// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	v7datas "github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	v7spec "github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	v7types "github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestNomsMigrate(t *testing.T) {
	suite.Run(t, &nomsMigrateTestSuite{})
}

type nomsMigrateTestSuite struct {
	clienttest.ClientTestSuite
}

func (s *nomsMigrateTestSuite) writeTestData(str string, value v7types.Value, meta v7types.Value) {
	sp, err := v7spec.ForDataset(str)
	s.NoError(err)
	defer sp.Close()

	_, err = sp.GetDatabase().Commit(sp.GetDataset(), value, v7datas.CommitOptions{
		Meta: v7types.NewStruct("", v7types.StructData{
			"value": meta,
		}),
	})
	s.NoError(err)
}

func (s *nomsMigrateTestSuite) TestNomsMigrate() {
	sourceDsName := "migrateSourceTest"
	sourceStr := v7spec.CreateValueSpecString("ldb", s.LdbDir, sourceDsName)

	destDsName := "migrateDestTest"
	destStr := spec.CreateValueSpecString("ldb", s.LdbDir, destDsName)

	str := "Hello world"
	v7val := v7types.String(str)
	v7meta := v7types.Number(42)

	s.writeTestData(sourceStr, v7val, v7meta)

	outStr, errStr := s.MustRun(main, []string{"migrate", sourceStr, destStr})
	s.Equal("", outStr)
	s.Equal("", errStr)

	sp, err := spec.ForDataset(destStr)
	s.NoError(err)
	defer sp.Close()

	destDs := sp.GetDataset()
	s.True(destDs.HeadValue().Equals(types.String(str)))
	s.True(destDs.Head().Get("meta").(types.Struct).Get("value").Equals(types.Number(42)))
}

func (s *nomsMigrateTestSuite) TestNomsMigrateNonCommit() {
	sourceDsName := "migrateSourceTest2"
	sourceStr := v7spec.CreateValueSpecString("ldb", s.LdbDir, sourceDsName)

	destDsName := "migrateDestTest2"
	destStr := spec.CreateValueSpecString("ldb", s.LdbDir, destDsName)

	str := "Hello world"
	v7val := v7types.NewStruct("", v7types.StructData{
		"str": v7types.String(str),
	})
	v7meta := v7types.Bool(true)

	s.writeTestData(sourceStr, v7val, v7meta)

	outStr, errStr := s.MustRun(main, []string{"migrate", sourceStr + ".value.str", destStr})
	s.Equal("", outStr)
	s.Equal("", errStr)

	sp, err := spec.ForDataset(destStr)
	s.NoError(err)
	defer sp.Close()

	s.True(sp.GetDataset().HeadValue().Equals(types.String(str)))

}

func (s *nomsMigrateTestSuite) TestNomsMigrateNil() {
	sourceDsName := "migrateSourceTest3"
	sourceStr := v7spec.CreateValueSpecString("ldb", s.LdbDir, sourceDsName)

	destDsName := "migrateDestTest3"
	destStr := spec.CreateValueSpecString("ldb", s.LdbDir, destDsName)

	defer func() {
		err := recover()
		s.Equal(clienttest.ExitError{Code: 1}, err)
	}()

	s.MustRun(main, []string{"migrate", sourceStr, destStr})
}
