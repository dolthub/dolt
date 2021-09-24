// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dtestutils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	TestHomeDir = "/user/bheni"
	WorkingDir  = "/user/bheni/datasets/states"
)

func testHomeDirFunc() (string, error) {
	return TestHomeDir, nil
}

func CreateTestEnv() *env.DoltEnv {
	const name = "billy bob"
	const email = "bigbillieb@fake.horse"
	initialDirs := []string{TestHomeDir, WorkingDir}
	fs := filesys.NewInMemFS(initialDirs, nil, WorkingDir)
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB, "test")
	cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	cfg.SetStrings(map[string]string{
		env.UserNameKey:  name,
		env.UserEmailKey: email,
	})
	err := dEnv.InitRepo(context.Background(), types.Format_Default, name, email, "main")

	if err != nil {
		panic("Failed to initialize environment:" + err.Error())
	}

	return dEnv
}

func CreateEnvWithSeedData(t *testing.T) *env.DoltEnv {
	dEnv := CreateTestEnv()
	imt, sch := CreateTestDataTable(true)

	ctx := context.Background()
	vrw := dEnv.DoltDB.ValueReadWriter()
	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(ctx, vrw, sch)

	_, _, err := table.PipeRows(ctx, rd, wr, false)
	require.NoError(t, err)
	err = rd.Close(ctx)
	require.NoError(t, err)
	err = wr.Close(ctx)
	require.NoError(t, err)

	ai := sch.Indexes().AllIndexes()
	sch = wr.GetSchema()
	sch.Indexes().Merge(ai...)

	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	require.NoError(t, err)
	empty, err := types.NewMap(ctx, vrw)
	require.NoError(t, err)
	tbl, err := doltdb.NewTable(ctx, vrw, schVal, wr.GetMap(), empty, nil)
	require.NoError(t, err)
	tbl, err = editor.RebuildAllIndexes(ctx, tbl, editor.TestEditorOptions(vrw))
	require.NoError(t, err)

	sch, err = tbl.GetSchema(ctx)
	require.NoError(t, err)
	rows, err := tbl.GetRowData(ctx)
	require.NoError(t, err)
	indexes, err := tbl.GetIndexData(ctx)
	require.NoError(t, err)
	err = putTableToWorking(ctx, dEnv, sch, rows, indexes, TableName, nil)
	require.NoError(t, err)

	return dEnv
}

func putTableToWorking(ctx context.Context, dEnv *env.DoltEnv, sch schema.Schema, rows types.Map, indexData types.Map, tableName string, autoVal types.Value) error {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return doltdb.ErrNomsIO
	}

	vrw := dEnv.DoltDB.ValueReadWriter()
	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	if err != nil {
		return env.ErrMarshallingSchema
	}

	tbl, err := doltdb.NewTable(ctx, vrw, schVal, rows, indexData, autoVal)
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, tableName, tbl)
	if err != nil {
		return err
	}

	rootHash, err := root.HashOf()
	if err != nil {
		return err
	}

	newRootHash, err := newRoot.HashOf()
	if err != nil {
		return err
	}
	if rootHash == newRootHash {
		return nil
	}

	return dEnv.UpdateWorkingRoot(ctx, newRoot)
}
