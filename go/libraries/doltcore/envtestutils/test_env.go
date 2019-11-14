package envtestutils

import (
	"context"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	HomeDir = "/users/home"
	TblName = "test_table"
)

func CreateTestEnv(ctx context.Context) *env.DoltEnv {
	hdp := func() (s string, e error) {
		return HomeDir, nil
	}

	fs := filesys.NewInMemFS([]string{HomeDir + "/datasets/test"}, map[string][]byte{}, HomeDir+"/datasets/test")
	return env.Load(ctx, hdp, fs, "mem://")
}

func CreateInitializedTestEnv(t *testing.T, ctx context.Context) *env.DoltEnv {
	dEnv := CreateTestEnv(ctx)

	err := dEnv.InitRepo(ctx, types.Format_Default, "Ash Ketchum", "ash@poke.mon")
	require.NoError(t, err)

	return dEnv
}
