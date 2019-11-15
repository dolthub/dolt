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
	homeDir = "/users/home"
)

// CreateTestEnv creates an unitialized DoltEnv.  This is the state of a dolt repository before 'dolt init' is run
func CreateTestEnv(ctx context.Context) *env.DoltEnv {
	hdp := func() (s string, e error) {
		return homeDir, nil
	}

	fs := filesys.NewInMemFS([]string{homeDir + "/datasets/test"}, map[string][]byte{}, homeDir+"/datasets/test")
	return env.Load(ctx, hdp, fs, "mem://")
}

// CreateInitializedTestEnv creates an initialized DoltEnv.  This is the state of a dolt repository after 'dolt init' is
// run
func CreateInitializedTestEnv(t *testing.T, ctx context.Context) *env.DoltEnv {
	dEnv := CreateTestEnv(ctx)

	err := dEnv.InitRepo(ctx, types.Format_Default, "Ash Ketchum", "ash@poke.mon")
	require.NoError(t, err)

	return dEnv
}
