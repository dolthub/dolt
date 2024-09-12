package commands

import (
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/gpg"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"testing"
)

const keyId = "573DA8C6366D04E35CDB1A44E09A0B208F666373"

func importKey(t *testing.T, ctx context.Context) {
	err := gpg.ImportKey(ctx, "testdata/signed_commits/private.pgp")
	require.NoError(t, err)

	ok, err := gpg.HasKey(ctx, keyId)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestSignAndVerifyCommit(t *testing.T) {
	ctx := context.Background()
	importKey(t, ctx)

	args := []string{"-S", keyId, "-m", "test commit"}
	apr, err := cli.CreateCommitArgParser().Parse(args)
	require.NoError(t, err)

	global := map[string]string{
		"user.name":  "First Last",
		"user.email": "test@dolthub.com",
	}

	commitOutput := execCommand(t, ctx, "testdata/signed_commits/db", CommitCmd{}, args, apr, map[string]string{}, global)
	require.NoError(t, err)

	log.Println("commit output", commitOutput)
}

func execCommand(t *testing.T, ctx context.Context, wd string, cmd cli.Command, args []string, apr *argparser.ArgParseResults, local, global map[string]string) (output string) {
	err := os.Chdir(wd)
	require.NoError(t, err)

	fs := filesys.LocalFS
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, wd, "test")

	ch := config.NewConfigHierarchy()
	if global != nil {
		ch.AddConfig("global", config.NewMapConfig(global))
	}

	if local != nil {
		ch.AddConfig("local", config.NewMapConfig(local))
	}

	cfg := env.NewTestDoltCliConfigFromHierarchy(ch, fs)

	mr, err := env.MultiEnvForDirectory(ctx, ch, fs, dEnv.Version, dEnv)
	require.NoError(t, err)

	latebind, verr := BuildSqlEngineQueryist(ctx, dEnv.FS, mr, &cli.UserPassword{}, apr)
	require.NoError(t, verr)
	cliCtx, err := cli.NewCliContext(apr, cfg, latebind)
	require.NoError(t, err)

	initialOut := os.Stdout
	initialErr := os.Stderr
	f, err := os.CreateTemp(os.TempDir(), "signed-commit-test-*")
	os.Stdout = f
	os.Stderr = f
	restoreIO := cli.InitIO()

	defer func() {
		err := f.Close()
		require.NoError(t, err)

		restoreIO()

		os.Stdout = initialOut
		os.Stderr = initialErr

		outputBytes, err := os.ReadFile(f.Name())
		require.NoError(t, err)

		output = string(outputBytes)
	}()

	n := cmd.Exec(ctx, "commit", args, dEnv, cliCtx)
	require.Equal(t, 0, n)
	return
}
