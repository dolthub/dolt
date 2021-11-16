package sqle

import (
	"context"
	"sync"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/buffer"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
)

func TestCommitHooksNoErrors(t *testing.T) {
	dEnv := dtestutils.CreateEnvWithSeedData(t)
	AddDoltSystemVariables()
	sql.SystemVariables.SetGlobal(SkipReplicationErrorsKey, true)
	sql.SystemVariables.SetGlobal(ReplicateToRemoteKey, "unknown")
	var wg *sync.WaitGroup
	hooks, err := GetCommitHooks(context.Background(), wg, dEnv, &buffer.Buffer{})
	assert.NoError(t, err)
	if len(hooks) < 1 {
		t.Error("failed to produce noop hook")
	} else {
		switch h := hooks[0].(type) {
		case *doltdb.LogHook:
		default:
			t.Errorf("expected LogHook, found: %s", h)
		}
	}
}
