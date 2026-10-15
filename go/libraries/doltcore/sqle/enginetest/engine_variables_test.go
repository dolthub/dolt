package enginetest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

func TestDoltHarnessEngineVariables(t *testing.T) {
	vars := sqle.NewEngineVariables()
	h := newDoltHarness(t).WithEngineVariables(vars)
	defer h.Close()
	_, err := h.NewEngine(t)
	require.NoError(t, err)
	require.Same(t, vars, h.Engine().Variables())
	require.Same(t, vars, h.NewContextForEngine(h.Engine()).EngineVariables())
	require.Same(t, vars, h.NewContextWithClientForEngine(h.Engine(), h.Session().Client()).EngineVariables())
	require.Same(t, vars, h.NewSessionForEngine(h.Engine()).EngineVariables())
}
