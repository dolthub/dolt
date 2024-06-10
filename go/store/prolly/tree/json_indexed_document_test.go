package tree

import (
	"context"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
	"testing"
)

// NewIndexedJsonDocumentFromBytes creates an IndexedJsonDocument from a byte sequence, which has already been validated and normalized
func newIndexedJsonDocumentFromValue(t *testing.T, ctx context.Context, ns NodeStore, v interface{}) IndexedJsonDocument {
	doc, _, err := types.JSON.Convert(v)
	require.NoError(t, err)
	root, err := SerializeJsonToAddr(ctx, ns, doc.(sql.JSONWrapper))
	require.NoError(t, err)
	return NewIndexedJsonDocument(ctx, root, ns)
}

func TestIndexedJsonDocument_Insert(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	convertStringToIndexedJsonDocument := func(t *testing.T, s string) interface{} {
		return newIndexedJsonDocumentFromValue(t, ctx, ns, s)
	}

	testCases := json.JsonInsertTestCases(t, convertStringToIndexedJsonDocument)
	json.RunJsonTests(t, testCases)
}
