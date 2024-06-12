package tree

import (
	"context"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json/jsontests"
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
	convertToIndexedJsonDocument := func(t *testing.T, s interface{}) interface{} {
		return newIndexedJsonDocumentFromValue(t, ctx, ns, s)
	}

	testCases := jsontests.JsonInsertTestCases(t, convertToIndexedJsonDocument)
	jsontests.RunJsonTests(t, testCases)
}

func TestIndexedJsonDocument_Extract(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	convertToIndexedJsonDocument := func(t *testing.T, s interface{}) interface{} {
		return newIndexedJsonDocumentFromValue(t, ctx, ns, s)
	}

	testCases := jsontests.JsonExtractTestCases(t, convertToIndexedJsonDocument)
	jsontests.RunJsonTests(t, testCases)
}

func TestIndexedJsonDocument_Value(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	convertToIndexedJsonDocument := func(t *testing.T, s interface{}) interface{} {
		return newIndexedJsonDocumentFromValue(t, ctx, ns, s)
	}

	jsontests.RunJsonValueTests(t, convertToIndexedJsonDocument)
}

func TestIndexedJsonDocument_ContainsPath(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	convertToIndexedJsonDocument := func(t *testing.T, s interface{}) interface{} {
		return newIndexedJsonDocumentFromValue(t, ctx, ns, s)
	}

	testCases := jsontests.JsonContainsPathTestCases(t, convertToIndexedJsonDocument)
	jsontests.RunJsonTests(t, testCases)
}
