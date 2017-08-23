package memdb

import (
	"testing"

	"gx/ipfs/QmbBhyDKsY4mbY6xsKt3qu9Y7FPvMJ6qbD8AMjYYvPRw1g/goleveldb/leveldb/testutil"
)

func TestMemDB(t *testing.T) {
	testutil.RunSuite(t, "MemDB Suite")
}
