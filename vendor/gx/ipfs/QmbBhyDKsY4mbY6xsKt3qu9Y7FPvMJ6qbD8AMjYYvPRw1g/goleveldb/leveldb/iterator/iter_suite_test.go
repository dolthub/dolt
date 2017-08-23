package iterator_test

import (
	"testing"

	"gx/ipfs/QmbBhyDKsY4mbY6xsKt3qu9Y7FPvMJ6qbD8AMjYYvPRw1g/goleveldb/leveldb/testutil"
)

func TestIterator(t *testing.T) {
	testutil.RunSuite(t, "Iterator Suite")
}
