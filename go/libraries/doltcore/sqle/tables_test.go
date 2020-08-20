package sqle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinRowsPerPartitionInTests(t *testing.T) {
	// If this fails then the method for determining if we are running in a test doesn't work all the time.
	assert.Equal(t, uint64(2), MinRowsPerPartition)
}
