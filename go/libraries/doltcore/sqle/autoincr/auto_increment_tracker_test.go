package autoincr

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextHasNoRepeats(t *testing.T) {
	allVals := make(map[uint64]int)
	var mu sync.Mutex

	aiTracker := NewAutoIncrementTracker()

	for i := 0; i < 100; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				nxt, err := aiTracker.Next("test", nil, 1)
				require.NoError(t, err)

				val, err := convertIntTypeToUint(nxt)
				require.NoError(t, err)

				mu.Lock()
				allVals[val]++
				mu.Unlock()
			}
		}()
	}

	for _, val := range allVals {
		require.Equal(t, 1, val)
	}
}
