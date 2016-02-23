package chunks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeepStats(t *testing.T) {
	sk := newStatKeeper(1)
	stat := "statName"
	sk.AddStat(stat)
	count := int64(0)

	update := func(c int64) {
		count += c
		sk.Chan(stat) <- c
	}

	update(2)
	update(4)
	update(-3)
	sk.Stop()
	assert.Equal(t, count, sk.Get(stat))
}

func TestKeepStatsHas(t *testing.T) {
	assert := assert.New(t)
	sk := newStatKeeper(1)
	stat := "statName"
	sk.AddStat(stat)
	assert.Panics(func() { sk.Has(stat) })

	sk.Chan(stat) <- 1
	sk.Stop()
	assert.True(sk.Has(stat))
	assert.False(sk.Has("other"))
}
