package main

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/google/uuid"
	"math/rand"
)

type TupleGen struct {
	keys []uint64
	pos  int
	rng  *rand.Rand
}

func NewTupleGen(count int) *TupleGen {
	rng := rand.New(rand.NewSource(0))
	keySet := make(map[uint64]struct{}, count)
	for len(keySet) < count {
		keySet[rng.Uint64()] = struct{}{}
	}

	keys := make([]uint64, 0, count)
	for k := range keySet {
		keys = append(keys, k)
	}

	return &TupleGen{keys, 0, rng}
}

func (tg *TupleGen) Reset() {
	tg.pos = 0
}

func (tg *TupleGen) NextKVP() (types.Value, types.Value) {
	if tg.pos >= len(tg.keys) {
		return nil, nil
	}

	key := types.Uint(tg.keys[tg.pos])
	val := types.NewTupleOneWriter(
		types.UUID(uuid.New()),
		types.Int(tg.rng.Int63()),
		types.Uint(tg.rng.Uint64()),
		types.Float(tg.rng.Float64()),
		types.String("test string"),
		types.Bool(tg.rng.Int()%2 == 0),
		types.NullValue)

	tg.pos++
	return key, val
}
