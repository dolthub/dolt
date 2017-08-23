package muxcodec

import (
	"math/rand"
	"testing"
	"testing/quick"

	mc "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec"
	mctest "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/test"
)

type TestType map[string]string

func TestRoundtripWrap(t *testing.T) {
	codec := RandMux()
	codec.Wrap = true
	f := func(o1 TestType) bool {
		var o2 TestType
		return mctest.RoundTripTest(t, codec, &o1, &o2)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestRoundtripNoWrap(t *testing.T) {
	codec := RandMux()
	codec.Wrap = false
	f := func(o1 TestType) bool {
		var o2 TestType
		return mctest.RoundTripTest(t, codec, &o1, &o2)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func RandMux() *Multicodec {
	c := StandardMux()
	c.Select = SelectRand()
	return c
}

func SelectRand() SelectCodec {
	// we need this reuse nonsense so that we use the same rand number
	// twice in a row, so we select the same codec twice (RoundTrip)

	var reuse bool
	var last int
	return func(v interface{}, codecs []mc.Multicodec) mc.Multicodec {
		if reuse {
			reuse = false
		} else {
			reuse = true
			last = rand.Intn(len(codecs))
		}
		return codecs[last%len(codecs)]
	}
}
