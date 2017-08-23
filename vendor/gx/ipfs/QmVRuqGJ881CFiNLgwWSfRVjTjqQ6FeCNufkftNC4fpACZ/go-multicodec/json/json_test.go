package mc_json

import (
	"testing"
	"testing/quick"

	mc "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec"
	mctest "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/test"
)

var testCases []interface{}

func init() {
	tc1 := map[string]string{
		"hello": "world",
	}

	tc2 := map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	tc3 := map[string]interface{}{
		"a": 1,
		"b": "hello",
		"c": map[string]interface{}{
			"c/a": 1,
			"c/b": "world",
			"c/c": []int{1, 2, 3, 4},
		},
	}

	testCases = []interface{}{tc1, tc2, tc3}
}

type TestType map[string]map[string]string

func TestRoundtripBasic(t *testing.T) {
	codecs := []mc.Codec{Codec(true), Codec(false)}
	for _, codec := range codecs {
		for _, tca := range testCases {
			var tcb map[string]interface{}
			mctest.RoundTripTest(t, codec, &tca, &tcb)
		}
	}
}

func TestRoundtripCheck(t *testing.T) {
	codecs := []mc.Codec{Codec(true), Codec(false)}
	for _, codec := range codecs {
		f := func(o1 TestType) bool {
			var o2 TestType
			return mctest.RoundTripTest(t, codec, &o1, &o2)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	}
}

func TestHeaderMC(t *testing.T) {
	codecs := []mc.Multicodec{Multicodec(true), Multicodec(false)}
	for _, codec := range codecs {
		for _, tc := range testCases {
			mctest.HeaderTest(t, codec, &tc)
		}
	}
}

func TestRoundtripBasicMC(t *testing.T) {
	codecs := []mc.Multicodec{Multicodec(true), Multicodec(false)}
	for _, codec := range codecs {
		for _, tca := range testCases {
			var tcb map[string]interface{}
			mctest.RoundTripTest(t, codec, &tca, &tcb)
		}
	}
}

func TestRoundtripCheckMC(t *testing.T) {
	codecs := []mc.Multicodec{Multicodec(true), Multicodec(false)}
	for _, codec := range codecs {
		f := func(o1 TestType) bool {
			var o2 TestType
			return mctest.RoundTripTest(t, codec, &o1, &o2)
		}
		if err := quick.Check(f, nil); err != nil {
			t.Error(err)
		}
	}
}
