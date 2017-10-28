package mc_json

import (
	"crypto/rand"
	"testing"
	"testing/quick"

	mctest "gx/ipfs/QmU4qokxecGJBZPGmc4D9g2HdTyo8CPqUoZ2gwXKsQxqc9/go-multicodec/test"
)

func pow(a, b int) int {
	for i := 0; i < b; i++ {
		a *= a
	}
	return a
}

func randBuf(t *testing.T, l int) []byte {
	b := make([]byte, l)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRoundtripBasic(t *testing.T) {
	codec := Codec()

	for i := 0; i < 5; i++ {
		l := pow(2, i)
		tca := randBuf(t, l)
		tcb := make([]byte, l)
		mctest.RoundTripTest(t, codec, tca, tcb)
	}
}

func TestRoundtripCheck(t *testing.T) {
	codec := Codec()
	f := func(o1 []byte) bool {
		o2 := make([]byte, len(o1))
		return mctest.RoundTripTest(t, codec, o1, o2)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestHeaderMC(t *testing.T) {
	codec := Multicodec()

	for i := 0; i < 5; i++ {
		l := pow(2, i)
		tc := randBuf(t, l)
		mctest.HeaderTest(t, codec, tc)
	}
}

func TestRoundtripBasicMC(t *testing.T) {
	codec := Multicodec()
	for i := 0; i < 5; i++ {
		l := pow(2, i)
		tca := randBuf(t, l)
		tcb := make([]byte, l)
		mctest.RoundTripTest(t, codec, tca, tcb)
	}

}

func TestRoundtripCheckMC(t *testing.T) {
	codec := Multicodec()
	f := func(o1 []byte) bool {
		o2 := make([]byte, len(o1))
		return mctest.RoundTripTest(t, codec, o1, o2)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}
