package mc_pb

import (
	"reflect"
	"testing"
	"testing/quick"

	pb "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/protobuf/testpb"
	mctest "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec/test"
)

var testCases []interface{}

func init() {
	a := int32(10)
	b := int32(20)
	d := int32(40)

	tc1 := &pb.Foo{}
	tc1.A = &a
	tc1.B = &b
	tc1.C = []int32{30, 31, 32, 33}
	tc1.D = &d

	tc2 := &pb.Foo{}
	tc2.A = &a
	tc2.B = &b
	tc2.C = []int32{30, 31, 32}
	tc2.D = &d

	tc3 := &pb.Bar{}
	tc3.Foos = []*pb.Foo{tc1, tc1, tc2, tc1, tc2}

	tc4 := &pb.Bar{}
	tc4.Foos = []*pb.Foo{tc1, tc2, tc1, tc2, tc2}
	tc4.Strs = []string{"aaa", "bbb", "ccc"}
	tc4.Bufs = [][]byte{[]byte("aaa"), []byte("bbb"), []byte("ccc")}

	testCases = []interface{}{tc3, tc4}
}

func TestRoundtripBasic(t *testing.T) {
	codec := Codec(nil)
	for _, tca := range testCases {
		var tcb pb.Bar
		mctest.RoundTripTest(t, codec, tca, &tcb)
	}
}

func TestRoundtripCheck(t *testing.T) {
	codec := Codec(nil)
	f := func(o1 pb.Bar) bool {
		var o2 pb.Bar
		o1 = fixObj(o1)
		return mctest.RoundTripTest(t, codec, &o1, &o2)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestHeaderMC(t *testing.T) {
	codec := Multicodec(nil)
	for _, tc := range testCases {
		mctest.HeaderTest(t, codec, tc)
	}
}

func TestRoundtripBasicMC(t *testing.T) {
	codec := Multicodec(nil)
	for _, tca := range testCases {
		tcb := reflect.New(reflect.TypeOf(tca).Elem()).Interface()
		mctest.RoundTripTest(t, codec, tca, tcb)
	}
}

func TestRoundtripCheckMC(t *testing.T) {
	codec := Multicodec(nil)
	f := func(o1 pb.Bar) bool {
		var o2 pb.Bar
		o1 = fixObj(o1)
		return mctest.RoundTripTest(t, codec, &o1, &o2)
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func fixObj(o pb.Bar) pb.Bar {
	var goodfoos []*pb.Foo
	for _, f := range o.GetFoos() {
		if f != nil {
			goodfoos = append(goodfoos, f)
		}
	}
	o.Foos = goodfoos
	return o
}
