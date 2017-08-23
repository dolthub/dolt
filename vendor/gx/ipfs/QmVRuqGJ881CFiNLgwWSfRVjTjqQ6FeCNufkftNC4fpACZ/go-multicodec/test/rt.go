package mc_test

import (
	"bytes"
	"encoding/json"
	"testing"

	mc "gx/ipfs/QmVRuqGJ881CFiNLgwWSfRVjTjqQ6FeCNufkftNC4fpACZ/go-multicodec"
)

func RoundTripTest(t *testing.T, codec mc.Codec, o1, o2 interface{}) bool {
	m1, err := mc.Marshal(codec, o1)
	if err != nil {
		t.Error(err)
		return false
	}

	if err := mc.Unmarshal(codec, m1, o2); err != nil {
		t.Log(m1)
		t.Error(err)
		return false
	}

	m2, err := mc.Marshal(codec, o2)
	if err != nil {
		t.Error(err)
		return false
	}

	if !bytes.Equal(m1, m2) {
		t.Error("marshalled values not equal")
		t.Log(m1)
		t.Log(m2)
		return false
	}

	return true
}

func HeaderTest(t *testing.T, codec mc.Multicodec, o interface{}) bool {
	h := codec.Header()
	if len(h) < 4 {
		t.Error("header should be >4 bytes (varint, /, _, \\n):", h)
		return false
	}

	if h[1] != byte('/') || h[len(h)-1] != byte('\n') {
		t.Error("malformed header (no / or \\n)")
		return false
	}

	m1, err := mc.Marshal(codec, o)
	if err != nil {
		t.Error(err)
		return false
	}

	if !bytes.HasPrefix(m1, h) {
		t.Error("marshalled data does not have multicodec header")
		return false
	}

	return true
}

func OutputJSON(t *testing.T, thing interface{}) {
	b, err := json.Marshal(thing)
	if err != nil {
		t.Error(err)
		return
	}

	var buf bytes.Buffer
	if err := json.Indent(&buf, b, "", "\t"); err != nil {
		t.Error(err)
		return
	}

	t.Log(buf.String())
}
