package bin

import (
	"bytes"
	"testing"

	mc "gx/ipfs/QmU4qokxecGJBZPGmc4D9g2HdTyo8CPqUoZ2gwXKsQxqc9/go-multicodec"
)

func TestBinaryDecoding(t *testing.T) {
	buf := bytes.Buffer{}
	buf.Write(Header)
	data := []byte("Multicodec")
	buf.Write(data)

	dataOut := make([]byte, len(data))
	Multicodec().Decoder(&buf).Decode(dataOut)

	if !bytes.Equal(data, dataOut) {
		t.Fatalf("dataOut(%v) is not eqal to data(%v)", dataOut, data)
	}
}

func TestBinaryEncoding(t *testing.T) {
	buf := bytes.Buffer{}
	data := []byte("Is Awesome")

	Multicodec().Encoder(&buf).Encode(data)

	err := mc.ConsumeHeader(&buf, Header)
	if err != nil {
		t.Fatal(err)
	}

	dataOut := make([]byte, len(data))
	buf.Read(dataOut)

	if !bytes.Equal(data, dataOut) {
		t.Fatalf("dataOut(%v) is not eqal to data(%v)", dataOut, data)
	}
}
