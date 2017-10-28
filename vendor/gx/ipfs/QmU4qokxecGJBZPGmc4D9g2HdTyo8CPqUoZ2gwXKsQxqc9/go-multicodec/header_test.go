package multicodec

import (
	"bytes"
	"testing"
)

var (
	TestPath   = "/testing"
	TestHeader = Header([]byte(TestPath))
)

func TestTestWrapP2H(t *testing.T) {
	b := bytes.Buffer{}
	b.WriteString(TestPath)
	b.WriteByte('\n')
	b.WriteString("data")

	r, err := WrapTransformPathToHeader(&b)

	if err != nil {
		t.Fatal(err)
	}

	err = ConsumeHeader(r, TestHeader)
	if err != nil {
		t.Fatal(err)
	}

	out := make([]byte, 4)
	_, err = r.Read(out)
	if err != nil {
		t.Fatal(err)
	}

	outS := string(out)
	if outS != "data" {
		t.Fatal("data is not equal, got %s", outS)
	}

}
