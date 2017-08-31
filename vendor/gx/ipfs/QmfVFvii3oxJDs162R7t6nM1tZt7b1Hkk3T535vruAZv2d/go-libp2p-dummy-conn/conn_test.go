package dconn

import (
	"bytes"
	"testing"
)

func TestDuplexWriteDoesntHang(t *testing.T) {
	conn1, conn2, err := NewDummyConnPair()
	if err != nil {
		t.Fatal(err)
	}

	conn1.Write([]byte("hello2"))
	conn2.Write([]byte("hello1"))
	conn1.Close()
	conn2.Close()

	buf := bytes.Buffer{}

	buf.ReadFrom(conn1)
	if buf.String() != "hello1" {
		t.Fatal("expected 'hello1' got '%s'", buf.String())
	}

	buf.Reset()
	buf.ReadFrom(conn2)
	if buf.String() != "hello2" {
		t.Fatal("expected 'hello2' got '%s'", buf.String())
	}

}
