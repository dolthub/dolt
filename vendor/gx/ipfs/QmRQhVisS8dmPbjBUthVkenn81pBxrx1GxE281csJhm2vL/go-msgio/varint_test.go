package msgio

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestVarintReadWrite(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewVarintWriter(buf)
	reader := NewVarintReader(buf)
	SubtestReadWrite(t, writer, reader)
}

func TestVarintReadWriteMsg(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewVarintWriter(buf)
	reader := NewVarintReader(buf)
	SubtestReadWriteMsg(t, writer, reader)
}

func TestVarintReadWriteMsgSync(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewVarintWriter(buf)
	reader := NewVarintReader(buf)
	SubtestReadWriteMsgSync(t, writer, reader)
}

func TestVarintWrite(t *testing.T) {
	SubtestVarintWrite(t, []byte("hello world"))
	SubtestVarintWrite(t, []byte("hello world hello world hello world"))
	SubtestVarintWrite(t, make([]byte, 1<<20))
	SubtestVarintWrite(t, []byte(""))
}

func SubtestVarintWrite(t *testing.T, msg []byte) {
	buf := bytes.NewBuffer(nil)
	writer := NewVarintWriter(buf)

	if err := writer.WriteMsg(msg); err != nil {
		t.Fatal(err)
	}

	bb := buf.Bytes()

	sbr := simpleByteReader{R: buf}
	length, err := binary.ReadUvarint(&sbr)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("checking varint is %d", len(msg))
	if int(length) != len(msg) {
		t.Fatalf("incorrect varint: %d != %d", length, len(msg))
	}

	lbuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lbuf, length)

	bblen := int(length) + n
	t.Logf("checking wrote (%d + %d) bytes", length, n)
	if len(bb) != bblen {
		t.Fatalf("wrote incorrect number of bytes: %d != %d", len(bb), bblen)
	}
}
