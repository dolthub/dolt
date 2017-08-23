package msgio

import (
	"bytes"
	randbuf "gx/ipfs/QmYNGtJHgaGZkpzq8yG6Wxqm6EQTKqgpBfnyyGBKbZeDUi/go-randbuf"
	"io"
	"math/rand"
	"testing"
	"time"
)

func TestReadChan(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	rchan := NewChan(10)
	msgs := [1000][]byte{}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range msgs {
		msgs[i] = randbuf.RandBuf(r, r.Intn(1000))
		err := writer.WriteMsg(msgs[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	go rchan.ReadFrom(buf)
	defer rchan.Close()

Loop:
	for i := 0; ; i++ {
		select {
		case err := <-rchan.ErrChan:
			if err != nil {
				t.Fatal("unexpected error", err)
			}

		case msg2, ok := <-rchan.MsgChan:
			if !ok {
				if i < len(msg2) {
					t.Error("failed to read all messages", len(msgs), i)
				}
				break Loop
			}

			msg1 := msgs[i]
			if !bytes.Equal(msg1, msg2) {
				t.Fatal("message retrieved not equal\n", msg1, "\n\n", msg2)
			}
		}
	}
}

func TestWriteChan(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	reader := NewReader(buf)
	wchan := NewChan(10)
	msgs := [1000][]byte{}

	go wchan.WriteTo(buf)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range msgs {
		msgs[i] = randbuf.RandBuf(r, r.Intn(1000))

		select {
		case err := <-wchan.ErrChan:
			if err != nil {
				t.Fatal("unexpected error", err)
			}

		case wchan.MsgChan <- msgs[i]:
		}
	}

	// tell chan we're done.
	close(wchan.MsgChan)
	// wait for writing to end
	<-wchan.CloseChan

	defer wchan.Close()

	for i := 0; ; i++ {
		msg2, err := reader.ReadMsg()
		if err != nil {
			if err == io.EOF {
				if i < len(msg2) {
					t.Error("failed to read all messages", len(msgs), i)
				}
				break
			}
			t.Error("unexpected error", err)
		}

		msg1 := msgs[i]
		if !bytes.Equal(msg1, msg2) {
			t.Fatal("message retrieved not equal\n", msg1, "\n\n", msg2)
		}
	}

	if err := reader.Close(); err != nil {
		t.Error(err)
	}
}
