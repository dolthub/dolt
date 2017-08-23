package msgio

import (
	"bytes"
	"fmt"
	randbuf "gx/ipfs/QmYNGtJHgaGZkpzq8yG6Wxqm6EQTKqgpBfnyyGBKbZeDUi/go-randbuf"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestReadWrite(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	reader := NewReader(buf)
	SubtestReadWrite(t, writer, reader)
}

func TestReadWriteMsg(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	reader := NewReader(buf)
	SubtestReadWriteMsg(t, writer, reader)
}

func TestReadWriteMsgSync(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := NewWriter(buf)
	reader := NewReader(buf)
	SubtestReadWriteMsgSync(t, writer, reader)
}

func SubtestReadWrite(t *testing.T, writer WriteCloser, reader ReadCloser) {
	msgs := [1000][]byte{}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range msgs {
		msgs[i] = randbuf.RandBuf(r, r.Intn(1000))
		n, err := writer.Write(msgs[i])
		if err != nil {
			t.Fatal(err)
		}
		if n != len(msgs[i]) {
			t.Fatal("wrong length:", n, len(msgs[i]))
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	for i := 0; ; i++ {
		msg2 := make([]byte, 1000)
		n, err := reader.Read(msg2)
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
		msg2 = msg2[:n]
		if !bytes.Equal(msg1, msg2) {
			t.Fatal("message retrieved not equal\n", msg1, "\n\n", msg2)
		}
	}

	if err := reader.Close(); err != nil {
		t.Error(err)
	}
}

func SubtestReadWriteMsg(t *testing.T, writer WriteCloser, reader ReadCloser) {
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

func SubtestReadWriteMsgSync(t *testing.T, writer WriteCloser, reader ReadCloser) {
	msgs := [1000][]byte{}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range msgs {
		msgs[i] = randbuf.RandBuf(r, r.Intn(1000)+4)
		NBO.PutUint32(msgs[i][:4], uint32(i))
	}

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	errs := make(chan error, 10000)
	for i := range msgs {
		wg1.Add(1)
		go func(i int) {
			defer wg1.Done()

			err := writer.WriteMsg(msgs[i])
			if err != nil {
				errs <- err
			}
		}(i)
	}

	wg1.Wait()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(msgs)+1; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()

			msg2, err := reader.ReadMsg()
			if err != nil {
				if err == io.EOF {
					if i < len(msg2) {
						errs <- fmt.Errorf("failed to read all messages", len(msgs), i)
					}
					return
				}
				errs <- fmt.Errorf("unexpected error", err)
			}

			mi := NBO.Uint32(msg2[:4])
			msg1 := msgs[mi]
			if !bytes.Equal(msg1, msg2) {
				errs <- fmt.Errorf("message retrieved not equal\n", msg1, "\n\n", msg2)
			}
		}(i)
	}

	wg2.Wait()
	close(errs)

	if err := reader.Close(); err != nil {
		t.Error(err)
	}

	for e := range errs {
		t.Error(e)
	}
}

func TestBadSizes(t *testing.T) {
	data := make([]byte, 4)

	// on a 64 bit system, this will fail because its too large
	// on a 32 bit system, this will fail because its too small
	NBO.PutUint32(data, 4000000000)
	buf := bytes.NewReader(data)
	read := NewReader(buf)
	msg, err := read.ReadMsg()
	if err == nil {
		t.Fatal(err)
	}
	_ = msg
}
