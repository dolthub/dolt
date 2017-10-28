package multiplex

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"testing"
	"time"
)

func init() {
	// Let's not slow down the tests too much...
	ReceiveTimeout = 100 * time.Millisecond
}

func TestSlowReader(t *testing.T) {
	a, b := net.Pipe()

	mpa := NewMultiplex(a, false)
	mpb := NewMultiplex(b, true)

	defer mpa.Close()
	defer mpb.Close()

	mes := []byte("Hello world")

	sa, err := mpa.NewStream()
	if err != nil {
		t.Fatal(err)
	}

	sb, err := mpb.Accept()
	if err != nil {
		t.Fatal(err)
	}

	// 100 is large enough that the buffer of the underlying connection will
	// fill up.
	for i := 0; i < 100; i++ {
		_, err = sa.Write(mes)
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("wrote too many messages")
	}

	// There's a race between reading this stream and processing the reset
	// so we have to read enough off to drain the queue.
	for i := 0; i < 8; i++ {
		_, err = sb.Read(mes)
		if err != nil {
			return
		}
	}
	t.Fatal("stream should have been reset")
}

func TestBasicStreams(t *testing.T) {
	a, b := net.Pipe()

	mpa := NewMultiplex(a, false)
	mpb := NewMultiplex(b, true)

	mes := []byte("Hello world")
	go func() {
		s, err := mpb.Accept()
		if err != nil {
			t.Fatal(err)
		}

		_, err = s.Write(mes)
		if err != nil {
			t.Fatal(err)
		}

		err = s.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	s, err := mpa.NewStream()
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(mes))
	n, err := s.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(mes) {
		t.Fatal("read wrong amount")
	}

	if string(buf) != string(mes) {
		t.Fatal("got bad data")
	}

	s.Close()

	mpa.Close()
	mpb.Close()
}

func TestWriteAfterClose(t *testing.T) {
	a, b := net.Pipe()

	mpa := NewMultiplex(a, false)
	mpb := NewMultiplex(b, true)

	done := make(chan struct{})
	mes := []byte("Hello world")
	go func() {
		s, err := mpb.Accept()
		if err != nil {
			t.Fatal(err)
		}

		_, err = s.Write(mes)
		if err != nil {
			return
		}

		_, err = s.Write(mes)
		if err != nil {
			return
		}

		s.Close()

		close(done)
	}()

	s, err := mpa.NewStream()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// wait for writes to complete and close to happen (and be noticed)
	<-done
	time.Sleep(time.Millisecond * 50)

	buf := make([]byte, len(mes)*10)
	n, _ := io.ReadFull(s, buf)
	if n != len(mes)*2 {
		t.Fatal("read incorrect amount of data: ", n)
	}

	// read after close should fail with EOF
	_, err = s.Read(buf)
	if err == nil {
		t.Fatal("read on closed stream should fail")
	}

	mpa.Close()
	mpb.Close()
}

func TestEcho(t *testing.T) {
	a, b := net.Pipe()

	mpa := NewMultiplex(a, false)
	mpb := NewMultiplex(b, true)

	mes := make([]byte, 40960)
	rand.Read(mes)
	go func() {
		s, err := mpb.Accept()
		if err != nil {
			t.Fatal(err)
		}

		defer s.Close()
		io.Copy(s, s)
	}()

	s, err := mpa.NewStream()
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Write(mes)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(mes))
	n, err := io.ReadFull(s, buf)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(mes) {
		t.Fatal("read wrong amount")
	}

	if err := arrComp(buf, mes); err != nil {
		t.Fatal(err)
	}
	s.Close()

	mpa.Close()
	mpb.Close()
}

func TestHalfClose(t *testing.T) {
	a, b := net.Pipe()
	mpa := NewMultiplex(a, false)
	mpb := NewMultiplex(b, true)

	wait := make(chan struct{})
	mes := make([]byte, 40960)
	rand.Read(mes)
	go func() {
		s, err := mpb.Accept()
		if err != nil {
			t.Fatal(err)
		}

		defer s.Close()

		<-wait
		_, err = s.Write(mes)
		if err != nil {
			t.Fatal(err)
		}
	}()

	s, err := mpa.NewStream()
	if err != nil {
		t.Fatal(err)
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}

	bn, err := s.Write([]byte("foo"))
	if err == nil {
		t.Fatal("expected error on write to closed stream")
	}
	if bn != 0 {
		t.Fatal("should not have written any bytes to closed stream")
	}

	close(wait)

	buf, err := ioutil.ReadAll(s)
	if err != nil {
		t.Fatal(err)
	}

	if len(buf) != len(mes) {
		t.Fatal("read wrong amount", len(buf), len(mes))
	}

	if err := arrComp(buf, mes); err != nil {
		t.Fatal(err)
	}

	mpa.Close()
	mpb.Close()
}

func TestFuzzCloseConnection(t *testing.T) {
	a, b := net.Pipe()

	for i := 0; i < 1000; i++ {
		mpa := NewMultiplex(a, false)
		mpb := NewMultiplex(b, true)

		go mpa.Close()
		go mpa.Close()

		mpb.Close()
	}
}

func TestClosing(t *testing.T) {
	a, b := net.Pipe()

	mpa := NewMultiplex(a, false)
	mpb := NewMultiplex(b, true)

	_, err := mpb.NewStream()
	if err != nil {
		t.Fatal(err)
	}

	_, err = mpa.Accept()
	if err != nil {
		t.Fatal(err)
	}

	err = mpa.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = mpb.Close()
	if err != nil {
		// not an error, the other side is closing the pipe/socket
		t.Log(err)
	}
	if len(mpa.channels) > 0 || len(mpb.channels) > 0 {
		t.Fatal("leaked closed streams")
	}
}

func TestReset(t *testing.T) {
	a, b := net.Pipe()

	mpa := NewMultiplex(a, false)
	mpb := NewMultiplex(b, true)

	defer mpa.Close()
	defer mpb.Close()

	sa, err := mpa.NewStream()
	if err != nil {
		t.Fatal(err)
	}
	sb, err := mpb.Accept()

	buf := make([]byte, 10)

	sa.Reset()
	n, err := sa.Read(buf)
	if n != 0 {
		t.Fatalf("read %d bytes on reset stream", n)
	}
	if err == nil {
		t.Fatalf("successfully read from reset stream")
	}

	n, err = sa.Write([]byte("test"))
	if n != 0 {
		t.Fatalf("wrote %d bytes on reset stream", n)
	}
	if err == nil {
		t.Fatalf("successfully wrote to reset stream")
	}

	time.Sleep(10 * time.Millisecond)

	n, err = sb.Write([]byte("test"))
	if n != 0 {
		t.Fatalf("wrote %d bytes on reset stream", n)
	}
	if err == nil {
		t.Fatalf("successfully wrote to reset stream")
	}
	n, err = sb.Read(buf)
	if n != 0 {
		t.Fatalf("read %d bytes on reset stream", n)
	}
	if err == nil {
		t.Fatalf("successfully read from reset stream")
	}
}

func TestOpenAfterClose(t *testing.T) {
	a, b := net.Pipe()

	mpa := NewMultiplex(a, false)
	mpb := NewMultiplex(b, true)

	sa, err := mpa.NewStream()
	if err != nil {
		t.Fatal(err)
	}
	sb, err := mpb.Accept()

	sa.Close()
	sb.Close()

	mpa.Close()

	s, err := mpa.NewStream()
	if err == nil || s != nil {
		t.Fatal("opened a stream on a closed connection")
	}
	mpb.Close()
}

func TestFuzzCloseStream(t *testing.T) {
	timer := time.AfterFunc(10*time.Second, func() {
		// This is really the *only* reliable way to set a timeout on
		// this test...
		// Don't add *anything* to this function. The go scheduler acts
		// a bit funny when it encounters a deadlock...
		panic("timeout")
	})
	defer timer.Stop()

	a, b := net.Pipe()

	mpa := NewMultiplex(a, false)
	mpb := NewMultiplex(b, true)

	defer mpa.Close()
	defer mpb.Close()

	done := make(chan struct{})

	go func() {
		streams := make([]*Stream, 100)
		for i := range streams {
			var err error
			streams[i], err = mpb.NewStream()
			if err != nil {
				t.Fatal(err)
			}

			go streams[i].Close()
			go streams[i].Close()
		}
		// Make sure they're closed before we move on.
		for _, s := range streams {
			if s == nil {
				continue
			}
			s.Close()
		}
		close(done)
	}()

	streams := make([]*Stream, 100)
	for i := range streams {
		var err error
		streams[i], err = mpa.Accept()
		if err != nil {
			t.Fatal(err)
		}
	}

	<-done

	for _, s := range streams {
		if s == nil {
			continue
		}
		err := s.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	if len(mpa.channels) > 0 || len(mpb.channels) > 0 {
		t.Fatal("leaked closed streams")
	}
}

func arrComp(a, b []byte) error {
	msg := ""
	if len(a) != len(b) {
		msg += fmt.Sprintf("arrays differ in length: %d %d\n", len(a), len(b))
	}

	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			msg += fmt.Sprintf("content differs at index %d [%d != %d]", i, a[i], b[i])
			return fmt.Errorf(msg)
		}
	}
	if len(msg) > 0 {
		return fmt.Errorf(msg)
	}
	return nil
}
