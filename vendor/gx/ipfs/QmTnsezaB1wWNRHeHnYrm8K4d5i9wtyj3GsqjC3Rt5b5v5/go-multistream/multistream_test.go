package multistream

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sort"
	"testing"
	"time"
)

func TestProtocolNegotiation(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	done := make(chan struct{})
	go func() {
		selected, _, err := mux.Negotiate(a)
		if err != nil {
			t.Error(err)
		}
		if selected != "/a" {
			t.Error("incorrect protocol selected")
		}
		close(done)
	}()

	err := SelectProtoOrFail("/a", b)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("protocol negotiation didnt complete")
	case <-done:
	}

	verifyPipe(t, a, b)
}

func TestProtocolNegotiationLazy(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	var ac Multistream
	done := make(chan struct{})
	go func() {
		m, selected, _, err := mux.NegotiateLazy(a)
		if err != nil {
			t.Error(err)
		}
		if selected != "/a" {
			t.Error("incorrect protocol selected")
		}
		ac = m
		close(done)
	}()

	sel, err := SelectOneOf([]string{"/foo", "/a"}, b)
	if err != nil {
		t.Fatal(err)
	}

	if sel != "/a" {
		t.Fatal("wrong protocol")
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("protocol negotiation didnt complete")
	case <-done:
	}

	verifyPipe(t, ac, b)
}

func TestNegLazyStressRead(t *testing.T) {
	count := 1000

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	message := []byte("this is the message")
	listener := make(chan io.ReadWriteCloser)
	go func() {
		for rwc := range listener {
			m, selected, _, err := mux.NegotiateLazy(rwc)
			if err != nil {
				t.Error(err)
				return
			}

			if selected != "/a" {
				t.Error("incorrect protocol selected")
				return
			}

			buf := make([]byte, len(message))
			_, err = io.ReadFull(m, buf)
			if err != nil {
				t.Error(err)
				return
			}

			if !bytes.Equal(message, buf) {
				t.Fatal("incorrect output: ", buf)
			}
			rwc.Close()
		}
	}()

	for i := 0; i < count; i++ {
		a, b := net.Pipe()
		listener <- a

		ms := NewMSSelect(b, "/a")

		_, err := ms.Write(message)
		if err != nil {
			t.Fatal(err)
		}

		b.Close()
	}
}

func TestNegLazyStressWrite(t *testing.T) {
	count := 1000

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	message := []byte("this is the message")
	listener := make(chan io.ReadWriteCloser)
	go func() {
		for rwc := range listener {
			m, selected, _, err := mux.NegotiateLazy(rwc)
			if err != nil {
				t.Error(err)
				return
			}

			if selected != "/a" {
				t.Error("incorrect protocol selected")
				return
			}

			_, err = m.Read(nil)
			if err != nil {
				t.Error(err)
				return
			}

			_, err = m.Write(message)
			if err != nil {
				t.Error(err)
				return
			}
		}
	}()

	for i := 0; i < count; i++ {
		a, b := net.Pipe()
		listener <- a

		ms := NewMSSelect(b, "/a")

		buf := make([]byte, len(message))
		_, err := io.ReadFull(ms, buf)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(message, buf) {
			t.Fatal("incorrect output: ", buf)
		}

		a.Close()
		b.Close()
	}
}

func TestInvalidProtocol(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, err := mux.Negotiate(a)
		if err != ErrIncorrectVersion {
			t.Fatal("expected incorrect version error here")
		}
	}()

	ms := NewMultistream(b, "/THIS_IS_WRONG")
	_, err := ms.Write(nil)
	if err == nil {
		t.Fatal("this write should not succeed")
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("protocol negotiation didnt complete")
	case <-done:
	}
}

func TestSelectOne(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	done := make(chan struct{})
	go func() {
		selected, _, err := mux.Negotiate(a)
		if err != nil {
			t.Error(err)
		}
		if selected != "/c" {
			t.Error("incorrect protocol selected")
		}
		close(done)
	}()

	sel, err := SelectOneOf([]string{"/d", "/e", "/c"}, b)
	if err != nil {
		t.Fatal(err)
	}

	if sel != "/c" {
		t.Fatal("selected wrong protocol")
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("protocol negotiation didnt complete")
	case <-done:
	}

	verifyPipe(t, a, b)
}

func TestSelectFails(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	go mux.Negotiate(a)

	_, err := SelectOneOf([]string{"/d", "/e"}, b)
	if err != ErrNotSupported {
		t.Fatal("expected to not be supported")
	}
}

func TestRemoveProtocol(t *testing.T) {
	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	protos := mux.Protocols()
	sort.Strings(protos)
	if protos[0] != "/a" || protos[1] != "/b" || protos[2] != "/c" {
		t.Fatal("didnt get expected protocols")
	}

	mux.RemoveHandler("/b")

	protos = mux.Protocols()
	sort.Strings(protos)
	if protos[0] != "/a" || protos[1] != "/c" {
		t.Fatal("didnt get expected protocols")
	}
}

func TestSelectOneAndWrite(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	done := make(chan struct{})
	go func() {
		selected, _, err := mux.Negotiate(a)
		if err != nil {
			t.Error(err)
		}
		if selected != "/c" {
			t.Error("incorrect protocol selected")
		}
		close(done)
	}()

	sel, err := SelectOneOf([]string{"/d", "/e", "/c"}, b)
	if err != nil {
		t.Fatal(err)
	}

	if sel != "/c" {
		t.Fatal("selected wrong protocol")
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("protocol negotiation didnt complete")
	case <-done:
	}

	verifyPipe(t, a, b)
}

func TestLazyConns(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	la := NewMSSelect(a, "/c")
	lb := NewMSSelect(b, "/c")

	verifyPipe(t, la, lb)
}

func TestLazyAndMux(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	done := make(chan struct{})
	go func() {
		selected, _, err := mux.Negotiate(a)
		if err != nil {
			t.Error(err)
		}
		if selected != "/c" {
			t.Error("incorrect protocol selected")
		}

		msg := make([]byte, 5)
		_, err = a.Read(msg)
		if err != nil {
			t.Error(err)
		}

		close(done)
	}()

	lb := NewMSSelect(b, "/c")

	// do a write to push the handshake through
	_, err := lb.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("failed to complete in time")
	case <-done:
	}

	verifyPipe(t, a, lb)
}

func TestHandleFunc(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", func(p string, rwc io.ReadWriteCloser) error {
		if p != "/c" {
			t.Error("failed to get expected protocol!")
		}
		return nil
	})

	go func() {
		err := SelectProtoOrFail("/c", a)
		if err != nil {
			t.Error(err)
		}
	}()

	err := mux.Handle(b)
	if err != nil {
		t.Fatal(err)
	}

	verifyPipe(t, a, b)
}

func TestAddHandlerOverride(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	mux.AddHandler("/foo", func(p string, rwc io.ReadWriteCloser) error {
		t.Error("shouldnt execute this handler")
		return nil
	})

	mux.AddHandler("/foo", func(p string, rwc io.ReadWriteCloser) error {
		return nil
	})

	go func() {
		err := SelectProtoOrFail("/foo", a)
		if err != nil {
			t.Error(err)
		}
	}()

	err := mux.Handle(b)
	if err != nil {
		t.Fatal(err)
	}

	verifyPipe(t, a, b)
}

func TestLazyAndMuxWrite(t *testing.T) {
	a, b := net.Pipe()

	mux := NewMultistreamMuxer()
	mux.AddHandler("/a", nil)
	mux.AddHandler("/b", nil)
	mux.AddHandler("/c", nil)

	done := make(chan struct{})
	go func() {
		selected, _, err := mux.Negotiate(a)
		if err != nil {
			t.Error(err)
		}
		if selected != "/c" {
			t.Error("incorrect protocol selected")
		}

		_, err = a.Write([]byte("hello"))
		if err != nil {
			t.Error(err)
		}

		close(done)
	}()

	lb := NewMSSelect(b, "/c")

	// do a write to push the handshake through
	msg := make([]byte, 5)
	_, err := lb.Read(msg)
	if err != nil {
		t.Fatal(err)
	}

	if string(msg) != "hello" {
		t.Fatal("wrong!")
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("failed to complete in time")
	case <-done:
	}

	verifyPipe(t, a, lb)
}

func verifyPipe(t *testing.T, a, b io.ReadWriter) {
	mes := make([]byte, 1024)
	rand.Read(mes)
	go func() {
		b.Write(mes)
		a.Write(mes)
	}()

	buf := make([]byte, len(mes))
	n, err := a.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatal("failed to read enough")
	}

	if string(buf) != string(mes) {
		t.Fatal("somehow read wrong message")
	}

	n, err = b.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatal("failed to read enough")
	}

	if string(buf) != string(mes) {
		t.Fatal("somehow read wrong message")
	}
}

func TestTooLargeMessage(t *testing.T) {
	buf := new(bytes.Buffer)
	mes := make([]byte, 100*1024)

	err := delimWrite(buf, mes)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ReadNextToken(buf)
	if err == nil {
		t.Fatal("should have failed to read message larger than 64k")
	}
}

func TestLs(t *testing.T) {
	// TODO: in go1.7, use subtests (t.Run(....) )
	subtestLs(nil)(t)
	subtestLs([]string{"a"})(t)
	subtestLs([]string{"a", "b", "c", "d", "e"})(t)
	subtestLs([]string{"", "a"})(t)
}

func subtestLs(protos []string) func(*testing.T) {
	return func(t *testing.T) {
		mr := NewMultistreamMuxer()
		mset := make(map[string]bool)
		for _, p := range protos {
			mr.AddHandler(p, nil)
			mset[p] = true
		}

		buf := new(bytes.Buffer)
		err := mr.Ls(buf)
		if err != nil {
			t.Fatal(err)
		}

		n, err := binary.ReadUvarint(buf)
		if err != nil {
			t.Fatal(err)
		}

		if int(n) != buf.Len() {
			t.Fatal("length wasnt properly prefixed")
		}

		items, err := Ls(buf)
		if err != nil {
			t.Fatal(err)
		}

		if len(items) != len(protos) {
			t.Fatal("got wrong number of protocols")
		}

		for _, tok := range items {
			if !mset[tok] {
				t.Fatalf("wasnt expecting protocol %s", tok)
			}
		}
	}
}

type readonlyBuffer struct {
	buf io.Reader
}

func (rob *readonlyBuffer) Read(b []byte) (int, error) {
	return rob.buf.Read(b)
}

func (rob *readonlyBuffer) Write(b []byte) (int, error) {
	return 0, fmt.Errorf("cannot write on this pipe")
}

func (rob *readonlyBuffer) Close() error {
	return nil
}

func TestNegotiateFail(t *testing.T) {
	buf := new(bytes.Buffer)

	err := delimWrite(buf, []byte(ProtocolID))
	if err != nil {
		t.Fatal(err)
	}

	err = delimWrite(buf, []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}

	mux := NewMultistreamMuxer()
	mux.AddHandler("foo", nil)

	rob := &readonlyBuffer{bytes.NewReader(buf.Bytes())}
	_, _, err = mux.Negotiate(rob)
	if err == nil {
		t.Fatal("normal negotiate should fail here")
	}

	rob = &readonlyBuffer{bytes.NewReader(buf.Bytes())}
	_, out, _, err := mux.NegotiateLazy(rob)
	if err != nil {
		t.Fatal("expected lazy negoatiate to succeed")
	}

	if out != "foo" {
		t.Fatal("got wrong protocol")
	}
}
