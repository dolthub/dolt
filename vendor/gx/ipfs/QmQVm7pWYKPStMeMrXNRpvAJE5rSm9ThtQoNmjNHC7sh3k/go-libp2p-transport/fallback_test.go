package transport

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	manet "gx/ipfs/QmX3U3YXCQ6UYBxq2LVWF8dARS1hPUTEYLrSx654Qyxyw6/go-multiaddr-net"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

func assertWrite(w io.Writer, data []byte) error {
	n, err := w.Write(data)
	if err != nil {
		return err
	}

	if n != len(data) {
		return fmt.Errorf("didnt write the correct amount of data (exp: %d, got: %d)", len(data), n)
	}

	return nil
}

func assertRead(r io.Reader, exp []byte) error {
	buf := make([]byte, len(exp))
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return err
	}

	if !bytes.Equal(buf, exp) {
		return fmt.Errorf("read wrong data %s vs %s", buf, exp)
	}
	return nil
}

func TestFallbackDialTcp(t *testing.T) {
	laddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}

	list, err := manet.Listen(laddr)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan bool)
	message := []byte("this is only a test")
	go func() {
		defer close(done)
		scon, err := list.Accept()
		if err != nil {
			t.Error(err)
		}

		err = assertWrite(scon, message)
		if err != nil {
			t.Error(err)
		}
	}()

	fbd := new(FallbackDialer)

	if !fbd.Matches(list.Multiaddr()) {
		t.Fatal("fallback dialer should match tcp multiaddr")
	}

	con, err := fbd.Dial(list.Multiaddr())
	if err != nil {
		t.Fatal(err)
	}

	err = assertRead(con, message)
	if err != nil {
		t.Fatal(err)
	}

	<-done
}

func TestCantDialUDP(t *testing.T) {
	fbd := new(FallbackDialer)

	udpa, err := ma.NewMultiaddr("/ip4/1.2.3.4/udp/9876")
	if err != nil {
		t.Fatal(err)
	}

	_, err = fbd.Dial(udpa)
	if err == nil {
		t.Fatal("fallback dialer shouldnt be able to dial udp connections")
	}
}
