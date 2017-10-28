package websocket

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
	"testing/iotest"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

func TestWebsocketListen(t *testing.T) {
	zero, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0/ws")
	if err != nil {
		t.Fatal(err)
	}

	tpt := &WebsocketTransport{}
	l, err := tpt.Listen(zero)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	msg := []byte("HELLO WORLD")

	go func() {
		d, _ := tpt.Dialer(nil)
		c, err := d.Dial(l.Multiaddr())
		if err != nil {
			t.Error(err)
			return
		}

		c.Write(msg)
		c.Close()
	}()

	c, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	obr := iotest.OneByteReader(c)

	out, err := ioutil.ReadAll(obr)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(out, msg) {
		t.Fatal("got wrong message", out, msg)
	}
}

func TestConcurrentClose(t *testing.T) {
	zero, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0/ws")
	if err != nil {
		t.Fatal(err)
	}

	tpt := &WebsocketTransport{}
	l, err := tpt.Listen(zero)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	msg := []byte("HELLO WORLD")

	go func() {
		d, _ := tpt.Dialer(nil)
		for i := 0; i < 100; i++ {
			c, err := d.Dial(l.Multiaddr())
			if err != nil {
				t.Error(err)
				return
			}

			go c.Write(msg)
			go c.Close()
		}
	}()

	for i := 0; i < 100; i++ {
		c, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
	}
}

func TestWriteZero(t *testing.T) {
	zero, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0/ws")
	if err != nil {
		t.Fatal(err)
	}

	tpt := &WebsocketTransport{}
	l, err := tpt.Listen(zero)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	msg := []byte(nil)

	go func() {
		d, _ := tpt.Dialer(nil)
		c, err := d.Dial(l.Multiaddr())
		defer c.Close()
		if err != nil {
			t.Error(err)
			return
		}

		for i := 0; i < 100; i++ {
			n, err := c.Write(msg)
			if n != 0 {
				t.Errorf("expected to write 0 bytes, wrote %d", n)
			}
			if err != nil {
				t.Error(err)
				return
			}
		}
	}()

	c, err := l.Accept()
	defer c.Close()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 100)
	n, err := c.Read(buf)
	if n != 0 {
		t.Errorf("read %d bytes, expected 0", n)
	}
	if err != io.EOF {
		t.Errorf("expected EOF, got err: %s", err)
	}
}
