package conn

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	msgio "gx/ipfs/QmRQhVisS8dmPbjBUthVkenn81pBxrx1GxE281csJhm2vL/go-msgio"
	travis "gx/ipfs/QmWRCn8vruNAzHx8i6SAXinuheRitKEGu8c7m26stKvsYx/go-testutil/ci/travis"
	iconn "gx/ipfs/QmfQAY7YU4fQi3sjGLs1hwkM2Aq7dxgDyoMjaKN4WBWvcB/go-libp2p-interface-conn"
)

func msgioWrap(c iconn.Conn) msgio.ReadWriter {
	return msgio.NewReadWriter(c)
}

func testOneSendRecv(t *testing.T, c1, c2 iconn.Conn) {
	mc1 := msgioWrap(c1)
	mc2 := msgioWrap(c2)

	log.Debugf("testOneSendRecv from %s to %s", c1.LocalPeer(), c2.LocalPeer())
	m1 := []byte("hello")
	if err := mc1.WriteMsg(m1); err != nil {
		t.Fatal(err)
	}
	m2, err := mc2.ReadMsg()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m1, m2) {
		t.Fatalf("failed to send: %s %s", m1, m2)
	}
}

func testNotOneSendRecv(t *testing.T, c1, c2 iconn.Conn) {
	mc1 := msgioWrap(c1)
	mc2 := msgioWrap(c2)

	m1 := []byte("hello")
	if err := mc1.WriteMsg(m1); err == nil {
		t.Fatal("write should have failed", err)
	}
	_, err := mc2.ReadMsg()
	if err == nil {
		t.Fatal("read should have failed", err)
	}
}

func TestClose(t *testing.T) {
	// t.Skip("Skipping in favor of another test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c1, c2, _, _ := setupSingleConn(t, ctx)

	testOneSendRecv(t, c1, c2)
	testOneSendRecv(t, c2, c1)

	c1.Close()
	testNotOneSendRecv(t, c1, c2)

	c2.Close()
	testNotOneSendRecv(t, c2, c1)
	testNotOneSendRecv(t, c1, c2)
}

func TestCloseLeak(t *testing.T) {
	// t.Skip("Skipping in favor of another test")
	if testing.Short() {
		t.SkipNow()
	}

	if travis.IsRunning() {
		t.Skip("this doesn't work well on travis")
	}

	var wg sync.WaitGroup

	runPair := func(num int) {
		ctx, cancel := context.WithCancel(context.Background())
		c1, c2, _, _ := setupSingleConn(t, ctx)

		mc1 := msgioWrap(c1)
		mc2 := msgioWrap(c2)

		for i := 0; i < num; i++ {
			b1 := []byte(fmt.Sprintf("beep%d", i))
			mc1.WriteMsg(b1)
			b2, err := mc2.ReadMsg()
			if err != nil {
				panic(err)
			}
			if !bytes.Equal(b1, b2) {
				panic(fmt.Errorf("bytes not equal: %s != %s", b1, b2))
			}

			b2 = []byte(fmt.Sprintf("boop%d", i))
			mc2.WriteMsg(b2)
			b1, err = mc1.ReadMsg()
			if err != nil {
				panic(err)
			}
			if !bytes.Equal(b1, b2) {
				panic(fmt.Errorf("bytes not equal: %s != %s", b1, b2))
			}

			<-time.After(time.Microsecond * 5)
		}

		c1.Close()
		c2.Close()
		cancel() // close the listener
		wg.Done()
	}

	var cons = 5
	var msgs = 50
	log.Debugf("Running %d connections * %d msgs.\n", cons, msgs)
	for i := 0; i < cons; i++ {
		wg.Add(1)
		go runPair(msgs)
	}

	log.Debugf("Waiting...\n")
	wg.Wait()
	// done!

	time.Sleep(time.Millisecond * 150)
	ngr := runtime.NumGoroutine()
	if ngr > 25 {
		// note, this is really innacurate
		//panic("uncomment me to debug")
		t.Fatal("leaking goroutines:", ngr)
	}
}
