package sm_test

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"net"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	smux "gx/ipfs/QmY9JXR3FupnYAYJWK9aMr9bCpqWKcToQ1tz8DVGTrHpHw/go-stream-muxer"
)

var randomness []byte

func init() {
	// read 1MB of randomness
	randomness = make([]byte, 1<<20)
	if _, err := crand.Read(randomness); err != nil {
		panic(err)
	}
}

type Options struct {
	tr        smux.Transport
	connNum   int
	streamNum int
	msgNum    int
	msgMin    int
	msgMax    int
}

func randBuf(size int) []byte {
	n := len(randomness) - size
	if size < 1 {
		panic(fmt.Errorf("requested too large buffer (%d). max is %d", size, len(randomness)))
	}

	start := mrand.Intn(n)
	return randomness[start : start+size]
}

func checkErr(t *testing.T, err error) {
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
}

func log(s string, v ...interface{}) {
	if testing.Verbose() {
		fmt.Fprintf(os.Stderr, "> "+s+"\n", v...)
	}
}

func echoStream(s smux.Stream) {
	defer s.Close()
	log("accepted stream")
	io.Copy(&LogWriter{s}, s) // echo everything
	log("closing stream")
}

type LogWriter struct {
	W io.Writer
}

func (lw *LogWriter) Write(buf []byte) (int, error) {
	if testing.Verbose() {
		log("logwriter: writing %d bytes", len(buf))
	}
	return lw.W.Write(buf)
}

func GoServe(t *testing.T, tr smux.Transport, l net.Listener) (done func()) {
	closed := make(chan struct{}, 1)

	go func() {
		for {
			c1, err := l.Accept()
			if err != nil {
				select {
				case <-closed:
					return // closed naturally.
				default:
					checkErr(t, err)
				}
			}

			log("accepted connection")
			sc1, err := tr.NewConn(c1, true)
			checkErr(t, err)
			go func() {
				for {
					str, err := sc1.AcceptStream()
					if err != nil {
						break
					}
					go echoStream(str)
				}
			}()
		}
	}()

	return func() {
		closed <- struct{}{}
	}
}

func SubtestSimpleWrite(t *testing.T, tr smux.Transport) {
	l, err := net.Listen("tcp", "localhost:0")
	checkErr(t, err)
	log("listening at %s", l.Addr().String())
	done := GoServe(t, tr, l)
	defer done()

	log("dialing to %s", l.Addr().String())
	nc1, err := net.Dial("tcp", l.Addr().String())
	checkErr(t, err)
	defer nc1.Close()

	log("wrapping conn")
	c1, err := tr.NewConn(nc1, false)
	checkErr(t, err)
	defer c1.Close()

	// serve the outgoing conn, because some muxers assume
	// that we _always_ call serve. (this is an error?)
	go c1.AcceptStream()

	log("creating stream")
	s1, err := c1.OpenStream()
	checkErr(t, err)
	defer s1.Close()

	buf1 := randBuf(4096)
	log("writing %d bytes to stream", len(buf1))
	_, err = s1.Write(buf1)
	checkErr(t, err)

	buf2 := make([]byte, len(buf1))
	log("reading %d bytes from stream (echoed)", len(buf2))
	_, err = s1.Read(buf2)
	checkErr(t, err)

	if string(buf2) != string(buf1) {
		t.Error("buf1 and buf2 not equal: %s != %s", string(buf1), string(buf2))
	}
	log("done")
}

func SubtestStress(t *testing.T, opt Options) {
	msgsize := 1 << 11
	errs := make(chan error, 0) // dont block anything.

	rateLimitN := 5000 // max of 5k funcs, because -race has 8k max.
	rateLimitChan := make(chan struct{}, rateLimitN)
	for i := 0; i < rateLimitN; i++ {
		rateLimitChan <- struct{}{}
	}

	rateLimit := func(f func()) {
		<-rateLimitChan
		f()
		rateLimitChan <- struct{}{}
	}

	writeStream := func(s smux.Stream, bufs chan<- []byte) {
		log("writeStream %p, %d msgNum", s, opt.msgNum)

		for i := 0; i < opt.msgNum; i++ {
			buf := randBuf(msgsize)
			bufs <- buf
			log("%p writing %d bytes (message %d/%d #%x)", s, len(buf), i, opt.msgNum, buf[:3])
			if _, err := s.Write(buf); err != nil {
				errs <- fmt.Errorf("s.Write(buf): %s", err)
				continue
			}
		}
	}

	readStream := func(s smux.Stream, bufs <-chan []byte) {
		log("readStream %p, %d msgNum", s, opt.msgNum)

		buf2 := make([]byte, msgsize)
		i := 0
		for buf1 := range bufs {
			i++
			log("%p reading %d bytes (message %d/%d #%x)", s, len(buf1), i-1, opt.msgNum, buf1[:3])

			if _, err := io.ReadFull(s, buf2); err != nil {
				errs <- fmt.Errorf("io.ReadFull(s, buf2): %s", err)
				log("%p failed to read %d bytes (message %d/%d #%x)", s, len(buf1), i-1, opt.msgNum, buf1[:3])
				continue
			}
			if !bytes.Equal(buf1, buf2) {
				errs <- fmt.Errorf("buffers not equal (%x != %x)", buf1[:3], buf2[:3])
			}
		}
	}

	openStreamAndRW := func(c smux.Conn) {
		log("openStreamAndRW %p, %d opt.msgNum", c, opt.msgNum)

		s, err := c.OpenStream()
		if err != nil {
			errs <- fmt.Errorf("Failed to create NewStream: %s", err)
			return
		}

		bufs := make(chan []byte, opt.msgNum)
		go func() {
			writeStream(s, bufs)
			close(bufs)
		}()

		readStream(s, bufs)
		s.Close()
	}

	openConnAndRW := func() {
		log("openConnAndRW")

		l, err := net.Listen("tcp", "localhost:0")
		checkErr(t, err)
		done := GoServe(t, opt.tr, l)
		defer done()

		nla := l.Addr()
		nc, err := net.Dial(nla.Network(), nla.String())
		checkErr(t, err)
		if err != nil {
			t.Fatal(fmt.Errorf("net.Dial(%s, %s): %s", nla.Network(), nla.String(), err))
			return
		}

		c, err := opt.tr.NewConn(nc, false)
		if err != nil {
			t.Fatal(fmt.Errorf("a.AddConn(%s <--> %s): %s", nc.LocalAddr(), nc.RemoteAddr(), err))
			return
		}

		// serve the outgoing conn, because some muxers assume
		// that we _always_ call serve. (this is an error?)
		go func() {
			log("serving connection")
			for {
				str, err := c.AcceptStream()
				if err != nil {
					break
				}
				go echoStream(str)
			}
		}()

		var wg sync.WaitGroup
		for i := 0; i < opt.streamNum; i++ {
			wg.Add(1)
			go rateLimit(func() {
				defer wg.Done()
				openStreamAndRW(c)
			})
		}
		wg.Wait()
		c.Close()
	}

	openConnsAndRW := func() {
		log("openConnsAndRW, %d conns", opt.connNum)

		var wg sync.WaitGroup
		for i := 0; i < opt.connNum; i++ {
			wg.Add(1)
			go rateLimit(func() {
				defer wg.Done()
				openConnAndRW()
			})
		}
		wg.Wait()
	}

	go func() {
		openConnsAndRW()
		close(errs) // done
	}()

	for err := range errs {
		t.Error(err)
	}

}

func tcpPipe(t *testing.T) (net.Conn, net.Conn) {
	list, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}

	con1, err := net.Dial("tcp", list.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	con2, err := list.Accept()
	if err != nil {
		t.Fatal(err)
	}

	return con1, con2
}

func SubtestStreamOpenStress(t *testing.T, tr smux.Transport) {
	a, b := tcpPipe(t)
	defer a.Close()
	defer b.Close()

	count := 10000
	go func() {
		muxa, err := tr.NewConn(a, true)
		if err != nil {
			t.Fatal(err)
		}
		stress := func() {
			for i := 0; i < count; i++ {
				s, err := muxa.OpenStream()
				if err != nil {
					panic(err)
				}
				s.Close()
			}
		}

		go stress()
		go stress()
		go stress()
		go stress()
		go stress()
	}()

	muxb, err := tr.NewConn(b, false)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 50)

	recv := make(chan struct{})
	go func() {
		for {
			str, err := muxb.AcceptStream()
			if err != nil {
				break
			}
			go func() {
				recv <- struct{}{}
				str.Close()
			}()
		}
	}()

	limit := time.After(time.Second * 10)
	for i := 0; i < count*5; i++ {
		select {
		case <-recv:
		case <-limit:
			t.Fatal("timed out receiving streams")
		}
	}
}

func SubtestStreamReset(t *testing.T, tr smux.Transport) {
	a, b := tcpPipe(t)
	defer a.Close()
	defer b.Close()

	done := make(chan struct{}, 2)
	go func() {
		muxa, err := tr.NewConn(a, true)
		if err != nil {
			panic(err)
		}

		s, err := muxa.OpenStream()
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Millisecond * 50)

		_, err = s.Write([]byte("foo"))
		if err == nil {
			t.Error("should have failed to write")
		}

		s.Close()
		done <- struct{}{}
	}()

	muxb, err := tr.NewConn(b, false)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		str, err := muxb.AcceptStream()
		checkErr(t, err)
		str.Reset()
		done <- struct{}{}
	}()

	<-done
	<-done
}

func SubtestStress1Conn1Stream1Msg(t *testing.T, tr smux.Transport) {
	SubtestStress(t, Options{
		tr:        tr,
		connNum:   1,
		streamNum: 1,
		msgNum:    1,
		msgMax:    100,
		msgMin:    100,
	})
}

func SubtestStress1Conn1Stream100Msg(t *testing.T, tr smux.Transport) {
	SubtestStress(t, Options{
		tr:        tr,
		connNum:   1,
		streamNum: 1,
		msgNum:    100,
		msgMax:    100,
		msgMin:    100,
	})
}

func SubtestStress1Conn100Stream100Msg(t *testing.T, tr smux.Transport) {
	SubtestStress(t, Options{
		tr:        tr,
		connNum:   1,
		streamNum: 100,
		msgNum:    100,
		msgMax:    100,
		msgMin:    100,
	})
}

func SubtestStress50Conn10Stream50Msg(t *testing.T, tr smux.Transport) {
	SubtestStress(t, Options{
		tr:        tr,
		connNum:   50,
		streamNum: 10,
		msgNum:    50,
		msgMax:    100,
		msgMin:    100,
	})
}

func SubtestStress1Conn1000Stream10Msg(t *testing.T, tr smux.Transport) {
	SubtestStress(t, Options{
		tr:        tr,
		connNum:   1,
		streamNum: 1000,
		msgNum:    10,
		msgMax:    100,
		msgMin:    100,
	})
}

func SubtestStress1Conn100Stream100Msg10MB(t *testing.T, tr smux.Transport) {
	SubtestStress(t, Options{
		tr:        tr,
		connNum:   1,
		streamNum: 100,
		msgNum:    100,
		msgMax:    10000,
		msgMin:    1000,
	})
}

func SubtestAll(t *testing.T, tr smux.Transport) {

	tests := []TransportTest{
		SubtestSimpleWrite,
		SubtestStress1Conn1Stream1Msg,
		SubtestStress1Conn1Stream100Msg,
		SubtestStress1Conn100Stream100Msg,
		SubtestStress50Conn10Stream50Msg,
		SubtestStress1Conn1000Stream10Msg,
		SubtestStress1Conn100Stream100Msg10MB,
		SubtestStreamOpenStress,
		SubtestStreamReset,
	}

	for _, f := range tests {
		if testing.Verbose() {
			fmt.Fprintf(os.Stderr, "==== RUN %s\n", GetFunctionName(f))
		}
		f(t, tr)
	}
}

type TransportTest func(t *testing.T, tr smux.Transport)

func TestNoOp(t *testing.T) {}

func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
