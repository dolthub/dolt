package dns

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

func HelloServer(w ResponseWriter, req *Msg) {
	m := new(Msg)
	m.SetReply(req)

	m.Extra = make([]RR, 1)
	m.Extra[0] = &TXT{Hdr: RR_Header{Name: m.Question[0].Name, Rrtype: TypeTXT, Class: ClassINET, Ttl: 0}, Txt: []string{"Hello world"}}
	w.WriteMsg(m)
}

func HelloServerBadId(w ResponseWriter, req *Msg) {
	m := new(Msg)
	m.SetReply(req)
	m.Id++

	m.Extra = make([]RR, 1)
	m.Extra[0] = &TXT{Hdr: RR_Header{Name: m.Question[0].Name, Rrtype: TypeTXT, Class: ClassINET, Ttl: 0}, Txt: []string{"Hello world"}}
	w.WriteMsg(m)
}

func AnotherHelloServer(w ResponseWriter, req *Msg) {
	m := new(Msg)
	m.SetReply(req)

	m.Extra = make([]RR, 1)
	m.Extra[0] = &TXT{Hdr: RR_Header{Name: m.Question[0].Name, Rrtype: TypeTXT, Class: ClassINET, Ttl: 0}, Txt: []string{"Hello example"}}
	w.WriteMsg(m)
}

func RunLocalUDPServer(laddr string) (*Server, string, error) {
	server, l, _, err := RunLocalUDPServerWithFinChan(laddr)

	return server, l, err
}

func RunLocalUDPServerWithFinChan(laddr string) (*Server, string, chan struct{}, error) {
	pc, err := net.ListenPacket("udp", laddr)
	if err != nil {
		return nil, "", nil, err
	}
	server := &Server{PacketConn: pc, ReadTimeout: time.Hour, WriteTimeout: time.Hour}

	waitLock := sync.Mutex{}
	waitLock.Lock()
	server.NotifyStartedFunc = waitLock.Unlock

	fin := make(chan struct{}, 0)

	go func() {
		server.ActivateAndServe()
		close(fin)
		pc.Close()
	}()

	waitLock.Lock()
	return server, pc.LocalAddr().String(), fin, nil
}

func RunLocalUDPServerUnsafe(laddr string) (*Server, string, error) {
	pc, err := net.ListenPacket("udp", laddr)
	if err != nil {
		return nil, "", err
	}
	server := &Server{PacketConn: pc, Unsafe: true,
		ReadTimeout: time.Hour, WriteTimeout: time.Hour}

	waitLock := sync.Mutex{}
	waitLock.Lock()
	server.NotifyStartedFunc = waitLock.Unlock

	go func() {
		server.ActivateAndServe()
		pc.Close()
	}()

	waitLock.Lock()
	return server, pc.LocalAddr().String(), nil
}

func RunLocalTCPServer(laddr string) (*Server, string, error) {
	l, err := net.Listen("tcp", laddr)
	if err != nil {
		return nil, "", err
	}

	server := &Server{Listener: l, ReadTimeout: time.Hour, WriteTimeout: time.Hour}

	waitLock := sync.Mutex{}
	waitLock.Lock()
	server.NotifyStartedFunc = waitLock.Unlock

	go func() {
		server.ActivateAndServe()
		l.Close()
	}()

	waitLock.Lock()
	return server, l.Addr().String(), nil
}

func TestServing(t *testing.T) {
	HandleFunc("miek.nl.", HelloServer)
	HandleFunc("example.com.", AnotherHelloServer)
	defer HandleRemove("miek.nl.")
	defer HandleRemove("example.com.")

	s, addrstr, err := RunLocalUDPServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("unable to run test server: %v", err)
	}
	defer s.Shutdown()

	c := new(Client)
	m := new(Msg)
	m.SetQuestion("miek.nl.", TypeTXT)
	r, _, err := c.Exchange(m, addrstr)
	if err != nil || len(r.Extra) == 0 {
		t.Fatal("failed to exchange miek.nl", err)
	}
	txt := r.Extra[0].(*TXT).Txt[0]
	if txt != "Hello world" {
		t.Error("unexpected result for miek.nl", txt, "!= Hello world")
	}

	m.SetQuestion("example.com.", TypeTXT)
	r, _, err = c.Exchange(m, addrstr)
	if err != nil {
		t.Fatal("failed to exchange example.com", err)
	}
	txt = r.Extra[0].(*TXT).Txt[0]
	if txt != "Hello example" {
		t.Error("unexpected result for example.com", txt, "!= Hello example")
	}

	// Test Mixes cased as noticed by Ask.
	m.SetQuestion("eXaMplE.cOm.", TypeTXT)
	r, _, err = c.Exchange(m, addrstr)
	if err != nil {
		t.Error("failed to exchange eXaMplE.cOm", err)
	}
	txt = r.Extra[0].(*TXT).Txt[0]
	if txt != "Hello example" {
		t.Error("unexpected result for example.com", txt, "!= Hello example")
	}
}

func BenchmarkServe(b *testing.B) {
	b.StopTimer()
	HandleFunc("miek.nl.", HelloServer)
	defer HandleRemove("miek.nl.")
	a := runtime.GOMAXPROCS(4)

	s, addrstr, err := RunLocalUDPServer("127.0.0.1:0")
	if err != nil {
		b.Fatalf("unable to run test server: %v", err)
	}
	defer s.Shutdown()

	c := new(Client)
	m := new(Msg)
	m.SetQuestion("miek.nl", TypeSOA)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.Exchange(m, addrstr)
	}
	runtime.GOMAXPROCS(a)
}

func benchmarkServe6(b *testing.B) {
	b.StopTimer()
	HandleFunc("miek.nl.", HelloServer)
	defer HandleRemove("miek.nl.")
	a := runtime.GOMAXPROCS(4)
	s, addrstr, err := RunLocalUDPServer("[::1]:0")
	if err != nil {
		b.Fatalf("unable to run test server: %v", err)
	}
	defer s.Shutdown()

	c := new(Client)
	m := new(Msg)
	m.SetQuestion("miek.nl", TypeSOA)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.Exchange(m, addrstr)
	}
	runtime.GOMAXPROCS(a)
}

func HelloServerCompress(w ResponseWriter, req *Msg) {
	m := new(Msg)
	m.SetReply(req)
	m.Extra = make([]RR, 1)
	m.Extra[0] = &TXT{Hdr: RR_Header{Name: m.Question[0].Name, Rrtype: TypeTXT, Class: ClassINET, Ttl: 0}, Txt: []string{"Hello world"}}
	m.Compress = true
	w.WriteMsg(m)
}

func BenchmarkServeCompress(b *testing.B) {
	b.StopTimer()
	HandleFunc("miek.nl.", HelloServerCompress)
	defer HandleRemove("miek.nl.")
	a := runtime.GOMAXPROCS(4)
	s, addrstr, err := RunLocalUDPServer("127.0.0.1:0")
	if err != nil {
		b.Fatalf("unable to run test server: %v", err)
	}
	defer s.Shutdown()

	c := new(Client)
	m := new(Msg)
	m.SetQuestion("miek.nl", TypeSOA)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c.Exchange(m, addrstr)
	}
	runtime.GOMAXPROCS(a)
}

func TestDotAsCatchAllWildcard(t *testing.T) {
	mux := NewServeMux()
	mux.Handle(".", HandlerFunc(HelloServer))
	mux.Handle("example.com.", HandlerFunc(AnotherHelloServer))

	handler := mux.match("www.miek.nl.", TypeTXT)
	if handler == nil {
		t.Error("wildcard match failed")
	}

	handler = mux.match("www.example.com.", TypeTXT)
	if handler == nil {
		t.Error("example.com match failed")
	}

	handler = mux.match("a.www.example.com.", TypeTXT)
	if handler == nil {
		t.Error("a.www.example.com match failed")
	}

	handler = mux.match("boe.", TypeTXT)
	if handler == nil {
		t.Error("boe. match failed")
	}
}

func TestCaseFolding(t *testing.T) {
	mux := NewServeMux()
	mux.Handle("_udp.example.com.", HandlerFunc(HelloServer))

	handler := mux.match("_dns._udp.example.com.", TypeSRV)
	if handler == nil {
		t.Error("case sensitive characters folded")
	}

	handler = mux.match("_DNS._UDP.EXAMPLE.COM.", TypeSRV)
	if handler == nil {
		t.Error("case insensitive characters not folded")
	}
}

func TestRootServer(t *testing.T) {
	mux := NewServeMux()
	mux.Handle(".", HandlerFunc(HelloServer))

	handler := mux.match(".", TypeNS)
	if handler == nil {
		t.Error("root match failed")
	}
}

type maxRec struct {
	max int
	sync.RWMutex
}

var M = new(maxRec)

func HelloServerLargeResponse(resp ResponseWriter, req *Msg) {
	m := new(Msg)
	m.SetReply(req)
	m.Authoritative = true
	m1 := 0
	M.RLock()
	m1 = M.max
	M.RUnlock()
	for i := 0; i < m1; i++ {
		aRec := &A{
			Hdr: RR_Header{
				Name:   req.Question[0].Name,
				Rrtype: TypeA,
				Class:  ClassINET,
				Ttl:    0,
			},
			A: net.ParseIP(fmt.Sprintf("127.0.0.%d", i+1)).To4(),
		}
		m.Answer = append(m.Answer, aRec)
	}
	resp.WriteMsg(m)
}

func TestServingLargeResponses(t *testing.T) {
	HandleFunc("example.", HelloServerLargeResponse)
	defer HandleRemove("example.")

	s, addrstr, err := RunLocalUDPServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("unable to run test server: %v", err)
	}
	defer s.Shutdown()

	// Create request
	m := new(Msg)
	m.SetQuestion("web.service.example.", TypeANY)

	c := new(Client)
	c.Net = "udp"
	M.Lock()
	M.max = 2
	M.Unlock()
	_, _, err = c.Exchange(m, addrstr)
	if err != nil {
		t.Errorf("failed to exchange: %v", err)
	}
	// This must fail
	M.Lock()
	M.max = 20
	M.Unlock()
	_, _, err = c.Exchange(m, addrstr)
	if err == nil {
		t.Error("failed to fail exchange, this should generate packet error")
	}
	// But this must work again
	c.UDPSize = 7000
	_, _, err = c.Exchange(m, addrstr)
	if err != nil {
		t.Errorf("failed to exchange: %v", err)
	}
}

func TestServingResponse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	HandleFunc("miek.nl.", HelloServer)
	s, addrstr, err := RunLocalUDPServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("unable to run test server: %v", err)
	}

	c := new(Client)
	m := new(Msg)
	m.SetQuestion("miek.nl.", TypeTXT)
	m.Response = false
	_, _, err = c.Exchange(m, addrstr)
	if err != nil {
		t.Fatal("failed to exchange", err)
	}
	m.Response = true
	_, _, err = c.Exchange(m, addrstr)
	if err == nil {
		t.Fatal("exchanged response message")
	}

	s.Shutdown()
	s, addrstr, err = RunLocalUDPServerUnsafe("127.0.0.1:0")
	if err != nil {
		t.Fatalf("unable to run test server: %v", err)
	}
	defer s.Shutdown()

	m.Response = true
	_, _, err = c.Exchange(m, addrstr)
	if err != nil {
		t.Fatal("could exchanged response message in Unsafe mode")
	}
}

func TestShutdownTCP(t *testing.T) {
	s, _, err := RunLocalTCPServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("unable to run test server: %v", err)
	}
	err = s.Shutdown()
	if err != nil {
		t.Errorf("could not shutdown test TCP server, %v", err)
	}
}

type trigger struct {
	done bool
	sync.RWMutex
}

func (t *trigger) Set() {
	t.Lock()
	defer t.Unlock()
	t.done = true
}
func (t *trigger) Get() bool {
	t.RLock()
	defer t.RUnlock()
	return t.done
}

func TestHandlerCloseTCP(t *testing.T) {

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr := ln.Addr().String()

	server := &Server{Addr: addr, Net: "tcp", Listener: ln}

	hname := "testhandlerclosetcp."
	triggered := &trigger{}
	HandleFunc(hname, func(w ResponseWriter, r *Msg) {
		triggered.Set()
		w.Close()
	})
	defer HandleRemove(hname)

	go func() {
		defer server.Shutdown()
		c := &Client{Net: "tcp"}
		m := new(Msg).SetQuestion(hname, 1)
		tries := 0
	exchange:
		_, _, err := c.Exchange(m, addr)
		if err != nil && err != io.EOF {
			t.Logf("exchange failed: %s\n", err)
			if tries == 3 {
				return
			}
			time.Sleep(time.Second / 10)
			tries += 1
			goto exchange
		}
	}()
	server.ActivateAndServe()
	if !triggered.Get() {
		t.Fatalf("handler never called")
	}
}

func TestShutdownUDP(t *testing.T) {
	s, _, fin, err := RunLocalUDPServerWithFinChan("127.0.0.1:0")
	if err != nil {
		t.Fatalf("unable to run test server: %v", err)
	}
	err = s.Shutdown()
	if err != nil {
		t.Errorf("could not shutdown test UDP server, %v", err)
	}
	select {
	case <-fin:
	case <-time.After(2 * time.Second):
		t.Error("Could not shutdown test UDP server. Gave up waiting")
	}
}

type ExampleFrameLengthWriter struct {
	Writer
}

func (e *ExampleFrameLengthWriter) Write(m []byte) (int, error) {
	fmt.Println("writing raw DNS message of length", len(m))
	return e.Writer.Write(m)
}

func ExampleDecorateWriter() {
	// instrument raw DNS message writing
	wf := DecorateWriter(func(w Writer) Writer {
		return &ExampleFrameLengthWriter{w}
	})

	// simple UDP server
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	server := &Server{
		PacketConn:     pc,
		DecorateWriter: wf,
		ReadTimeout:    time.Hour, WriteTimeout: time.Hour,
	}

	waitLock := sync.Mutex{}
	waitLock.Lock()
	server.NotifyStartedFunc = waitLock.Unlock
	defer server.Shutdown()

	go func() {
		server.ActivateAndServe()
		pc.Close()
	}()

	waitLock.Lock()

	HandleFunc("miek.nl.", HelloServer)

	c := new(Client)
	m := new(Msg)
	m.SetQuestion("miek.nl.", TypeTXT)
	_, _, err = c.Exchange(m, pc.LocalAddr().String())
	if err != nil {
		fmt.Println("failed to exchange", err.Error())
		return
	}
	// Output: writing raw DNS message of length 56
}
