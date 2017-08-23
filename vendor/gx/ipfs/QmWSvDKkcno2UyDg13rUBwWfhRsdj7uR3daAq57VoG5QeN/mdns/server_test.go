package mdns

import (
	"testing"
	"time"
)

func TestServer_StartStop(t *testing.T) {
	s := makeService(t)
	serv, err := NewServer(&Config{Zone: s})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer serv.Shutdown()
}

func TestServer_Lookup(t *testing.T) {
	serv, err := NewServer(&Config{Zone: makeServiceWithServiceName(t, "_foobar._tcp")})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer serv.Shutdown()

	entries := make(chan *ServiceEntry, 1)
	found := false
	go func() {
		select {
		case e := <-entries:
			if e.Name != "hostname._foobar._tcp.local." {
				t.Fatalf("bad: %v", e)
			}
			if e.Port != 80 {
				t.Fatalf("bad: %v", e)
			}
			if e.Info != "Local web server" {
				t.Fatalf("bad: %v", e)
			}
			found = true

		case <-time.After(80 * time.Millisecond):
			t.Fatalf("timeout")
		}
	}()

	params := &QueryParam{
		Service: "_foobar._tcp",
		Domain:  "local",
		Timeout: 50 * time.Millisecond,
		Entries: entries,
	}
	err = Query(params)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !found {
		t.Fatalf("record not found")
	}
}
