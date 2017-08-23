package addr

import (
	"sort"
	"testing"
)

func TestAddressSorting(t *testing.T) {
	u1 := newAddrOrFatal(t, "/ip4/152.12.23.53/udp/1234/utp")
	u2l := newAddrOrFatal(t, "/ip4/127.0.0.1/udp/1234/utp")
	local := newAddrOrFatal(t, "/ip4/127.0.0.1/tcp/1234")
	norm := newAddrOrFatal(t, "/ip4/6.5.4.3/tcp/1234")

	l := AddrList{local, u1, u2l, norm}
	sort.Sort(l)

	if !l[0].Equal(u2l) {
		t.Fatal("expected utp local addr to be sorted first: ", l[0])
	}

	if !l[1].Equal(u1) {
		t.Fatal("expected utp addr to be sorted second")
	}

	if !l[2].Equal(local) {
		t.Fatal("expected tcp localhost addr thid")
	}

	if !l[3].Equal(norm) {
		t.Fatal("expected normal addr last")
	}
}
