package addr

import (
	"fmt"
	"testing"

	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

func newAddrOrFatal(t *testing.T, s string) ma.Multiaddr {
	a, err := ma.NewMultiaddr(s)
	if err != nil {
		t.Fatal("error parsing multiaddr", err)
	}
	return a
}

func newAddrs(t *testing.T, n int) []ma.Multiaddr {
	addrs := make([]ma.Multiaddr, n)
	for i := 0; i < n; i++ {
		s := fmt.Sprintf("/ip4/1.2.3.4/tcp/%d", i)
		addrs[i] = newAddrOrFatal(t, s)
	}
	return addrs
}

func addrSetsSame(a, b []ma.Multiaddr) bool {
	if len(a) != len(b) {
		return false
	}
	for i, aa := range a {
		bb := b[i]
		if !aa.Equal(bb) {
			return false
		}
	}
	return true
}

func addrSourcesSame(a, b Source) bool {
	return addrSetsSame(a.Addrs(), b.Addrs())
}

func TestAddrCombine(t *testing.T) {
	addrs := newAddrs(t, 30)
	a := Slice(addrs[0:10])
	b := Slice(addrs[10:20])
	c := Slice(addrs[20:30])
	d := CombineSources(a, b, c)
	if !addrSetsSame(addrs, d.Addrs()) {
		t.Error("addrs differ")
	}
	if !addrSourcesSame(Slice(addrs), d) {
		t.Error("addrs differ")
	}
}

func TestAddrUnique(t *testing.T) {

	addrs := newAddrs(t, 40)
	a := Slice(addrs[0:20])
	b := Slice(addrs[10:30])
	c := Slice(addrs[20:40])
	d := CombineSources(a, b, c)
	e := UniqueSource(a, b, c)
	if addrSetsSame(addrs, d.Addrs()) {
		t.Error("addrs same")
	}
	if addrSourcesSame(Slice(addrs), d) {
		t.Error("addrs same")
	}
	if !addrSetsSame(addrs, e.Addrs()) {
		t.Error("addrs differ", addrs, "\n\n", e.Addrs(), "\n\n")
	}
	if !addrSourcesSame(Slice(addrs), e) {
		t.Error("addrs differ", addrs, "\n\n", e.Addrs(), "\n\n")
	}
}
