package mask

import (
	"net"
	"testing"
)

func TestValidMasks(t *testing.T) {

	cidrOrFatal := func(s string) *net.IPNet {
		_, ipn, err := net.ParseCIDR(s)
		if err != nil {
			t.Fatal(err)
		}
		return ipn
	}

	testCases := map[string]*net.IPNet{
		"/ip4/1.2.3.4/ipcidr/0":      cidrOrFatal("1.2.3.4/0"),
		"/ip4/1.2.3.4/ipcidr/32":     cidrOrFatal("1.2.3.4/32"),
		"/ip4/1.2.3.4/ipcidr/24":     cidrOrFatal("1.2.3.4/24"),
		"/ip4/192.168.0.0/ipcidr/28": cidrOrFatal("192.168.0.0/28"),
		"/ip6/fe80::/ipcidr/0":       cidrOrFatal("fe80::/0"),
		"/ip6/fe80::/ipcidr/64":      cidrOrFatal("fe80::/64"),
		"/ip6/fe80::/ipcidr/128":     cidrOrFatal("fe80::/128"),
	}

	for s, m1 := range testCases {
		m2, err := NewMask(s)
		if err != nil {
			t.Error("should be invalid:", s)
			continue
		}

		if m1.String() != m2.String() {
			t.Error("masks not equal:", m1, m2)
		}
	}

}

func TestInvalidMasks(t *testing.T) {

	testCases := []string{
		"/",
		"/ip4/10.1.2.3",
		"/ip6/::",
		"/ip4/1.2.3.4/cidr/24",
		"/ip6/fe80::/cidr/24",
		"/eth/aa:aa:aa:aa:aa/ipcidr/24",
		"foobar/ip4/1.2.3.4/ipcidr/32",
	}

	for _, s := range testCases {
		_, err := NewMask(s)
		if err != ErrInvalidFormat {
			t.Error("should be invalid:", s)
		}
	}

	testCases2 := []string{
		"/ip4/1.2.3.4/ipcidr/33",
		"/ip4/192.168.0.0/ipcidr/-1",
		"/ip6/fe80::/ipcidr/129",
	}

	for _, s := range testCases2 {
		_, err := NewMask(s)
		if err == nil {
			t.Error("should be invalid:", s)
		}
	}

}

func TestFiltered(t *testing.T) {
	var tests = map[string]map[string]bool{
		"/ip4/10.0.0.0/ipcidr/8": map[string]bool{
			"10.3.3.4":   true,
			"10.3.4.4":   true,
			"10.4.4.4":   true,
			"15.52.34.3": false,
		},
		"/ip4/192.168.0.0/ipcidr/16": map[string]bool{
			"192.168.0.0": true,
			"192.168.1.0": true,
			"192.1.0.0":   false,
			"10.4.4.4":    false,
		},
	}

	for mask, set := range tests {
		m, err := NewMask(mask)
		if err != nil {
			t.Fatal(err)
		}
		for addr, val := range set {
			ip := net.ParseIP(addr)
			if m.Contains(ip) != val {
				t.Fatalf("expected contains(%s, %s) == %s", mask, addr, val)
			}
		}
	}
}

func TestParsing(t *testing.T) {
	var addrs = map[string]string{
		"/ip4/192.168.0.0/ipcidr/16": "192.168.0.0/16",
		"/ip4/192.0.0.0/ipcidr/8":    "192.0.0.0/8",
		"/ip6/2001:db8::/ipcidr/32":  "2001:db8::/32",
	}

	for k, v := range addrs {
		m, err := NewMask(k)
		if err != nil {
			t.Fatal(err)
		}

		if m.String() != v {
			t.Fatalf("mask is wrong: ", m, v)
		}

		orig, err := ConvertIPNet(m)
		if err != nil {
			t.Fatal(err)
		}

		if orig != k {
			t.Fatal("backwards conversion failed: ", orig, k)
		}
	}
}
