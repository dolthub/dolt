package gateway

import (
	"net"
	"testing"
)

type testcase struct {
	output  []byte
	ok      bool
	gateway string
}

func TestParseWindowsRoutePrint(t *testing.T) {
	correctData := []byte(`
IPv4 Route Table
===========================================================================
Active Routes:
Network Destination        Netmask          Gateway       Interface  Metric
          0.0.0.0          0.0.0.0       10.88.88.2     10.88.88.149     10
===========================================================================
Persistent Routes:
`)
	randomData := []byte(`
Lorem ipsum dolor sit amet, consectetur adipiscing elit,
sed do eiusmod tempor incididunt ut labore et dolore magna
aliqua. Ut enim ad minim veniam, quis nostrud exercitation
`)
	noRoute := []byte(`
IPv4 Route Table
===========================================================================
Active Routes:
`)
	badRoute1 := []byte(`
IPv4 Route Table
===========================================================================
Active Routes:
===========================================================================
Persistent Routes:
`)
	badRoute2 := []byte(`
IPv4 Route Table
===========================================================================
Active Routes:
Network Destination        Netmask          Gateway       Interface  Metric
          0.0.0.0          0.0.0.0          foo           10.88.88.149     10
===========================================================================
Persistent Routes:
`)

	testcases := []testcase{
		{correctData, true, "10.88.88.2"},
		{randomData, false, ""},
		{noRoute, false, ""},
		{badRoute1, false, ""},
		{badRoute2, false, ""},
	}

	test(t, testcases, parseWindowsRoutePrint)
}

func TestParseLinuxIPRoutePrint(t *testing.T) {
	correctData := []byte(`
default via 192.168.178.1 dev wlp3s0  metric 303
192.168.178.0/24 dev wlp3s0  proto kernel  scope link  src 192.168.178.76  metric 303
`)
	randomData := []byte(`
test
Lorem ipsum dolor sit amet, consectetur adipiscing elit,
sed do eiusmod tempor incididunt ut labore et dolore magna
aliqua. Ut enim ad minim veniam, quis nostrud exercitation
`)
	noRoute := []byte(`
192.168.178.0/24 dev wlp3s0  proto kernel  scope link  src 192.168.178.76  metric 303
`)
	badRoute := []byte(`
default via foo dev wlp3s0  metric 303
192.168.178.0/24 dev wlp3s0  proto kernel  scope link  src 192.168.178.76  metric 303
`)

	testcases := []testcase{
		{correctData, true, "192.168.178.1"},
		{randomData, false, ""},
		{noRoute, false, ""},
		{badRoute, false, ""},
	}

	test(t, testcases, parseLinuxIPRoute)
}

func TestParseLinuxRoutePrint(t *testing.T) {
	correctData := []byte(`
Kernel IP routing table
Destination     Gateway         Genmask         Flags Metric Ref    Use Iface
0.0.0.0         192.168.1.1     0.0.0.0         UG    0      0        0 eth0
`)
	randomData := []byte(`
test
Lorem ipsum dolor sit amet, consectetur adipiscing elit,
sed do eiusmod tempor incididunt ut labore et dolore magna
aliqua. Ut enim ad minim veniam, quis nostrud exercitation
`)
	noRoute := []byte(`
Kernel IP routing table
Destination     Gateway         Genmask         Flags Metric Ref    Use Iface
`)
	badRoute := []byte(`
Kernel IP routing table
Destination     Gateway         Genmask         Flags Metric Ref    Use Iface
0.0.0.0         foo     0.0.0.0         UG    0      0        0 eth0
`)

	testcases := []testcase{
		{correctData, true, "192.168.1.1"},
		{randomData, false, ""},
		{noRoute, false, ""},
		{badRoute, false, ""},
	}

	test(t, testcases, parseLinuxRoute)
}

func TestParseDarwinRouteGet(t *testing.T) {
	correctData := []byte(`
   route to: 0.0.0.0
destination: default
       mask: default
    gateway: 172.16.32.1
  interface: en0
      flags: <UP,GATEWAY,DONE,STATIC,PRCLONING>
 recvpipe  sendpipe  ssthresh  rtt,msec    rttvar  hopcount      mtu     expire
       0         0         0         0         0         0      1500         0
`)
	randomData := []byte(`
test
Lorem ipsum dolor sit amet, consectetur adipiscing elit,
sed do eiusmod tempor incididunt ut labore et dolore magna
aliqua. Ut enim ad minim veniam, quis nostrud exercitation
`)
	noRoute := []byte(`
   route to: 0.0.0.0
destination: default
       mask: default
`)
	badRoute := []byte(`
   route to: 0.0.0.0
destination: default
       mask: default
    gateway: foo
  interface: en0
      flags: <UP,GATEWAY,DONE,STATIC,PRCLONING>
 recvpipe  sendpipe  ssthresh  rtt,msec    rttvar  hopcount      mtu     expire
       0         0         0         0         0         0      1500         0
`)

	testcases := []testcase{
		{correctData, true, "172.16.32.1"},
		{randomData, false, ""},
		{noRoute, false, ""},
		{badRoute, false, ""},
	}

	test(t, testcases, parseDarwinRouteGet)
}

func TestParseBSDSolarisNetstat(t *testing.T) {
	correctDataFreeBSD := []byte(`
Routing tables

Internet:
Destination        Gateway            Flags      Netif Expire
default            10.88.88.2         UGS         em0
10.88.88.0/24      link#1             U           em0
10.88.88.148       link#1             UHS         lo0
127.0.0.1          link#2             UH          lo0

Internet6:
Destination                       Gateway                       Flags      Netif Expire
::/96                             ::1                           UGRS        lo0
::1                               link#2                        UH          lo0
::ffff:0.0.0.0/96                 ::1                           UGRS        lo0
fe80::/10                         ::1                           UGRS        lo0
`)
	correctDataSolaris := []byte(`
Routing Table: IPv4
  Destination           Gateway           Flags  Ref     Use     Interface
-------------------- -------------------- ----- ----- ---------- ---------
default              172.16.32.1          UG        2      76419 net0
127.0.0.1            127.0.0.1            UH        2         36 lo0
172.16.32.0          172.16.32.17         U         4       8100 net0

Routing Table: IPv6
  Destination/Mask            Gateway                   Flags Ref   Use    If
--------------------------- --------------------------- ----- --- ------- -----
::1                         ::1                         UH      3   75382 lo0
2001:470:deeb:32::/64       2001:470:deeb:32::17        U       3    2744 net0
fe80::/10                   fe80::6082:52ff:fedc:7df0   U       3    8430 net0
`)
	randomData := []byte(`
Lorem ipsum dolor sit amet, consectetur adipiscing elit,
sed do eiusmod tempor incididunt ut labore et dolore magna
aliqua. Ut enim ad minim veniam, quis nostrud exercitation
`)
	noRoute := []byte(`
Internet:
Destination        Gateway            Flags      Netif Expire
10.88.88.0/24      link#1             U           em0
10.88.88.148       link#1             UHS         lo0
127.0.0.1          link#2             UH          lo0
`)
	badRoute := []byte(`
Internet:
Destination        Gateway            Flags      Netif Expire
default            foo                UGS         em0
10.88.88.0/24      link#1             U           em0
10.88.88.148       link#1             UHS         lo0
127.0.0.1          link#2             UH          lo0
`)

	testcases := []testcase{
		{correctDataFreeBSD, true, "10.88.88.2"},
		{correctDataSolaris, true, "172.16.32.1"},
		{randomData, false, ""},
		{noRoute, false, ""},
		{badRoute, false, ""},
	}

	test(t, testcases, parseBSDSolarisNetstat)
}

func test(t *testing.T, testcases []testcase, fn func([]byte) (net.IP, error)) {
	for i, tc := range testcases {
		net, err := fn(tc.output)
		if tc.ok {
			if err != nil {
				t.Errorf("Unexpected error in test #%d: %v", i, err)
			}
			if net.String() != tc.gateway {
				t.Errorf("Unexpected gateway address %v != %s", net, tc.gateway)
			}
		} else if err == nil {
			t.Errorf("Unexpected nil error in test #%d", i)
		}
	}
}
