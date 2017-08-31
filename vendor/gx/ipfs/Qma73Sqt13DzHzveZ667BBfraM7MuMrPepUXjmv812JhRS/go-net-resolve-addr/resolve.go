// Copyright 2012 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package resolve

import (
	"errors"
	"net"
)

var (
	ErrDNSNotSupported   = errors.New("dns resolution not supported")
	ErrNoSuitableAddress = errors.New("no suitable address found")
	ErrMissingAddress    = errors.New("missing address")
)

func parseNetwork(netw string) (afnet string, proto int, err error) {
	switch netw {
	case "tcp", "tcp4", "tcp6":
	case "udp", "udp4", "udp6":
	case "ip", "ip4", "ip6":
	case "unix", "unixgram", "unixpacket":
	default:
		return "", 0, net.UnknownNetworkError(netw)
	}
	return netw, 0, nil
}

// ResolveAddr resolves addr that is either an Internet or Unix Addr
func ResolveAddr(op, netw, addr string) (net.Addr, error) {
	afnet, _, err := parseNetwork(netw)
	if err != nil {
		return nil, err
	}
	if op == "dial" && addr == "" {
		return nil, ErrMissingAddress
	}
	switch afnet {
	case "unix", "unixgram", "unixpacket":
		return net.ResolveUnixAddr(afnet, addr)
	}
	return ResolveInternetAddr(afnet, addr)
}

// resolveInternetAddr resolves addr that is either a literal IP
// address or a DNS name and returns an internet protocol family
// address. It returns a list that contains a pair of different
// address family addresses when addr is a DNS name and the name has
// multiple address family records. The result contains at least one
// address when error is nil.
func ResolveInternetAddr(netw, addr string) (net.Addr, error) {
	var (
		err              error
		host, port, zone string
		portnum          int
	)
	switch netw {
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
		if addr != "" {
			if host, port, err = net.SplitHostPort(addr); err != nil {
				return nil, err
			}
			if portnum, err = ParsePort(netw, port); err != nil {
				return nil, err
			}
		}
	case "ip", "ip4", "ip6":
		if addr != "" {
			host = addr
		}
	default:
		return nil, net.UnknownNetworkError(netw)
	}
	inetaddr := func(ip net.IP) net.Addr {
		switch netw {
		case "tcp", "tcp4", "tcp6":
			return &net.TCPAddr{IP: ip, Port: portnum, Zone: zone}
		case "udp", "udp4", "udp6":
			return &net.UDPAddr{IP: ip, Port: portnum, Zone: zone}
		case "ip", "ip4", "ip6":
			return &net.IPAddr{IP: ip, Zone: zone}
		default:
			panic("unexpected network: " + netw)
		}
	}
	if host == "" {
		return inetaddr(nil), nil
	}
	// Try as a literal IP address.
	var ip net.IP
	if ip = ParseIPv4(host); ip != nil {
		return inetaddr(ip), nil
	}
	if ip, zone = ParseIPv6(host, true); ip != nil {
		return inetaddr(ip), nil
	}
	return inetaddr(nil), ErrDNSNotSupported
}

// ParsePort parses port as a network service port number for both
// TCP and UDP.
func ParsePort(netw, port string) (int, error) {
	p, i, ok := DTOI(port, 0)
	if !ok || i != len(port) {
		var err error
		p, err = net.LookupPort(netw, port)
		if err != nil {
			return 0, err
		}
	}
	if p < 0 || p > 0xFFFF {
		return 0, &net.AddrError{"invalid port", port}
	}
	return p, nil
}

// Parse IPv4 address (d.d.d.d).
func ParseIPv4(s string) net.IP {
	var p [net.IPv4len]byte
	i := 0
	for j := 0; j < net.IPv4len; j++ {
		if i >= len(s) {
			// Missing octets.
			return nil
		}
		if j > 0 {
			if s[i] != '.' {
				return nil
			}
			i++
		}
		var (
			n  int
			ok bool
		)
		n, i, ok = DTOI(s, i)
		if !ok || n > 0xFF {
			return nil
		}
		p[j] = byte(n)
	}
	if i != len(s) {
		return nil
	}
	return net.IPv4(p[0], p[1], p[2], p[3])
}

func ParseIPv6(s string, zoneAllowed bool) (ip net.IP, zone string) {
	ip = make(net.IP, net.IPv6len)
	ellipsis := -1 // position of ellipsis in p
	i := 0         // index in string s

	if zoneAllowed {
		s, zone = SplitHostZone(s)
	}

	// Might have leading ellipsis
	if len(s) >= 2 && s[0] == ':' && s[1] == ':' {
		ellipsis = 0
		i = 2
		// Might be only ellipsis
		if i == len(s) {
			return ip, zone
		}
	}

	// Loop, parsing hex numbers followed by colon.
	j := 0
	for j < net.IPv6len {
		// Hex number.
		n, i1, ok := XTOI(s, i)
		if !ok || n > 0xFFFF {
			return nil, zone
		}

		// If followed by dot, might be in trailing IPv4.
		if i1 < len(s) && s[i1] == '.' {
			if ellipsis < 0 && j != net.IPv6len-net.IPv4len {
				// Not the right place.
				return nil, zone
			}
			if j+net.IPv4len > net.IPv6len {
				// Not enough room.
				return nil, zone
			}
			ip4 := ParseIPv4(s[i:])
			if ip4 == nil {
				return nil, zone
			}
			ip[j] = ip4[12]
			ip[j+1] = ip4[13]
			ip[j+2] = ip4[14]
			ip[j+3] = ip4[15]
			i = len(s)
			j += net.IPv4len
			break
		}

		// Save this 16-bit chunk.
		ip[j] = byte(n >> 8)
		ip[j+1] = byte(n)
		j += 2

		// Stop at end of string.
		i = i1
		if i == len(s) {
			break
		}

		// Otherwise must be followed by colon and more.
		if s[i] != ':' || i+1 == len(s) {
			return nil, zone
		}
		i++

		// Look for ellipsis.
		if s[i] == ':' {
			if ellipsis >= 0 { // already have one
				return nil, zone
			}
			ellipsis = j
			if i++; i == len(s) { // can be at end
				break
			}
		}
	}

	// Must have used entire string.
	if i != len(s) {
		return nil, zone
	}

	// If didn't parse enough, expand ellipsis.
	if j < net.IPv6len {
		if ellipsis < 0 {
			return nil, zone
		}
		n := net.IPv6len - j
		for k := j - 1; k >= ellipsis; k-- {
			ip[k+n] = ip[k]
		}
		for k := ellipsis + n - 1; k >= ellipsis; k-- {
			ip[k] = 0
		}
	} else if ellipsis >= 0 {
		// Ellipsis must represent at least one 0 group.
		return nil, zone
	}
	return ip, zone
}

func SplitHostZone(s string) (host, zone string) {
	// The IPv6 scoped addressing zone identifier starts after the
	// last percent sign.
	if i := last(s, '%'); i > 0 {
		host, zone = s[:i], s[i+1:]
	} else {
		host = s
	}
	return
}
