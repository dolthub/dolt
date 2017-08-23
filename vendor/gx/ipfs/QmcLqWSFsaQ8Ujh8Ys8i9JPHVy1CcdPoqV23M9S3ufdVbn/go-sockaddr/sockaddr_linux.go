package sockaddr

import (
	"syscall"
	"unsafe"
)

func sockaddrToAny(sa syscall.Sockaddr) (*syscall.RawSockaddrAny, Socklen, error) {
	if sa == nil {
		return nil, 0, syscall.EINVAL
	}

	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		if sa.Port < 0 || sa.Port > 0xFFFF {
			return nil, 0, syscall.EINVAL
		}
		var raw syscall.RawSockaddrInet4
		raw.Family = syscall.AF_INET
		p := (*[2]byte)(unsafe.Pointer(&raw.Port))
		p[0] = byte(sa.Port >> 8)
		p[1] = byte(sa.Port)
		for i := 0; i < len(sa.Addr); i++ {
			raw.Addr[i] = sa.Addr[i]
		}
		return (*syscall.RawSockaddrAny)(unsafe.Pointer(&raw)), syscall.SizeofSockaddrInet4, nil

	case *syscall.SockaddrInet6:
		if sa.Port < 0 || sa.Port > 0xFFFF {
			return nil, 0, syscall.EINVAL
		}
		var raw syscall.RawSockaddrInet6
		raw.Family = syscall.AF_INET6
		p := (*[2]byte)(unsafe.Pointer(&raw.Port))
		p[0] = byte(sa.Port >> 8)
		p[1] = byte(sa.Port)
		raw.Scope_id = sa.ZoneId
		for i := 0; i < len(sa.Addr); i++ {
			raw.Addr[i] = sa.Addr[i]
		}
		return (*syscall.RawSockaddrAny)(unsafe.Pointer(&raw)), syscall.SizeofSockaddrInet6, nil

	case *syscall.SockaddrUnix:
		name := sa.Name
		n := len(name)
		var raw syscall.RawSockaddrUnix
		if n >= len(raw.Path) {
			return nil, 0, syscall.EINVAL
		}
		raw.Family = syscall.AF_UNIX
		for i := 0; i < n; i++ {
			raw.Path[i] = int8(name[i])
		}
		// length is family (uint16), name, NUL.
		sl := Socklen(2)
		if n > 0 {
			sl += Socklen(n) + 1
		}
		if raw.Path[0] == '@' {
			raw.Path[0] = 0
			// Don't count trailing NUL for abstract address.
			sl--
		}
		return (*syscall.RawSockaddrAny)(unsafe.Pointer(&raw)), sl, nil

	case *syscall.SockaddrLinklayer:
		if sa.Ifindex < 0 || sa.Ifindex > 0x7fffffff {
			return nil, 0, syscall.EINVAL
		}
		var raw syscall.RawSockaddrLinklayer
		raw.Family = syscall.AF_PACKET
		raw.Protocol = sa.Protocol
		raw.Ifindex = int32(sa.Ifindex)
		raw.Hatype = sa.Hatype
		raw.Pkttype = sa.Pkttype
		raw.Halen = sa.Halen
		for i := 0; i < len(sa.Addr); i++ {
			raw.Addr[i] = sa.Addr[i]
		}
		return (*syscall.RawSockaddrAny)(unsafe.Pointer(&raw)), syscall.SizeofSockaddrLinklayer, nil
	}
	return nil, 0, syscall.EAFNOSUPPORT
}

func anyToSockaddr(rsa *syscall.RawSockaddrAny) (syscall.Sockaddr, error) {
	switch rsa.Addr.Family {
	case syscall.AF_NETLINK:
		pp := (*syscall.RawSockaddrNetlink)(unsafe.Pointer(rsa))
		sa := new(syscall.SockaddrNetlink)
		sa.Family = pp.Family
		sa.Pad = pp.Pad
		sa.Pid = pp.Pid
		sa.Groups = pp.Groups
		return sa, nil

	case syscall.AF_PACKET:
		pp := (*syscall.RawSockaddrLinklayer)(unsafe.Pointer(rsa))
		sa := new(syscall.SockaddrLinklayer)
		sa.Protocol = pp.Protocol
		sa.Ifindex = int(pp.Ifindex)
		sa.Hatype = pp.Hatype
		sa.Pkttype = pp.Pkttype
		sa.Halen = pp.Halen
		for i := 0; i < len(sa.Addr); i++ {
			sa.Addr[i] = pp.Addr[i]
		}
		return sa, nil

	case syscall.AF_UNIX:
		pp := (*syscall.RawSockaddrUnix)(unsafe.Pointer(rsa))
		sa := new(syscall.SockaddrUnix)
		if pp.Path[0] == 0 {
			// "Abstract" Unix domain socket.
			// Rewrite leading NUL as @ for textual display.
			// (This is the standard convention.)
			// Not friendly to overwrite in place,
			// but the callers below don't care.
			pp.Path[0] = '@'
		}

		// Assume path ends at NUL.
		// This is not technically the Linux semantics for
		// abstract Unix domain sockets--they are supposed
		// to be uninterpreted fixed-size binary blobs--but
		// everyone uses this convention.
		n := 0
		for n < len(pp.Path) && pp.Path[n] != 0 {
			n++
		}
		bytes := (*[10000]byte)(unsafe.Pointer(&pp.Path[0]))[0:n]
		sa.Name = string(bytes)
		return sa, nil

	case syscall.AF_INET:
		pp := (*syscall.RawSockaddrInet4)(unsafe.Pointer(rsa))
		sa := new(syscall.SockaddrInet4)
		p := (*[2]byte)(unsafe.Pointer(&pp.Port))
		sa.Port = int(p[0])<<8 + int(p[1])
		for i := 0; i < len(sa.Addr); i++ {
			sa.Addr[i] = pp.Addr[i]
		}
		return sa, nil

	case syscall.AF_INET6:
		pp := (*syscall.RawSockaddrInet6)(unsafe.Pointer(rsa))
		sa := new(syscall.SockaddrInet6)
		p := (*[2]byte)(unsafe.Pointer(&pp.Port))
		sa.Port = int(p[0])<<8 + int(p[1])
		sa.ZoneId = pp.Scope_id
		for i := 0; i < len(sa.Addr); i++ {
			sa.Addr[i] = pp.Addr[i]
		}
		return sa, nil
	}
	return nil, syscall.EAFNOSUPPORT
}
