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
		return (*syscall.RawSockaddrAny)(unsafe.Pointer(&raw)), int32(unsafe.Sizeof(raw)), nil

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
		return (*syscall.RawSockaddrAny)(unsafe.Pointer(&raw)), int32(unsafe.Sizeof(raw)), nil

	case *syscall.SockaddrUnix:
		return nil, 0, syscall.EWINDOWS
	}
	return nil, 0, syscall.EAFNOSUPPORT
}

func anyToSockaddr(rsa *syscall.RawSockaddrAny) (syscall.Sockaddr, error) {
	if rsa == nil {
		return nil, 0, syscall.EINVAL
	}

	switch rsa.Addr.Family {
	case syscall.AF_UNIX:
		return nil, syscall.EWINDOWS

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
