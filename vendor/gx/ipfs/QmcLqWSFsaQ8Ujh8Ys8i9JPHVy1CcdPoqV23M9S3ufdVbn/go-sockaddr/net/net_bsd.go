// +build darwin dragonfly freebsd netbsd openbsd

package sockaddrnet

import (
	"syscall"
)

const (
	AF_INET   = syscall.AF_INET
	AF_INET6  = syscall.AF_INET6
	AF_UNIX   = syscall.AF_UNIX
	AF_UNSPEC = syscall.AF_UNSPEC

	IPPROTO_IP   = syscall.IPPROTO_IP
	IPPROTO_IPV4 = syscall.IPPROTO_IPV4
	IPPROTO_IPV6 = syscall.IPPROTO_IPV6
	IPPROTO_TCP  = syscall.IPPROTO_TCP
	IPPROTO_UDP  = syscall.IPPROTO_UDP

	SOCK_DGRAM     = syscall.SOCK_DGRAM
	SOCK_STREAM    = syscall.SOCK_STREAM
	SOCK_SEQPACKET = syscall.SOCK_SEQPACKET
)
