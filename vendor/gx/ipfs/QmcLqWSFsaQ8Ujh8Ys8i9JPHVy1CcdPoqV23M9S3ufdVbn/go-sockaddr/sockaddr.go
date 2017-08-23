package sockaddr

import (
	"syscall"
	"unsafe"
)

import "C"

// Socklen is a type for the length of a sockaddr.
type Socklen uint

// SockaddrToAny converts a syscall.Sockaddr into a syscall.RawSockaddrAny
// The implementation is platform dependent.
func SockaddrToAny(sa syscall.Sockaddr) (*syscall.RawSockaddrAny, Socklen, error) {
	return sockaddrToAny(sa)
}

// SockaddrToAny converts a syscall.RawSockaddrAny into a syscall.Sockaddr
// The implementation is platform dependent.
func AnyToSockaddr(rsa *syscall.RawSockaddrAny) (syscall.Sockaddr, error) {
	return anyToSockaddr(rsa)
}

// AnyToCAny casts a *RawSockaddrAny to a *C.struct_sockaddr_any
func AnyToCAny(a *syscall.RawSockaddrAny) *C.struct_sockaddr_any {
	return (*C.struct_sockaddr_any)(unsafe.Pointer(a))
}

// CAnyToAny casts a *C.struct_sockaddr_any to a *RawSockaddrAny
func CAnyToAny(a *C.struct_sockaddr_any) *syscall.RawSockaddrAny {
	return (*syscall.RawSockaddrAny)(unsafe.Pointer(a))
}
