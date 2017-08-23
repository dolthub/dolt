package sockaddr

// import (
//   "syscall"
//   "unsafe"
// )

// func sockaddrToAny(sa syscall.Sockaddr) (*syscall.RawSockaddrAny, Socklen, error) {
//   if sa == nil {
//     return nil, 0, syscall.EINVAL
//   }

//   switch sa.(type) {
//   case *syscall.SockaddrInet4:
//   case *syscall.SockaddrInet6:
//   case *syscall.SockaddrUnix:
//   case *syscall.SockaddrDatalink:
//   }
//   return nil, 0, syscall.EAFNOSUPPORT
// }

// func anyToSockaddr(rsa *syscall.RawSockaddrAny) (syscall.Sockaddr, error) {
//   if rsa == nil {
//     return nil, 0, syscall.EINVAL
//   }

//   switch rsa.Addr.Family {
//   case syscall.AF_NETLINK:
//   case syscall.AF_PACKET:
//   case syscall.AF_UNIX:
//   case syscall.AF_INET:
//   case syscall.AF_INET6:
//   }
//   return nil, syscall.EAFNOSUPPORT
// }
