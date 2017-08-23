## go-sockaddr - `{Raw,}Sockaddr` conversions

See https://groups.google.com/d/msg/golang-nuts/B-meiFfkmH0/-TxP1r6zvk8J
This package extracts unexported code from `syscall` to help in converting
between:

```Go
syscall.Sockaddr
syscall.RawSockaddrAny
C.struct_sockaddr_any
net.*Addr
```

Godoc:

- sockaddr - http://godoc.org/github.com/jbenet/go-sockaddr
- sockaddr/net - http://godoc.org/github.com/jbenet/go-sockaddr/net
