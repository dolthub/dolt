# go-stream-muxer - generalized stream multiplexing


go-stream-muxer is a common interface for stream muxers, with common tests. It wraps other stream muxers (like [muxado](https://github.com/inconshreveable/muxado), [spdystream](https://github.com/docker/spdystream) and [yamux](https://github.com/hashicorp/yamux)).

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io) [![](https://img.shields.io/badge/freenode-%23ipfs-blue.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23ipfs)

> A test suite and interface you can use to implement a stream muxer.

### Godoc: https://godoc.org/github.com/jbenet/go-stream-muxer

## Implementations

* [yamux](yamux)
* [muxado](muxado)
* [multiplex](multiplex)
* [spdystream](spdystream)

## Badge

Include this badge in your readme if you make a new module that uses abstract-stream-muxer API.

![](img/badge.png)

## Installation

```sh
go get -d github.com/jbenet/go-stream-muxer
cd $GOPATH/src/github.com/jbenet/go-stream-muxer
make deps
```

## Client example

```go
import (
  "net"
  "fmt"
  "io"
  ymux "github.com/jbenet/go-stream-muxer/yamux"
  smux "github.com/jbenet/go-stream-muxer"
)

func dial() {
  nconn, _ := net.Dial("tcp", "localhost:1234")
  sconn, _ := ymux.DefaultTransport.NewConn(nconn, false) // false == client

  go sconn.Serve(func(smux.Stream) {}) // no-op

  s1, _ := sconn.OpenStream()
  s1.Write([]byte("hello"))

  s2, _ := sconn.OpenStream()
  s2.Write([]byte("world"))

  length := 20
  buf2 := make([]byte, length)
  fmt.Printf("reading %d bytes from stream (echoed)\n", length)

  s1.Read(buf2)

  fmt.Printf("received %s as a response\n", string(buf2))

  s3, _ := sconn.OpenStream()
  io.Copy(s3, os.Stdin)
}
```

## Server example

```go
import (
  "net"
  "fmt"
  "io"
  ymux "github.com/jbenet/go-stream-muxer/yamux"
  smux "github.com/jbenet/go-stream-muxer"
)

func listen() {
  tr := ymux.DefaultTransport
  l, _ := net.Listen("tcp", "localhost:1234")

  go func() {
    for {
      c, _ := l.Accept()

      fmt.Println("accepted connection")
      sc, _ := tr.NewConn(c, true)

      go sc.Serve(func(s smux.Stream) {
        fmt.Println("serving connection")
        echoStream(s)
      })
    }
  }()
}

func echoStream(s smux.Stream) {
  defer s.Close()

  fmt.Println("accepted stream")
  io.Copy(s, s) // echo everything
  fmt.Println("closing stream")
}
```
