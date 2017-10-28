# General performance debugging guidelines

This is a document for helping debug go-ipfs. Please add to it if you can!

### Table of Contents
- [Beginning](#beginning)
- [Analysing the stack dump](#analysing-the-stack-dump)
- [Analyzing the CPU Profile](#analyzing-the-cpu-profile)
- [Other](#other)

### Beginning

When you see ipfs doing something (using lots of CPU, memory, or otherwise
being weird), the first thing you want to do is gather all the relevant
profiling information.

- goroutine dump
  - `curl localhost:5001/debug/pprof/goroutine\?debug=2 > ipfs.stacks`
- 30 second cpu profile
  - `curl localhost:5001/debug/pprof/profile > ipfs.cpuprof`
- heap trace dump
  - `curl localhost:5001/debug/pprof/heap > ipfs.heap`
- system information
  - `ipfs diag sys > ipfs.sysinfo`

Bundle all that up and include a copy of the ipfs binary that you are running
(having the exact same binary is important, it contains debug info).

You can investigate yourself if you feel intrepid:

### Analysing the stack dump

The first thing to look for is hung goroutines -- any goroutine thats been stuck
for over a minute will note that in the trace. It looks something like:

```
goroutine 2306090 [semacquire, 458 minutes]:
sync.runtime_Semacquire(0xc8222fd3e4)
  /home/whyrusleeping/go/src/runtime/sema.go:47 +0x26
sync.(*Mutex).Lock(0xc8222fd3e0)
  /home/whyrusleeping/go/src/sync/mutex.go:83 +0x1c4
gx/ipfs/QmedFDs1WHcv3bcknfo64dw4mT1112yptW1H65Y2Wc7KTV/yamux.(*Session).Close(0xc8222fd340, 0x0, 0x0)
  /home/whyrusleeping/gopkg/src/gx/ipfs/QmedFDs1WHcv3bcknfo64dw4mT1112yptW1H65Y2Wc7KTV/yamux/session.go:205 +0x55
gx/ipfs/QmWSJzRkCMJFHYUQZxKwPX8WA7XipaPtfiwMPARP51ymfn/go-stream-muxer/yamux.(*conn).Close(0xc8222fd340, 0x0, 0x0)
  /home/whyrusleeping/gopkg/src/gx/ipfs/QmWSJzRkCMJFHYUQZxKwPX8WA7XipaPtfiwMPARP51ymfn/go-stream-muxer/yamux/yamux.go:39 +0x2d
gx/ipfs/QmZK81vcgMhpb2t7GNbozk7qzt6Rj4zFqitpvsWT9mduW8/go-peerstream.(*Conn).Close(0xc8257a2000, 0x0, 0x0)
  /home/whyrusleeping/gopkg/src/gx/ipfs/QmZK81vcgMhpb2t7GNbozk7qzt6Rj4zFqitpvsWT9mduW8/go-peerstream/conn.go:156 +0x1f2
created by gx/ipfs/QmZK81vcgMhpb2t7GNbozk7qzt6Rj4zFqitpvsWT9mduW8/go-peerstream.(*Conn).GoClose
  /home/whyrusleeping/gopkg/src/gx/ipfs/QmZK81vcgMhpb2t7GNbozk7qzt6Rj4zFqitpvsWT9mduW8/go-peerstream/conn.go:131 +0xab
```

At the top, you can see that this goroutine (number 2306090) has been waiting
to acquire a semaphore for 458 minutes. That seems bad. Looking at the rest of
the trace, we see the exact line it's waiting on is line 47 of runtime/sema.go.
That's not particularly helpful, so we move on. Next, we see that call was made
by line 205 of yamux/session.go in the `Close` method of `yamux.Session`. This
one appears to be the issue.

Given that information, look for another goroutine that might be
holding the semaphore in question in the rest of the stack dump.
(If you need help doing this, ping and we'll stub this out.)

There are a few different reasons that goroutines can be hung:
- `semacquire` means we're waiting to take a lock or semaphore.
- `select` means that the goroutine is hanging in a select statement and none of
  the cases are yielding anything.
- `chan receive` and `chan send` are waiting for a channel to be received from
  or sent on, respectively.
- `IO wait` generally means that we are waiting on a socket to read or write
  data, although it *can* mean we are waiting on a very slow filesystem.

If you see any of those tags _without_ a `,
X minutes` suffix, that generally means there isn't a problem -- you just caught
that goroutine in the middle of a short wait for something. If the wait time is
over a few minutes, that either means that goroutine doesn't do much, or
something is pretty wrong.

### Analyzing the CPU Profile

The go team wrote an [excellent article on profiling go
programs](http://blog.golang.org/profiling-go-programs). If you've already
gathered the above information, you can skip down to where they start talking
about `go tool pprof`. My go-to method of analyzing these is to run the `web`
command, which generates an SVG dotgraph and opens it in your browser. This is
the quickest way to easily point out where the hot spots in the code are.

### Other

If you have any questions, or want us to analyze some weird go-ipfs behaviour,
just let us know, and be sure to include all the profiling information
mentioned at the top.

