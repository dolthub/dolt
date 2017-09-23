[Home](../../README.md) » [Use Cases](../../README.md#use-cases) » **Decentralized** »

[About](about.md)&nbsp; | &nbsp;[Quickstart](quickstart.md)&nbsp; | &nbsp;[Architectures](architectures.md)&nbsp; | &nbsp;[P2P Chat Demo](demo-p2p-chat.md)&nbsp; | &nbsp;**IPFS Chat Demo**
<br><br>
# Demo App: IPFS-based Decentralized Chat

This sample app demonstrates backing a P2P noms app by a decentralized blockstore (in this case, IPFS). Data is pulled off the network dynamically as needed - each client doesn't need a complete copy.

# Build and Run

Demo app code is in the
[ipfs-chat](https://github.com/attic-labs/noms/tree/master/samples/go/decent/ipfs-chat/)
directory. To get it up and running take the following steps:

* Use git to clone the noms repository onto your computer:

```shell
go get github.com/attic-labs/noms/samples/go/decent/ipfs-chat
```

* From the noms/samples/go/decent/ipfs-chat directory, build the program with the following command:

```shell
go build
```

* Run the ipfs-chat client with the following command:

```shell
./ipfs-chat client --username <aname1> --node-idx=1 ipfs:/tmp/ipfs1::chat >& /tmp/err1
```

* Run a second ipfs-chat client with the following command:

```shell
./ipfs-chat client --username <aname2> --node-idx=2 ipfs:/tmp/ipfs2::chat >& /tmp/err2
```
  
If desired, ipfs-chat can be run as a daemon which will replicate all
chat content in a local store which will enable clients to go offline
without causing data to become unavailable to other clients:

```shell
./ipfs-chat daemon --node-idx=3 ipfs:/tmp/ipfs3::chat
```

Note: the 'node-idx' argument ensures that each IPFS-based program
uses a distinct set of ports. This is useful when running multiple
IPFS-based programs on the same machine.
