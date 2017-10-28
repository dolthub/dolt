# Echo client/server with libp2p

This is an example that quickly shows how to use the `go-libp2p` stack, including Host/Basichost, Network/Swarm, Streams, Peerstores and Multiaddresses.

This example can be started in either listen mode, or dial mode.

In listen mode, it will sit and wait for incoming connections on the `/echo/1.0.0` protocol. Whenever it receives a stream, it will write the message `"Hello, world!"` over the stream and close it.

In dial mode, the node will start up, connect to the given address, open a stream to the target peer, and read a message on the protocol `/echo/1.0.0`.

## Build

From `go-libp2p` base folder:

```
> make deps
> go build ./examples/echo
```

## Usage

```
> ./echo -secio -l 10000
2017/03/15 14:11:32 I am /ip4/127.0.0.1/tcp/10000/ipfs/QmYo41GybvrXk8y8Xnm1P7pfA4YEXCpfnLyzgRPnNbG35e
2017/03/15 14:11:32 Now run "./echo -l 10001 -d /ip4/127.0.0.1/tcp/10000/ipfs/QmYo41GybvrXk8y8Xnm1P7pfA4YEXCpfnLyzgRPnNbG35e -secio" on a different terminal
2017/03/15 14:11:32 listening for connections
```

The listener libp2p host will print its `Multiaddress`, which indicates how it can be reached (ip4+tcp) and its randomly generated ID (`QmYo41Gyb...`)

Now, launch another node that talks to the listener:

```
> ./echo -secio -l 10001 -d /ip4/127.0.0.1/tcp/10000/ipfs/QmYo41GybvrXk8y8Xnm1P7pfA4YEXCpfnLyzgRPnNbG35e
```

The new node with send the message `"Hello, world!"` to the listener, which will in turn echo it over the stream and close it. The listener logs the message, and the sender logs the response.

## Details

The `makeBasicHost()` function creates a [go-libp2p-basichost](https://godoc.org/github.com/libp2p/go-libp2p/p2p/host/basic) object. `basichost` objects wrap [go-libp2 swarms](https://godoc.org/github.com/libp2p/go-libp2p-swarm#Swarm) and should be used preferentially. A [go-libp2p-swarm Network](https://godoc.org/github.com/libp2p/go-libp2p-swarm#Network) is a `swarm` which complies to the [go-libp2p-net Network interface](https://godoc.org/github.com/libp2p/go-libp2p-net#Network) and takes care of maintaining streams, connections, multiplexing different protocols on them, handling incoming connections etc.

In order to create the swarm (and a `basichost`), the example needs:

- An [ipfs-procotol ID](https://godoc.org/github.com/libp2p/go-libp2p-peer#ID) like `QmNtX1cvrm2K6mQmMEaMxAuB4rTexhd87vpYVot4sEZzxc`. The example autogenerates a key pair on every run and uses an ID extracted from the public key (the hash of the public key). When using `-secio`, it uses the key pair to encrypt communications (otherwise, it leaves the connections unencrypted).
- A [Multiaddress](https://godoc.org/github.com/multiformats/go-multiaddr), which indicates how to reach this peer. There can be several of them (using different protocols or locations for example). Example: `/ip4/127.0.0.1/tcp/1234`.
- A [go-libp2p-peerstore](https://godoc.org/github.com/libp2p/go-libp2p-peerstore), which is used as a address book which matches node IDs to the multiaddresses through which they can be contacted. This peerstore gets autopopulated when manually opening a connection (with [`Connect()`](https://godoc.org/github.com/libp2p/go-libp2p/p2p/host/basic#BasicHost.Connect). Alternatively, we can manually [`AddAddr()`](https://godoc.org/github.com/libp2p/go-libp2p-peerstore#AddrManager.AddAddr) as in the example.

A `basichost` can now open streams (bi-directional channel between to peers) using [NewStream](https://godoc.org/github.com/libp2p/go-libp2p/p2p/host/basic#BasicHost.NewStream) and use them to send and receive data tagged with a `Protocol.ID` (a string). The host can also listen for incoming connections for a given
`Protocol` with [`SetStreamHandle()`](https://godoc.org/github.com/libp2p/go-libp2p/p2p/host/basic#BasicHost.SetStreamHandler).

The example makes use of all of this to enable communication between a listener and a sender using protocol `/echo/1.0.0` (which could be any other thing).
