# Experimental features of go-ipfs

This document contains a list of experimental features in go-ipfs.
These features, commands, and APIs aren't mature, and you shouldn't rely on them.
Once they reach maturity, there's going to be mention in the changelog and
release posts. If they don't reach maturity, the same applies, and their code is
removed.

Subscribe to https://github.com/ipfs/go-ipfs/issues/3397 to get updates.

When you add a new experimental feature to go-ipfs, or change an experimental
feature, you MUST please make a PR updating this document, and link the PR in
the above issue.

- [ipfs pubsub](#ipfs-pubsub)
- [Client mode DHT routing](#client-mode-dht-routing)
- [go-multiplex stream muxer](#go-multiplex-stream-muxer)
- [Raw leaves for unixfs files](#raw-leaves-for-unixfs-files)
- [ipfs filestore](#ipfs-filestore)
- [Private Networks](#private-networks)
- [ipfs p2p](#ipfs-p2p)
- [Circuit Relay](#circuit-relay)

---

## ipfs pubsub

### State

experimental, default-disabled.

### In Version

0.4.5

### How to enable

run your daemon with the `--enable-pubsub-experiment` flag. Then use the
`ipfs pubsub` commands.

### Road to being a real feature
- [ ] Needs more people to use and report on how well it works
- [ ] Needs authenticated modes to be implemented
- [ ] needs performance analyses to be done

---

## Client mode DHT routing
Allows the dht to be run in a mode that doesnt serve requests to the network,
saving bandwidth.

### State
experimental.

### In Version
0.4.5

### How to enable
run your daemon with the `--routing=dhtclient` flag.

### Road to being a real feature
- [ ] Needs more people to use and report on how well it works.
- [ ] Needs analysis of effect it has on the network as a whole.

---

## go-multiplex stream muxer
Adds support for using the go-multiplex stream muxer alongside (or instead of)
yamux and spdy. This multiplexer is far simpler, and uses less memory and
bandwidth than the others, but is lacking on congestion control and backpressure
logic. It is available to try out and experiment with.

### State
Experimental

### In Version
0.4.5

### How to enable
run your daemon with `--enable-mplex-experiment`

To make it the default stream muxer, set the environment variable
`LIBP2P_MUX_PREFS` as follows:
```
export LIBP2P_MUX_PREFS="/mplex/6.7.0 /yamux/1.0.0 /spdy/3.1.0"
```

To check which stream muxer is being used between any two given peers, check the
json output of the `ipfs swarm peers` command, you'll see something like this:
```
$ ipfs swarm peers -v --enc=json | jq .
{
  "Peers": [
    {
      "Addr": "/ip4/104.131.131.82/tcp/4001",
      "Peer": "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
      "Latency": "46.032624ms",
      "Muxer": "*peerstream_multiplex.conn",
      "Streams": [
        {
          "Protocol": "/ipfs/bitswap/1.1.0"
        },
        {
          "Protocol": "/ipfs/kad/1.0.0"
        },
        {
          "Protocol": "/ipfs/kad/1.0.0"
        }
      ]
    },
    {
...
```

### Road to being a real feature
- [ ] Significant real world testing and performance metrics across a wide
      variety of workloads showing that it works well.

---

## Raw Leaves for unixfs files
Allows files to be added with no formatting in the leaf nodes of the graph.

### State
experimental.

### In Version
master, 0.4.5

### How to enable
Use `--raw-leaves` flag when calling `ipfs add`.

### Road to being a real feature
- [ ] Needs more people to use and report on how well it works.

---

## ipfs filestore
Allows files to be added without duplicating the space they take up on disk.

### State
experimental.

### In Version
master, 0.4.7

### How to enable
Modify your ipfs config:
```
ipfs config --json Experimental.FilestoreEnabled true
```

And then pass the `--nocopy` flag when running `ipfs add`

### Road to being a real feature
- [ ] Needs more people to use and report on how well it works.
- [ ] Need to address error states and failure conditions
- [ ] Need to write docs on usage, advantages, disadvantages
- [ ] Need to merge utility commands to aid in maintenance and repair of filestore

---

## Private Networks

Allows ipfs to only connect to other peers who have a shared secret key.

### State
Experimental

### In Version
master, 0.4.7

### How to enable
Generate a pre-shared-key using [ipfs-swarm-key-gen](https://github.com/Kubuxu/go-ipfs-swarm-key-gen)):
```
go get github.com/Kubuxu/go-ipfs-swarm-key-gen/ipfs-swarm-key-gen
ipfs-swarm-key-gen > ~/.ipfs/swarm.key
```

To join a given private network, get the key file from someone in the network
and save it to `~/.ipfs/swarm.key` (If you are using a custom `$IPFS_PATH`, put
it in there instead).

When using this feature, you will not be able to connect to the default bootstrap
nodes (Since we arent part of your private network) so you will need to set up
your own bootstrap nodes.

First, to prevent your node from even trying to connect to the default bootstrap nodes, run:
```bash
ipfs bootstrap rm --all
```

Then add your own bootstrap peers with:
```bash
ipfs bootstrap add <multiaddr>
```

For example:
```
ipfs bootstrap add /ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64
```

Bootstrap nodes are no different from all other nodes in the network apart from
the function they serve.

To be extra cautious, You can also set the `LIBP2P_FORCE_PNET` environment
variable to `1` to force the usage of private networks. If no private network is
configured, the daemon will fail to start.

### Road to being a real feature
- [ ] Needs more people to use and report on how well it works
- [ ] More documentation

---

## ipfs p2p
Allows to tunnel TCP connections through Libp2p sterams

### State
Experimental

### In Version
master, 0.4.10

### How to enable
P2P command needs to be enabled in config

`ipfs config --json Experimental.Libp2pStreamMounting true`

### How to use

Basic usage:

- Open a listener on one node (node A)
`ipfs p2p listener open p2p-test /ip4/127.0.0.1/tcp/10101`
- Where `/ip4/127.0.0.1/tcp/10101` put address of application you want to pass
  p2p connections to
- On the other node, connect to the listener on node A
`ipfs p2p stream dial $NODE_A_PEERID p2p-test /ip4/127.0.0.1/tcp/10102`
- Node B is now listening for a connection on TCP at 127.0.0.1:10102, connect
  your application there to complete the connection

### Road to being a real feature
- [ ] Needs more people to use and report on how well it works / fits use cases
- [ ] More documentation
- [ ] Support other protocols

---

## Circuit Relay

Allows peers to connect through an intermediate relay node when there
is no direct connectivity.

### State
Experimental

### In Version
master, 0.4.11

### How to enable

The relay transport is enabled by default, which allows peers to dial through
relay and listens for incoming relay connections. The transport can be disabled
by setting `Swarm.DisableRelay = true` in the configuration.

By default, peers don't act as intermediate nodes (relays). This can be enabled
by setting `Swarm.EnableRelayHop = true` in the configuration. Note that the
option needs to be set before online services are started to have an effect; an
already online node would have to be restarted.

### Basic Usage:

In order to connect peers QmA and QmB through a relay node QmRelay:

- Both peers should connect to the relay:
`ipfs swarm connect /transport/address/ipfs/QmRelay`
- Peer QmA can then connect to peer QmB using the relay:
`ipfs swarm connect /ipfs/QmRelay/p2p-cricuit/ipfs/QmB`

Peers can also connect with an unspecific relay address, which will
try to dial through known relays:
`ipfs swarm connect /p2p-circuit/ipfs/QmB`

Peers can see their (unspecific) relay address in the output of
`ipfs swarm addrs listen`

### Road to being a real feature

- [ ] Needs more people to use it and report on how well it works.
- [ ] Advertise relay addresses to the DHT for NATed or otherwise unreachable
      peers.
- [ ] Active relay discovery for specific relay address advertisement. We would
      like advertised relay addresses to designate specific relays for efficient
      dialing.
- [ ] Dialing priorities for relay addresses; arguably, relay addresses should
      have lower priority than direct dials.

## Plugins

### In Version
0.4.11

Plugins allow to add functionality without the need to recompile the daemon.

### Basic Usage:

See [Plugin docs](./plugins.md)

### Road to being a real feature

- [ ] Better support for platforms other than Linux
- [ ] More plugins and plugin types
- [ ] Feedback on stability
