# Network

The IPFS Network package handles all of the peer-to-peer networking. It connects to other hosts, it encrypts communications, it muxes messages between the network's client services and target hosts. It has multiple subcomponents:

- `Conn` - a connection to a single Peer
  - `MultiConn` - a set of connections to a single Peer
  - `SecureConn` - an encrypted (tls-like) connection
- `Swarm` - holds connections to Peers, multiplexes from/to each `MultiConn`
- `Muxer` - multiplexes between `Services` and `Swarm`. Handles `Requet/Reply`.
  - `Service` - connects between an outside client service and Network.
  - `Handler` - the client service part that handles requests

It looks a bit like this:


![](https://docs.google.com/drawings/d/1FvU7GImRsb9GvAWDDo1le85jIrnFJNVB_OTPXC15WwM/pub?h=480)

