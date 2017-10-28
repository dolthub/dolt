# The go-ipfs config file

The go-ipfs config file is a json document. It is read once at node instantiation,
either for an offline command, or when starting the daemon. Commands that execute
on a running daemon do not read the config file at runtime.

## Table of Contents

- [`Addresses`](#addresses)
- [`API`](#api)
- [`Bootstrap`](#bootstrap)
- [`Datastore`](#datastore)
- [`Discovery`](#discovery)
- [`Gateway`](#gateway)
- [`Identity`](#identity)
- [`Ipns`](#ipns)
- [`Mounts`](#mounts)
- [`Reprovider`](#reprovider)
- [`Swarm`](#swarm)

## `Addresses`
Contains information about various listener addresses to be used by this node.

- `API`
Multiaddr describing the address to serve the local HTTP API on.

Default: `/ip4/127.0.0.1/tcp/4001`

- `Gateway`
Multiaddr describing the address to serve the local gateway on.

Default: `/ip4/127.0.0.1/tcp/8080`

- `Swarm`
Array of multiaddrs describing which addresses to listen on for p2p swarm connections.

Default:
```json
[
  "/ip4/0.0.0.0/tcp/4001",
  "/ip6/::/tcp/4001"
]
```

## `API`
Contains information used by the API gateway.

- `HTTPHeaders`
Map of HTTP headers to set on responses from the API HTTP server.

Example:
```json
{
	"Foo": ["bar"]
}
```

Default: `null`

## `Bootstrap`
Bootstrap is an array of multiaddrs of trusted nodes to connect to in order to
initiate a connection to the network.

Default: The ipfs.io bootstrap nodes

## `Datastore`
Contains information related to the construction and operation of the on-disk
storage system.

- `StorageMax`
An upper limit on the total size of the ipfs repository's datastore. Writes to
the datastore will begin to fail once this limit is reached.

Default: `10GB`

- `StorageGCWatermark`
The percentage of the `StorageMax` value at which a garbage collection will be
triggered automatically if the daemon was run with automatic gc enabled (that
option defaults to false currently).

Default: `90`

- `GCPeriod`
A time duration specifying how frequently to run a garbage collection. Only used
if automatic gc is enabled.

Default: `1h`

- `HashOnRead`
A boolean value. If set to true, all block reads from disk will be hashed and
verified. This will cause increased CPU utilization.

- `BloomFilterSize`
A number representing the size in bytes of the blockstore's bloom filter. A
value of zero represents the feature being disabled.

Default: `0`

- `Spec`
Spec defines the structure of the ipfs datastore. It is a composable structure, where each datastore is represented by a json object. Datastores can wrap other datastores to provide extra functionality (eg metrics, logging, or caching).

This can be changed manually, however, if you make any changes that require a different on-disk structure, you will need to run the [ipfs-ds-convert tool](https://github.com/ipfs/ipfs-ds-convert) to migrate data into the new structures.

For more information on possible values for this configuration option, see docs/datastores.md 

Default:
```
{
  "mounts": [
	{
	  "child": {
		"path": "blocks",
		"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
		"sync": true,
		"type": "flatfs"
	  },
	  "mountpoint": "/blocks",
	  "prefix": "flatfs.datastore",
	  "type": "measure"
	},
	{
	  "child": {
		"compression": "none",
		"path": "datastore",
		"type": "levelds"
	  },
	  "mountpoint": "/",
	  "prefix": "leveldb.datastore",
	  "type": "measure"
	}
  ],
  "type": "mount"
}
```

## `Discovery`
Contains options for configuring ipfs node discovery mechanisms.

- `MDNS`
Options for multicast dns peer discovery.

  - `Enabled`
A boolean value for whether or not mdns should be active.

Default: `true`

  -  `Interval`
A number of seconds to wait between discovery checks.


## `Gateway`
Options for the HTTP gateway.

- `HTTPHeaders`
Headers to set on gateway responses.

Default:
```json
{
	"Access-Control-Allow-Headers": [
		"X-Requested-With"
	],
	"Access-Control-Allow-Methods": [
		"GET"
	],
	"Access-Control-Allow-Origin": [
		"*"
	]
}
```

- `RootRedirect`
A url to redirect requests for `/` to.

Default: `""`

- `Writeable`
A boolean to configure whether the gateway is writeable or not.

Default: `false`

- `PathPrefixes`
TODO

Default: `[]`

## `Identity`

- `PeerID`
The unique PKI identity label for this configs peer. Set on init and never read,
its merely here for convenience. Ipfs will always generate the peerID from its
keypair at runtime.

- `PrivKey`
The base64 encoded protobuf describing (and containing) the nodes private key.

## `Ipns`

- `RepublishPeriod`
A time duration specifying how frequently to republish ipns records to ensure
they stay fresh on the network. If unset, we default to 12 hours.

- `RecordLifetime`
A time duration specifying the value to set on ipns records for their validity
lifetime.
If unset, we default to 24 hours.

- `ResolveCacheSize`
The number of entries to store in an LRU cache of resolved ipns entries. Entries
will be kept cached until their lifetime is expired.

Default: `128`

## `Mounts`
FUSE mount point configuration options.

- `IPFS`
Mountpoint for `/ipfs/`.

- `IPNS`
Mountpoint for `/ipns/`.

- `FuseAllowOther`
Sets the FUSE allow other option on the mountpoint.

## `Reprovider`

- `Interval`
Sets the time between rounds of reproviding local content to the routing
system. If unset, it defaults to 12 hours. If set to the value `"0"` it will
disable content reproviding.

Note: disabling content reproviding will result in other nodes on the network
not being able to discover that you have the objects that you have. If you want
to have this disabled and keep the network aware of what you have, you must
manually announce your content periodically.

- `Strategy`
Tells reprovider what should be announced. Valid strategies are:
  - "all" (default) - announce all stored data
  - "pinned" - only announce pinned data
  - "roots" - only announce directly pinned keys and root keys of recursive pins

## `Swarm`
Options for configuring the swarm.

- `AddrFilters`
An array of address filters (multiaddr netmasks) to filter dials to.
See [this issue](https://github.com/ipfs/go-ipfs/issues/1226#issuecomment-120494604) for more
information.

- `DisableBandwidthMetrics`
A boolean value that when set to true, will cause ipfs to not keep track of
bandwidth metrics. Disabling bandwidth metrics can lead to a slight performance
improvement, as well as a reduction in memory usage.

- `DisableNatPortMap`
Disable NAT discovery.

- `DisableRelay`
Disables the p2p-circuit relay transport.

- `EnableRelayHop`
Enables HOP relay for the node. If this is enabled, the node will act as
an intermediate (Hop Relay) node in relay circuits for connected peers.

### `ConnMgr`
Connection manager configuration.

- `Type`
Sets the type of connection manager to use, options are: `"none"` and `"basic"`.

- `LowWater`
LowWater is the minimum number of connections to maintain.

- `HighWater`
HighWater is the number of connections that, when exceeded, will trigger a connection GC operation.
- `GracePeriod`
GracePeriod is a time duration that new connections are immune from being closed by the connection manager.
