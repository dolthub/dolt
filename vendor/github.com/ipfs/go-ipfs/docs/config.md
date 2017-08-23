# The go-ipfs config file

The go-ipfs config file is a json document. It is read once at node instantiation,
either for an offline command, or for starting the daemon. Commands that execute on
a running daemon do not read the config file at runtime.

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
- [`ReproviderInterval`](#reproviderinterval)
- [`SupernodeRouting`](#supernoderouting)
- [`Swarm`](#swarm)
- [`Tour`](#tour)

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

- `Type`
Denotes overall datastore type. The only currently valid option is `leveldb`.

Default: `leveldb`

- `Path`
Path to the leveldb datastore directory. Set during init to either `$IPFS_PATH/datastore`, or `$HOME/.ipfs/datastore` if `$IPFS_PATH` is unset.

- `StorageMax`
An upper limit on the total size of the ipfs repository's datastore. Writes to the datastore will begin to fail once this limit is reached.

Default: `10GB`

- `StorageGCWatermark`
The percentage of the `StorageMax` value at which a garbage collection will be triggered automatically if the daemon was run with automatic gc enabled (that option defaults to false currently).

Default: `90`

- `GCPeriod`
A time duration specifying how frequently to run a garbage collection. Only used if automatic gc is enabled.

Default: `1h`

- `NoSync` *!*
A boolean value denoting whether or not to disable sanity syncing in the flatfs datastore code. Setting this to true may significantly improve performance, but be careful using it as if the daemon is killed before a write is synchronized to disk, there is a chance of data loss.

Default: `false`

- `HashOnRead`
A boolean value. If set to true, all block reads from disk will be hashed and verified. This will cause increased CPU utilization.

- `BloomFilterSize`
A number representing the size in bytes of the blockstore's bloom filter. A value of zero represents the feature being disabled.

Default: `0` 

- `Params`
Extra parameters for datastore construction, not currently used.

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
The unique PKI identity label for this configs peer. Set on init and never read, its merely here for convenience. Ipfs will always generate the peerID from its keypair at runtime.

- `PrivKey`
The base64 encoded protobuf describing (and containing) the nodes private key.

## `Ipns`

- `RepublishPeriod`
A time duration specifying how frequently to republish ipns records to ensure they stay fresh on the network. If unset, we default to 12 hours.

- `RecordLifetime`
A time duration specifying the value to set on ipns records for their validity lifetime.
If unset, we default to 24 hours.

- `ResolveCacheSize`
The number of entries to store in an LRU cache of resolved ipns entries. Entries will be kept cached until their lifetime is expired.

Default: `128`

## `Mounts`
FUSE mount point configuration options.

- `IPFS`
Mountpoint for `/ipfs/`.

- `IPNS`
Mountpoint for `/ipns/`.

- `FuseAllowOther`
Sets the FUSE allow other option on the mountpoint.

## `ReproviderInterval`
Sets the time between rounds of reproviding local content to the routing
system. If unset, it defaults to 12 hours. If set to the value `"0"` it will
disable content reproviding.

Note: disabling content reproviding will result in other nodes on the network
not being able to discover that you have the objects that you have. If you want
to have this disabled and keep the network aware of what you have, you must
manually announce your content periodically.

## `SupernodeRouting`
Deprecated.

## `Swarm`
Options for configuring the swarm.

- `AddrFilters`
An array of address filters (multiaddr netmasks) to filter dials to.
See https://github.com/ipfs/go-ipfs/issues/1226#issuecomment-120494604 for more information.

- `DisableBandwidthMetrics`
A boolean value that when set to true, will cause ipfs to not keep track of
bandwidth metrics. Disabling bandwidth metrics can lead to a slight performance
improvement, as well as a reduction in memory usage.

- `DisableNatPortMap`
Disable NAT discovery.

## `Tour`
Unused.
