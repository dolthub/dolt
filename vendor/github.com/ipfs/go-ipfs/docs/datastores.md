# Datastore Configuration Options

This document describes the different possible values for the `Datastore.Spec`
field in the ipfs configuration file.

## flatfs
Stores each key value pair as a file on the filesystem.

The shardFunc is prefixed with `/repo/flatfs/shard/v1` then followed by a descriptor of the sharding strategy. Some example values are:
- `/repo/flatfs/shard/v1/next-to-last/2`
  - Shards on the two next to last characters of the key
- `/repo/flatfs/shard/v1/prefix/2`
  - Shards based on the two character prefix of the key

```json
{
	"type": "flatfs",
	"path": "<relative path within repo for flatfs root>",
	"shardFunc": "<a descriptor of the sharding scheme>",
	"sync": true|false
}
```

NOTE: flatfs should only be used as a block store (mounted at `/blocks`) as the
current implementation is not complete.

## levelds
Uses a leveldb database to store key value pairs.

```json
{
	"type": "levelds",
	"path": "<location of db inside repo>",
	"compression": "none" | "snappy",
}
```

## badgerds
Uses [badger](https://github.com/dgraph-io/badger) as a key value store.

```json
{
	"type": "badgerds",
	"path": "<location of badger inside repo",
	"syncWrites": true|false
}
```

## mount
Allows specified datastores to handle keys prefixed with a given path.
The mountpoints are added as keys within the child datastore definitions.

```json
{
	"type": "mount",
	"mounts": [
		{
			// Insert other datastore definition here, but add the following key:
			"mountpoint": "/path/to/handle"
		},
		{
			// Insert other datastore definition here, but add the following key:
			"mountpoint": "/path/to/handle"
		},
	]
}
```

## measure
This datastore is a wrapper that adds metrics tracking to any datastore.

```json
{
	"type": "measure",
	"prefix": "sometag.datastore",
	"child": { datastore being wrapped }
}
```

