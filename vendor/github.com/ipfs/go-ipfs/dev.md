# go-ipfs development

This is a simple description of where the codebase stands.

There are multiple subpackages:

- `bitswap` - the block exchange
- `blocks` - handles dealing with individual blocks and sharding files
- `blockservice` - handles getting and storing blocks
- `cmd/ipfs` - cli ipfs tool - the main **entrypoint** atm
- `config` - load/edit configuration
- `core` - the core node, joins all the pieces
- `fuse/readonly` - mount `/ipfs` as a readonly fuse fs
- `importer` - import files into ipfs
- `merkledag` - merkle dag data structure
- `path` - path resolution over merkledag data structure
- `peer` - identity + addresses of local and remote peers
- `routing` - the routing system
- `routing/dht` - the DHT default routing system implementation
- `swarm` - connection multiplexing, many peers and many transports
- `util` - various utilities


### What's done:

- merkle dag data structure
- path resolution over merkle dag
- local storage of blocks
- basic file import/export (`ipfs add`, `ipfs cat`)
- mounting `/ipfs` (try `{cat, ls} /ipfs/<path>`)
- multiplexing connections (tcp atm)
- peer addressing
- dht - impl basic kademlia routing
- bitswap - impl basic block exchange functionality
- crypto - building trust between peers in the network
- block splitting on import - Rabin fingerprints, etc

### What's in progress:

- ipns - impl `/ipns` obj publishing + path resolution
- expose objects to the web at `http://ipfs.io/<path>`


### What's next:

- version control - `commit` like data structure
- more...

## Cool demos

A list of cool demos to work towards

- boot a VM from an image in ipfs
- boot a VM from a filesystem tree in ipfs
- publish static websites directly from ipfs
- expose objects to the web at `http://ipfs.io/<path>`
- mounted auto-committing versioned personal dropbox
- mounted encrypted personal/group dropbox
- mounted {npm, apt, other pkg manager} registry
- open a video on ipfs, stream it in
- watch a video with a topology of 1 seed N leechers (N ~100)
- more in section 3.8 in the [paper](https://github.com/ipfs/ipfs/blob/master/papers/ipfs-cap2pfs/ipfs-p2p-file-system.pdf)
