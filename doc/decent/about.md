[Home](../../README.md) » [Use Cases](../../README.md#use-cases) » **Decentralized** »

**About**&nbsp; | &nbsp;[Quickstart](quickstart.md)&nbsp; | &nbsp;[Architectures](architectures.md)&nbsp; | &nbsp;[P2P Chat Demo](demo-p2p-chat.md)&nbsp; | &nbsp;[IPFS Chat Demo](demo-ipfs-chat.md)
<br><br>
# Noms — The Decentralized Database

[Noms](http://noms.io) makes it ~~easy~~ tractable to create rich,
multiuser, collaborative, fully-decentralized applications.

Like most databases, Noms features a rich data model, atomic
transactions, support for large-scale data, and efficient searches,
scans, reads, and updates.

Unlike any other database, Noms has built-in multiparty sync and
conflict resolution. This feature makes Noms a very good fit for P2P
decentralized applications.

Any number of dapp peers in a P2P network can
concurrently modify the same logical Noms database, and continuously
and efficiently sync their changes with each other. All peers will
converge to the same state.

For many applications, peers can store an entire local copy of the
data they are interested in. For larger applications, it should be
possible to back Noms by a decentralized blockstore like IPFS, Swarm,
or Sia (or in the future, Filecoin), and store large-scale data in a
completely decentralized way, without replicating it on every
node. Noms also has a blockstore for S3, which is ideal for
applications that have some centralized components.

**We'd love to talk to you about the possibility of using noms in your project** so please don't hestitate to contact us at [noms@attic.io](mailto:noms@attic.io). 

## How it Works

Think of Noms like a programmable Git: changes are bundled as commits
which reference previous states of the database. Apps pull changes
from peers and merge them using a principled set of APIs and
strategies. Except that rather than users manually pulling and
merging, applications typically do this continuously, automatically
converging to a shared state.

Your application uses a [Go client
library](https://github.com/attic-labs/noms/blob/master/doc/go-tour.md)
to interact with Noms data. There is also a [command-line
interface](https://github.com/attic-labs/noms/blob/master/doc/cli-tour.md)
for working with data and initial support for a [GraphQL-based query
language](https://github.com/attic-labs/noms/blob/master/go/ngql/README.md).

Some additional features include:
* **Versioning**: It’s easy to use, compare, or revert to older database versions
* **Efficient diffs**: diffing even huge datasets is efficient due to
  noms’ use of a novel BTree-like data structure called a [Prolly
  Tree](https://github.com/attic-labs/noms/blob/master/doc/intro.md#prolly-trees-probabilistic-b-trees)
* **Efficient storage**: data are chunked and content-addressable, so
  there is exactly one copy of each chunk in the database, shared by
  other data that reference it. Small changes to massive data
  structures always result in small operations.
* **Verifiable**: The entire database rolls up to a single 20-byte hash
 that uniquely represents the database at that moment - anyone can
 verify that a particular database hashes to the same value

Read the [Noms design overview](https://github.com/attic-labs/noms/blob/master/doc/decent/intro.md).

## Status

For overall status of the database, see [Noms Status](../../README.md#status).

For the decentralized use case in particular: we are fairly confident in this approach and are actively looking for partners to work with to build it out.

- [x] Demonstrate core concept of using Noms to continuously sync across many users (Done! See noms-chat demos)
- [ ] Demonstrate using libp2p or similar to traverse NATs
- [ ] Investigate backing IPFS with Noms rather than the reverse - this should improve stability and dramatically improve local performance
- [ ] Demonstrate using IPFS with a schema that permits nodes to disappear

**_If you would like to use noms in your project we’d love to hear from you_**:
drop us an email ([noms@attic.io](mailto:noms@attic.io)) or send us a
message in slack ([slack.noms.io](http://slack.noms.io)).
