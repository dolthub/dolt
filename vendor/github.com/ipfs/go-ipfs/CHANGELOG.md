# go-ipfs changelog

## 0.4.18 2018-10-26

This is probably one of the largest go-ipfs releases in recent history, 3 months
in the making.

### Features

The headline features this release are experimental QUIC support, the gossipsub
pubsub routing algorithm, pubsub message signing, and a refactored `ipfs p2p`
command. However, that's just scratching the surface.

#### QUIC

First up, on the networking front, this release has also introduced experimental
support for the QUIC protocol. QUIC is a new UDP-based network transport that
solves many of the long standing issues with TCP.

For us, this means (eventually):

* **Fewer local resources.** TCP requires a file-descriptor per connection while
  QUIC (and most UDP based transports) can share a single file descriptor
  between all connections. This should allow us to dial faster and keep more
  connections open.
* **Faster connection establishment.** When client authentication is included,
  QUIC has a three-way handshake like TCP. However, unlike TCP, this handshake
  brings us from all the way from 0 to a fully encrypted, authenticated, and
  multiplexed connection. In theory (not yet in practice), this should
  significantly reduce the latency of DHT queries.
* **Behaves better on lossy networks.** When multiplexing multiple requests over
  a single TCP connection, a single dropped packet will bring the entire
  connection to a halt while the packet is re-transmitted. However, because QUIC
  handles multiplexing internally, dropping a single packets affects only the
  related stream.
* **Better NAT traversal.** TL;DR: NAT hole-punching is significantly easier
  and, in many cases, more reliable with UDP than with TCP.

However, we still have a long way to go. While we encourage users to test this,
the IETF QUIC protocol is still being actively developed and *will* change. You
can find instructions for enabling it
[here](https://github.com/ipfs/go-ipfs/docs/experimental-features.md#QUIC).

#### Pubsub

In terms of pubsub, go-ipfs now supports the gossipsub routing algorithm and
message signing.

The gossipsub routing algorithm is *significantly* more efficient than the
current floodsub routing algorithm. Even better, it's fully backwards compatible
so you can enable it and still talk to nodes using the floodsub algorithm. You
can find instructions to enable gossipsub in go-ipfs
[here](https://github.com/ipfs/go-ipfs/docs/experimental-features.md#gossipsub).

Messages are now signed by their authors. While signing has now been enabled by
default, strict signature verification has not been and will not be for at least
one release (probably multiple) to avoid breaking existing applications. You can
read about how to configure this feature
[here](https://github.com/ipfs/go-ipfs/docs/experimental-features.md#message-signing).

#### Commands

In terms of new toys, this release introduces a new `ipfs cid` subcommand for
working with CIDs, a completely refactored `ipfs p2p` command, streaming name
resolution, and complete inline block support.

The new `ipfs cid` command allows users to both inspect CIDs and convert them
between various formats and versions. For example:

```sh
# Print out the CID metadata (prefix)
> ipfs cid format -f %P QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o
cidv0-protobuf-sha2-256-32

# Get the hex sha256 hash from the CID.
> ipfs cid format -b base16 -f '0x%D' QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o
0x46d44814b9c5af141c3aaab7c05dc5e844ead5f91f12858b021eba45768b4c0e

# Convert a base58 v0 CID to a base32 v1 CID.
> ipfs cid base32 QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o
bafybeicg2rebjoofv4kbyovkw7af3rpiitvnl6i7ckcywaq6xjcxnc2mby
```

The refactored `ipfs p2p` command allows forwarding TCP streams through two IPFS
nodes from one host to another. It's `ssh -L` but for IPFS. You can find
documentation 
[here](https://github.com/ipfs/go-ipfs/docs/experimental-features.md#ipfs-p2p).
It's still experimental but we don't expect too many breaking changes at this
point (it will very likely be stabilized in the next release). Quick summary of
breaking changes:

* We don't stop listening for local (forwarded) connections after accepting a
  single connection.
* `ipfs p2p stream ls` output now returns more useful output, first address is
  always the initiator address.
* `ipfs p2p listener ls` is renamed to `ipfs p2p ls`
* `ipfs p2p listener close` is renamed to `ipfs p2p close`
* Protocol names have to be prefixed with `/x/` and are now just passed to
  libp2p as handler name. Previous version did this 'under the hood' and with
  `/p2p/` prefix. There is a `--allow-custom-protocol` flag which allows you
  to use any libp2p handler name.
* `ipfs p2p listener open` and `ipfs p2p stream dial` got renamed:
    * `ipfs p2p listener open p2p-test /ip4/127.0.0.1/tcp/10101`
      new becomes `ipfs p2p listen /x/p2p-test /ip4/127.0.0.1/tcp/10101`
    * `ipfs p2p stream dial $NODE_A_PEERID p2p-test /ip4/127.0.0.1/tcp/10102`
      is now `ipfs p2p forward /x/p2p-test /ip4/127.0.0.1/tcp/10102 /ipfs/$NODE_A_PEERID`

There is now a new flag for `ipfs name resolve` - `--stream`. When the command
is invoked with the flag set, it will start returning results as soon as they
are discovered in the DHT and other routing mechanisms. This enables certain
applications to start prefetching/displaying data while the discovery is still
running. Note that this command will likely return many outdated records
before it finding and returning the latest. However, it will always return
*valid* records (even if a bit stale).

Finally, in the previous release, we added support for extracting blocks inlined
into CIDs. In this release, we've added support for creating these CIDs. You can
now run `ipfs add` with the `--inline` flag to inline blocks less than or equal
to 32 bytes in length into a CID, instead of writing an actual block. This
should significantly reduce the size of filesystem trees with many empty
directories and tiny files.

#### IPNS

You can now publish and resolve paths with namespaces *other* than `/ipns` and
`/ipfs` through IPNS. Critically, IPNS can now be used with IPLD paths (paths
starting with `/ipld`).

#### WebUI

Finally, this release includes the shiny [updated
webui](https://github.com/ipfs-shipyard/ipfs-webui). You can view it by
installing go-ipfs and visiting http://localhost:5001/webui.

### Performance

This release includes some significant performance improvements, both in terms
of resource utilization and speed. This section will go into some technical
details so feel free to skip it if you're just looking for shiny new features.

#### Resource Utilization

In this release, we've (a) fixed a slow memory leak in libp2p and (b)
significantly reduced the allocation load. Together, these should improve both
memory and CPU usage.

##### Datastructures

We've changed two of our most frequently used datastructures, CIDs and
Multiaddrs, to reduce allocation load.

First, we now store CIDs *encode* as strings, instead of decoded in structs
(behind pointers). In addition to being more compact, our `Cid` type is now a
valid `map` key so we no longer have to encode CIDs every time we want to use
them in a map/set. Allocations when inserting CIDs into maps/sets was showing up
as a significant source of allocations under heavy load so this change should
improve memory usage.

Second, we've changed many of our multiaddr parsing/processing/formatting
functions to allocate less. Much of our DHT related-work includes processing
multiaddrs so this should reduce CPU utilization when heavily using the DHT.

##### Streams and Yamux

Streams have always plagued us in terms of memory utilization. This was
partially solved by introducing the connection manager, keeping our maximum
connection count to a reasonable number but they're still a major memory sink.

This release sees two improvements on this front:

1. A memory [leak in identify](https://github.com/libp2p/go-libp2p/issues/419)
   has been fixed. This was slowly causing us to leak connections (locking up
   the memory used by the connections' streams).
2. Yamux streams now use a buffer-pool backed, auto shrinking read buffer.
   Before, this read buffer would grow to its maximum size (a few megabytes) and
   never shrink but these buffers now shrink as they're emptied.

#### Bitswap Performance

Bitswap will now pack *multiple* small blocks into a single message thanks
[ipfs/go-bitswap#5](https://github.com/ipfs/go-bitswap/pull/5). While this won't
help when transferring large files (with large blocks), this should help when
transferring many tiny files.

### Refactors and Endeavors

This release saw yet another commands-library refactor, work towards the
CoreAPI, and the first step towards reliable base32 CID support.

#### Commands Lib

We've completely refactored our commands library (again). While it still needs
quite a bit of work, it now requires significantly less boilerplate and should
be significantly more robust. The refactor immediately found two broken tests
and probably fixed quite a few bugs around properly returning and handling
errors.

#### CoreAPI

CoreAPI is a new way to interact with IPFS from Go. While it's still not
final, most things you can do via the CLI or HTTP interfaces, can now be done
through the new API.

Currently there is only one implementation, backed by go-ipfs node, and there are
plans to start http-api backed one soon. We are also looking into creating RPC
interface using this API, which could help performance in some use cases.

You can track progress in https://github.com/ipfs/go-ipfs/issues/4498

#### IPLD paths

We introduced new path type which introduces distinction between IPLD and
IPFS (unixfs) paths. From now on paths prefixed with `/ipld/` will always
use IPLD link traversal and `/ipfs/` will use unixfs path resolver, which
takes things like shardnig into account.

Note that this is only initial support and there likely are some bugs in
how the paths are handled internally, so consider this feature
experimental for now.

#### CIDv1/Base32 Migration

Currently, IPFS is usually used in browsers by browsing to
`https://SOME_GATEWAY/ipfs/CID/...`. There are two significant drawbacks to this
approach:

1. From a browser security standpoint, all IPFS "sites" will live under the same
   origin (SOME_GATEWAY).
2. From a UX standpoint, this doesn't feel very "native" (even if the gateway is
   a local IPFS node).

To fix the security issue, we intend to switch IPFS gateway links
`https://ipfs.io/ipfs/CID` to to `https://CID.ipfs.dweb.link`. This way, the CID
will be a part of the
["origin"](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Origin) so
each IPFS website will get a separate security origin.

To fix the UX issue, we've been working on adding support for `ipfs://CID/...`
to web browsers through our
[ipfs-companion](https://github.com/ipfs/ipfs-companion/) add-on and some new,
experimental extension APIs from Mozilla. This has the same effect of putting
the CID in the URL origin but has the added benefit of looking "native".

Unfortunately, origins must be *case insensitive*. Currently, most CIDs users
see are *CIDv0* CIDs (those starting with `Qm`) which are *always* base58
encoded and are therefore case-sensitive.

Fortunately, CIDv1 (the latest CID format) supports arbitrary bases using the
[multibase](https://github.com/multiformats/multibase/) standard. Unfortunately,
IPFS has always treated equivalent CIDv0 and CIDv1 CIDs as distinct. This means
that files added with CIDv0 CIDs (the default) can't be looked up using the
equivalent CIDv1.

This release makes some significant progress towards solving this issue by
introducing two features:

(1) The previous mentioned `ipfs cid base32` command for converting CID to a
case intensive encoding required by domain names. This command converts a CID to
version 1 and encodes it using base32.

(2) A hack to allow locally looking up blocks associated with a CIDv0 CID using
the equivalent CIDv1 CID (or the reverse). This hack will eventually
be replaced with a multihash indexed blockstore, which is agnostic to both the
CID version and multicodec content type.

### go-ipfs changelog

Features (i.e., users take heed):
  - gossipsub ([ipfs/go-ipfs#5373](https://github.com/ipfs/go-ipfs/pull/5373))
  - support /ipfs/CID in `ipfs dht findprovs` ([ipfs/go-ipfs#5329](https://github.com/ipfs/go-ipfs/pull/5329))
  - return a json object from config show ([ipfs/go-ipfs#5345](https://github.com/ipfs/go-ipfs/pull/5345))
  - Set filename in Content-Disposition if filename=x is passed in URI query ([ipfs/go-ipfs#4177](https://github.com/ipfs/go-ipfs/pull/4177))
  - Allow mfs files.write command to create parent directories ([ipfs/go-ipfs#5359](https://github.com/ipfs/go-ipfs/pull/5359))
  - Run DNS lookup for --api endpoint provided in CLI ([ipfs/go-ipfs#5372](https://github.com/ipfs/go-ipfs/pull/5372))
  - Add support for inlinling blocks into CIDs the id-hash ([ipfs/go-ipfs#5281](https://github.com/ipfs/go-ipfs/pull/5281))
  - depth limited refs -r ([ipfs/go-ipfs#5337](https://github.com/ipfs/go-ipfs/pull/5337))
  - remove bitswap unwant ([ipfs/go-ipfs#5308](https://github.com/ipfs/go-ipfs/pull/5308))
  - add experimental QUIC support ([ipfs/go-ipfs#5350](https://github.com/ipfs/go-ipfs/pull/5350))
  - add a --stdin-name flag for naming files from stdin ([ipfs/go-ipfs#5399](https://github.com/ipfs/go-ipfs/pull/5399))
  - Refactor `ipfs p2p` ([ipfs/go-ipfs#4929](https://github.com/ipfs/go-ipfs/pull/4929))
  - add dns support in`ipfs p2p forward` and refactor code ([ipfs/go-ipfs#5533](https://github.com/ipfs/go-ipfs/pull/5533))
  - feat(command): expose connection direction ([ipfs/go-ipfs#5457](https://github.com/ipfs/go-ipfs/pull/5457))
  - error when publishing ipns records without a running daemon ([ipfs/go-ipfs#5477](https://github.com/ipfs/go-ipfs/pull/5477))
  - feat(daemon): print version on start ([ipfs/go-ipfs#5503](https://github.com/ipfs/go-ipfs/pull/5503))
  - add quieter option to name publish ([ipfs/go-ipfs#5494](https://github.com/ipfs/go-ipfs/pull/5494))
  - Provide new "cid" sub-command. ([ipfs/go-ipfs#5385](https://github.com/ipfs/go-ipfs/pull/5385))
  - feat(command): add force flag for files rm ([ipfs/go-ipfs#5555](https://github.com/ipfs/go-ipfs/pull/5555))
  - Add support for datastore plugins ([ipfs/go-ipfs#5187](https://github.com/ipfs/go-ipfs/pull/5187))
  - files ls: append slash to directory names ([ipfs/go-ipfs#5605](https://github.com/ipfs/go-ipfs/pull/5605))
  - ipfs name resolve --stream ([ipfs/go-ipfs#5404](https://github.com/ipfs/go-ipfs/pull/5404))
  - update webui to 2.1.0 ([ipfs/go-ipfs#5627](https://github.com/ipfs/go-ipfs/pull/5627))
  - feat: add dry-run flag for config profile apply command ([ipfs/go-ipfs#5455](https://github.com/ipfs/go-ipfs/pull/5455))
  - configurable pubsub signing ([ipfs/go-ipfs#5647](https://github.com/ipfs/go-ipfs/pull/5647))

Fixes (i.e., users take note):
  - pin update fixes ([ipfs/go-ipfs#5265](https://github.com/ipfs/go-ipfs/pull/5265))
  - Fix inability to pin two things at once ([ipfs/go-ipfs#5512](https://github.com/ipfs/go-ipfs/pull/5512))
  - wait for all connections to close before exiting on shutdown. ([ipfs/go-ipfs#5322](https://github.com/ipfs/go-ipfs/pull/5322))
  - Fixed ipns address resolution in fuse unix mount ([ipfs/go-ipfs#5384](https://github.com/ipfs/go-ipfs/pull/5384))
  - core/commands/ls: wrap `NewDirectoryFromNode` error ([ipfs/go-ipfs#5166](https://github.com/ipfs/go-ipfs/pull/5166))
  - fix goroutine leaks in filestore.go ([ipfs/go-ipfs#5427](https://github.com/ipfs/go-ipfs/pull/5427))
  - move VersionOption after GatewayOption to fix #5422 ([ipfs/go-ipfs#5424](https://github.com/ipfs/go-ipfs/pull/5424))
  - fix(commands): fix filestore.go goroutine leak ([ipfs/go-ipfs#5439](https://github.com/ipfs/go-ipfs/pull/5439))
  - fix(commands): goroutine leaks in ping.go ([ipfs/go-ipfs#5444](https://github.com/ipfs/go-ipfs/pull/5444))
  - fix output of object command ([ipfs/go-ipfs#5459](https://github.com/ipfs/go-ipfs/pull/5459))
  - add warning when no bootstrap in config ([ipfs/go-ipfs#5445](https://github.com/ipfs/go-ipfs/pull/5445))
  - fix behaviour of key rename to same name ([ipfs/go-ipfs#5465](https://github.com/ipfs/go-ipfs/pull/5465))
  - fix(object): print object diff error ([ipfs/go-ipfs#5469](https://github.com/ipfs/go-ipfs/pull/5469))
  - fix(pin): goroutine leaks ([ipfs/go-ipfs#5453](https://github.com/ipfs/go-ipfs/pull/5453))
  - fix offline id bug ([ipfs/go-ipfs#5486](https://github.com/ipfs/go-ipfs/pull/5486))
  - files cp: improve flush error message ([ipfs/go-ipfs#5485](https://github.com/ipfs/go-ipfs/pull/5485))
  - resolve: fix unixfs resolution through sharded directories ([ipfs/go-ipfs#5484](https://github.com/ipfs/go-ipfs/pull/5484))
  - Switch name publish/resolve to coreapi ([ipfs/go-ipfs#5563](https://github.com/ipfs/go-ipfs/pull/5563))
  - use CoreAPI resolver everywhere (fixes sharded directory resolution) ([ipfs/go-ipfs#5492](https://github.com/ipfs/go-ipfs/pull/5492))
  - add pin lock in AddallPin function ([ipfs/go-ipfs#5506](https://github.com/ipfs/go-ipfs/pull/5506))
  - take the pinlock when updating pins ([ipfs/go-ipfs#5550](https://github.com/ipfs/go-ipfs/pull/5550))
  - fix(object): add support for raw leaves in object diff ([ipfs/go-ipfs#5472](https://github.com/ipfs/go-ipfs/pull/5472))
  - don't use the domain name as a filename in /ipns/a.com ([ipfs/go-ipfs#5564](https://github.com/ipfs/go-ipfs/pull/5564))
  - refactor(command): modify int to int64 ([ipfs/go-ipfs#5612](https://github.com/ipfs/go-ipfs/pull/5612))
  - fix(core): ipns config RecordLifetime panic ([ipfs/go-ipfs#5648](https://github.com/ipfs/go-ipfs/pull/5648))
  - simplify dag put and correctly take pin lock ([ipfs/go-ipfs#5667](https://github.com/ipfs/go-ipfs/pull/5667))
  - fix prometheus concurrent map write bug ([ipfs/go-ipfs#5706](https://github.com/ipfs/go-ipfs/pull/5706))

Regressions Fixes (fixes for bugs introduced since the last release):
  - namesys: properly attach path in name.Resolve ([ipfs/go-ipfs#5660](https://github.com/ipfs/go-ipfs/pull/5660))
  - fix(p2p): issue #5523 ([ipfs/go-ipfs#5529](https://github.com/ipfs/go-ipfs/pull/5529))
  - fix infinite loop in `stats bw` ([ipfs/go-ipfs#5598](https://github.com/ipfs/go-ipfs/pull/5598))
  - make warnings on no bootstrap peers less noisy ([ipfs/go-ipfs#5466](https://github.com/ipfs/go-ipfs/pull/5466))
  - fix two transport related bugs ([ipfs/go-ipfs#5417](https://github.com/ipfs/go-ipfs/pull/5417))
  - Fix pin ls output when hash is specified ([ipfs/go-ipfs#5699](https://github.com/ipfs/go-ipfs/pull/5699))
  - ping: switch to the ping service enabled in the libp2p constructor ([ipfs/go-ipfs#5698](https://github.com/ipfs/go-ipfs/pull/5698))
  - commands: fix a bunch of tiny commands-lib issues ([ipfs/go-ipfs#5697](https://github.com/ipfs/go-ipfs/pull/5697))
  - cleanup the ping command ([ipfs/go-ipfs#5680](https://github.com/ipfs/go-ipfs/pull/5680))
  - fix gossipsub goroutine explosion ([ipfs/go-ipfs#5688](https://github.com/ipfs/go-ipfs/pull/5688))
  - fix(cmd/gc): Run func does not return error when Emit func returns error ([ipfs/go-ipfs#5687](https://github.com/ipfs/go-ipfs/pull/5687))

Extractions:
  - Extract bitswap to go-bitswap ([ipfs/go-ipfs#5294](https://github.com/ipfs/go-ipfs/pull/5294))
  - Extract blockservice and verifcid ([ipfs/go-ipfs#5296](https://github.com/ipfs/go-ipfs/pull/5296))
  - Extract merkledag package, move dagutils to top level ([ipfs/go-ipfs#5298](https://github.com/ipfs/go-ipfs/pull/5298))
  - Extract path and resolver ([ipfs/go-ipfs#5306](https://github.com/ipfs/go-ipfs/pull/5306))
  - Extract config package ([ipfs/go-ipfs#5277](https://github.com/ipfs/go-ipfs/pull/5277))
  - Extract unixfs and importers to go-unixfs ([ipfs/go-ipfs#5316](https://github.com/ipfs/go-ipfs/pull/5316))
  - delete unixfs code... ([ipfs/go-ipfs#5319](https://github.com/ipfs/go-ipfs/pull/5319))
  - Extract /mfs to github.com/ipfs/go-mfs ([ipfs/go-ipfs#5391](https://github.com/ipfs/go-ipfs/pull/5391))
  - re-format log output as ndjson ([ipfs/go-ipfs#5708](https://github.com/ipfs/go-ipfs/pull/5708))
  - error on resolving non-terminal paths ([ipfs/go-ipfs#5705](https://github.com/ipfs/go-ipfs/pull/5705))

Documentation:
  - document the fact that we now publish releases on GitHub ([ipfs/go-ipfs#5301](https://github.com/ipfs/go-ipfs/pull/5301))
  - docs: add url to dev weekly sync to the README ([ipfs/go-ipfs#5371](https://github.com/ipfs/go-ipfs/pull/5371))
  - docs: README refresh, add cli-http-api-core diagram ([ipfs/go-ipfs#5396](https://github.com/ipfs/go-ipfs/pull/5396))
  - add some basic gateway documentation ([ipfs/go-ipfs#5393](https://github.com/ipfs/go-ipfs/pull/5393))
  - fix the default gateway port ([ipfs/go-ipfs#5419](https://github.com/ipfs/go-ipfs/pull/5419))
  - fix order of events in the release process ([ipfs/go-ipfs#5434](https://github.com/ipfs/go-ipfs/pull/5434))
  - docs: add some minimal read-only API documentation ([ipfs/go-ipfs#5437](https://github.com/ipfs/go-ipfs/pull/5437))
  - feat: use package-table ([ipfs/go-ipfs#5395](https://github.com/ipfs/go-ipfs/pull/5395))
  - link to go-{libp2p,ipld} package tables ([ipfs/go-ipfs#5446](https://github.com/ipfs/go-ipfs/pull/5446))
  - api: fix outdated HTTPHeaders config documentation ([ipfs/go-ipfs#5451](https://github.com/ipfs/go-ipfs/pull/5451))
  - add version, usage, and planning info for urlstore ([ipfs/go-ipfs#5552](https://github.com/ipfs/go-ipfs/pull/5552))
  - debug-guide.md added memory statistics command ([ipfs/go-ipfs#5546](https://github.com/ipfs/go-ipfs/pull/5546))
  - Change to point to combined go contributing guidelines ([ipfs/go-ipfs#5607](https://github.com/ipfs/go-ipfs/pull/5607))
  - docs: Update link format ([ipfs/go-ipfs#5617](https://github.com/ipfs/go-ipfs/pull/5617))
  - Fix link in readme ([ipfs/go-ipfs#5632](https://github.com/ipfs/go-ipfs/pull/5632))
  - docs: add a note for dns command ([ipfs/go-ipfs#5629](https://github.com/ipfs/go-ipfs/pull/5629))
  - Dockerfile: Specifies comments on exposed ports ([ipfs/go-ipfs#5615](https://github.com/ipfs/go-ipfs/pull/5615))
  - document pubsub message signing ([ipfs/go-ipfs#5669](https://github.com/ipfs/go-ipfs/pull/5669))

Testing:
  - Include cid-fmt binary in test/bin. ([ipfs/go-ipfs#5297](https://github.com/ipfs/go-ipfs/pull/5297))
  - wait for the nodes to fully stop ([ipfs/go-ipfs#5315](https://github.com/ipfs/go-ipfs/pull/5315))
  - apply timeout for build steps after getting node ([ipfs/go-ipfs#5313](https://github.com/ipfs/go-ipfs/pull/5313))
  - ci: check for gx deps dupes ([ipfs/go-ipfs#5338](https://github.com/ipfs/go-ipfs/pull/5338))
  - ci: call cleanWs after each step ([ipfs/go-ipfs#5374](https://github.com/ipfs/go-ipfs/pull/5374))
  - add correct test for GC completeness ([ipfs/go-ipfs#5364](https://github.com/ipfs/go-ipfs/pull/5364))
  - fix the urlstore tests ([ipfs/go-ipfs#5397](https://github.com/ipfs/go-ipfs/pull/5397))
  - improve gateway options test ([ipfs/go-ipfs#5433](https://github.com/ipfs/go-ipfs/pull/5433))
  - coreapi name: Increase test swarm size ([ipfs/go-ipfs#5481](https://github.com/ipfs/go-ipfs/pull/5481))
  - fix fuse unmount test ([ipfs/go-ipfs#5476](https://github.com/ipfs/go-ipfs/pull/5476))
  - test(add): add test for issue \#5456 ([ipfs/go-ipfs#5493](https://github.com/ipfs/go-ipfs/pull/5493))
  - fixed tests of raised fd limits ([ipfs/go-ipfs#5496](https://github.com/ipfs/go-ipfs/pull/5496))
  - pprof: create HTTP endpoint for setting MutexProfileFraction ([ipfs/go-ipfs#5527](https://github.com/ipfs/go-ipfs/pull/5527))
  - fix(command):update `add --chunker` test ([ipfs/go-ipfs#5571](https://github.com/ipfs/go-ipfs/pull/5571))
  - switch to go 1.11 ([ipfs/go-ipfs#5483](https://github.com/ipfs/go-ipfs/pull/5483))
  - fix: sharness race in directory_size if file is removed ([ipfs/go-ipfs#5586](https://github.com/ipfs/go-ipfs/pull/5586))
  - Bump Go versions and use '.x' to always get latest minor versions ([ipfs/go-ipfs#5682](https://github.com/ipfs/go-ipfs/pull/5682))
  - add rabin min error test ([ipfs/go-ipfs#5449](https://github.com/ipfs/go-ipfs/pull/5449))
  - Use CircleCI 2.0 ([ipfs/go-ipfs#5691](https://github.com/ipfs/go-ipfs/pull/5691))

Internal:
  - Add ability to retrieve blocks even if given using a different CID version ([ipfs/go-ipfs#5285](https://github.com/ipfs/go-ipfs/pull/5285))
  - update gogo-protobuf ([ipfs/go-ipfs#5355](https://github.com/ipfs/go-ipfs/pull/5355))
  - update protobuf files in go-ipfs ([ipfs/go-ipfs#5356](https://github.com/ipfs/go-ipfs/pull/5356))
  - string-backed CIDs ([ipfs/go-ipfs#5441](https://github.com/ipfs/go-ipfs/pull/5441))
  - commands: switch object to CoreAPI ([ipfs/go-ipfs#4643](https://github.com/ipfs/go-ipfs/pull/4643))
  - coreapi: dag: Batching interface ([ipfs/go-ipfs#5340](https://github.com/ipfs/go-ipfs/pull/5340))
  - key cmd: Refactor to use coreapi ([ipfs/go-ipfs#5339](https://github.com/ipfs/go-ipfs/pull/5339))
  - coreapi: DHT API ([ipfs/go-ipfs#4804](https://github.com/ipfs/go-ipfs/pull/4804))
  - block cmd: Use coreapi ([ipfs/go-ipfs#5331](https://github.com/ipfs/go-ipfs/pull/5331))
  - mk: embed CurrentCommit in the right place ([ipfs/go-ipfs#5507](https://github.com/ipfs/go-ipfs/pull/5507))
  - added binary executable files to .dockerignore ([ipfs/go-ipfs#5544](https://github.com/ipfs/go-ipfs/pull/5544))
  - Add sessions when fetching MerkleDAG in LS ([ipfs/go-ipfs#5509](https://github.com/ipfs/go-ipfs/pull/5509))
  - coreapi: Swarm API ([ipfs/go-ipfs#4803](https://github.com/ipfs/go-ipfs/pull/4803))
  - coreapi swarm: unify impl type with other apis ([ipfs/go-ipfs#5551](https://github.com/ipfs/go-ipfs/pull/5551))
  - Refactor UnixFS CoreAPI ([ipfs/go-ipfs#5501](https://github.com/ipfs/go-ipfs/pull/5501))
  - coreapi: PubSub API ([ipfs/go-ipfs#4805](https://github.com/ipfs/go-ipfs/pull/4805))
  - fix: maketarball.sh for OSX ([ipfs/go-ipfs#5575](https://github.com/ipfs/go-ipfs/pull/5575))
  - test the correct return value when checking directory size ([ipfs/go-ipfs#5580](https://github.com/ipfs/go-ipfs/pull/5580))
  - coreapi unixfs: remove Cat ([ipfs/go-ipfs#5574](https://github.com/ipfs/go-ipfs/pull/5574))
  - Explicitally use BufferedDAG after removing Batch from importers ([ipfs/go-ipfs#5626](https://github.com/ipfs/go-ipfs/pull/5626))

Cleanup:
  - Fix some weird code in core/coreunix/add.go ([ipfs/go-ipfs#5354](https://github.com/ipfs/go-ipfs/pull/5354))
  - name cmd: move subcommands to subdirectory ([ipfs/go-ipfs#5392](https://github.com/ipfs/go-ipfs/pull/5392))
  - directly parse peer IDs as peer IDs ([ipfs/go-ipfs#5409](https://github.com/ipfs/go-ipfs/pull/5409))
  - don't bother caching if we're using a nil repo ([ipfs/go-ipfs#5414](https://github.com/ipfs/go-ipfs/pull/5414))
  - object:refactor data encode error ([ipfs/go-ipfs#5426](https://github.com/ipfs/go-ipfs/pull/5426))
  - remove Godeps ([ipfs/go-ipfs#5440](https://github.com/ipfs/go-ipfs/pull/5440))
  - update for the go-ipfs-cmds refactor ([ipfs/go-ipfs#5035](https://github.com/ipfs/go-ipfs/pull/5035))
  - fix(unixfs): issue #5217 (Avoid use of `pb.Data`) ([ipfs/go-ipfs#5505](https://github.com/ipfs/go-ipfs/pull/5505))
  - fix(unixfs): issue #5055 ([ipfs/go-ipfs#5525](https://github.com/ipfs/go-ipfs/pull/5525))
  - add offline id test #4978 and refactor command code ([ipfs/go-ipfs#5562](https://github.com/ipfs/go-ipfs/pull/5562))
  - refact(command): replace option name with const string ([ipfs/go-ipfs#5642](https://github.com/ipfs/go-ipfs/pull/5642))
  - remove p2p-circuit addr hack in ipfs swarm peers ([ipfs/go-ipfs#5645](https://github.com/ipfs/go-ipfs/pull/5645))
  - refactor(commands/id): use new command ([ipfs/go-ipfs#5646](https://github.com/ipfs/go-ipfs/pull/5646))
  - object patch rm-link: change arg from 'link' to 'name' ([ipfs/go-ipfs#5638](https://github.com/ipfs/go-ipfs/pull/5638))
  - refactor(cmds): use new cmds lib in version, tar and dns ([ipfs/go-ipfs#5650](https://github.com/ipfs/go-ipfs/pull/5650))
  - cmds/dag: use new cmds lib ([ipfs/go-ipfs#5662](https://github.com/ipfs/go-ipfs/pull/5662))
  - commands/ping: use new cmds lib ([ipfs/go-ipfs#5675](https://github.com/ipfs/go-ipfs/pull/5675))

### related changelogs

Changes to sub-packages go-ipfs depends on. This *does not* include libp2p or multiformats.

github.com/ipfs/go-log
  - update gogo protobuf ([ipfs/go-log#39](https://github.com/ipfs/go-log/pull/39))
  - rename the protobuf to loggabletracer ([ipfs/go-log#41](https://github.com/ipfs/go-log/pull/41))
  - protect loggers with rwmutex ([ipfs/go-log#44](https://github.com/ipfs/go-log/pull/44))
  - make logging prettier ([ipfs/go-log#45](https://github.com/ipfs/go-log/pull/45))
  - add env vars for logging to file and syslog ([ipfs/go-log#46](https://github.com/ipfs/go-log/pull/46))
  - remove syslogger ([ipfs/go-log#47](https://github.com/ipfs/go-log/pull/47))

github.com/ipfs/go-datastore
  - implement DiskUsage for the rest of the datastores ([ipfs/go-datastore#86](https://github.com/ipfs/go-datastore/pull/86))
  - switch to google's uuid library ([ipfs/go-datastore#89](https://github.com/ipfs/go-datastore/pull/89))
  - return ErrNotFound from the NullDatastore instead of nil, nil ([ipfs/go-datastore#92](https://github.com/ipfs/go-datastore/pull/92))
  - Add TTL and Transactional interfaces ([ipfs/go-datastore#91](https://github.com/ipfs/go-datastore/pull/91))
  - improve testing ([ipfs/go-datastore#93](https://github.com/ipfs/go-datastore/pull/93))
  - Add support for querying entry expiration ([ipfs/go-datastore#96](https://github.com/ipfs/go-datastore/pull/96))
  - Allow ds.NewTransaction() to return an error ([ipfs/go-datastore#98](https://github.com/ipfs/go-datastore/pull/98))
  - add a GetSize method ([ipfs/go-datastore#99](https://github.com/ipfs/go-datastore/pull/99))

github.com/ipfs/go-cid
  - Add tests for Set type ([ipfs/go-cid#63](https://github.com/ipfs/go-cid/pull/63))
  - Create new Builder interface for creating CIDs. ([ipfs/go-cid#53](https://github.com/ipfs/go-cid/pull/53))
  - cid-fmt Enhancments ([ipfs/go-cid#61](https://github.com/ipfs/go-cid/pull/61))
  - add String benchmark ([ipfs/go-cid#44](https://github.com/ipfs/go-cid/pull/44))
  - add a streaming CID set ([ipfs/go-cid#67](https://github.com/ipfs/go-cid/pull/67))
  - Extract non-core functionality from go-cid into go-cidutil ([ipfs/go-cid#69](https://github.com/ipfs/go-cid/pull/69))
  - cid implementation research ([ipfs/go-cid#70](https://github.com/ipfs/go-cid/pull/70))
  - cid implementation variations++ ([ipfs/go-cid#72](https://github.com/ipfs/go-cid/pull/72))
  - Create a new Encode method that is like StringOfBase but never errors ([ipfs/go-cid#60](https://github.com/ipfs/go-cid/pull/60))
  - add codecs for Dash blocks, tx ([ipfs/go-cid#78](https://github.com/ipfs/go-cid/pull/78))

github.com/ipfs/go-ds-flatfs
  - check error before defer-removing disk usage file ([ipfs/go-ds-flatfs#47](https://github.com/ipfs/go-ds-flatfs/pull/47))
  - add GetSize function ([ipfs/go-ds-flatfs#48](https://github.com/ipfs/go-ds-flatfs/pull/48))

github.com/ipfs/go-ds-measure
  -  ([ipfs/go-ds-measure#](https://github.com/ipfs/go-ds-measure/pull/))

github.com/ipfs/go-ds-leveldb
  - recover datastore on corruption ([ipfs/go-ds-leveldb#15](https://github.com/ipfs/go-ds-leveldb/pull/15))
  - Add transactional support to leveldb datastore. ([ipfs/go-ds-leveldb#17](https://github.com/ipfs/go-ds-leveldb/pull/17))
  - implement GetSize ([ipfs/go-ds-leveldb#18](https://github.com/ipfs/go-ds-leveldb/pull/18))

github.com/ipfs/go-metrics-prometheus
  - use an existing metric when it has already been registered ([ipfs/go-metrics-prometheus#1](https://github.com/ipfs/go-metrics-prometheus/pull/1))

github.com/ipfs/go-metrics-interface
  - update the counter interface to match prometheus ([ipfs/go-metrics-interface#2](https://github.com/ipfs/go-metrics-interface/pull/2))

github.com/ipfs/go-ipld-format
  - add copy dagservice function ([ipfs/go-ipld-format#41](https://github.com/ipfs/go-ipld-format/pull/41))

github.com/ipfs/go-ipld-cbor
  - Refactor to refmt ([ipfs/go-ipld-cbor#30](https://github.com/ipfs/go-ipld-cbor/pull/30))
  - import changes from the filecoin branch ([ipfs/go-ipld-cbor#41](https://github.com/ipfs/go-ipld-cbor/pull/41))
  - register the BitIntAtlasEntry for the tests ([ipfs/go-ipld-cbor#43](https://github.com/ipfs/go-ipld-cbor/pull/43))
  - attempt to allocate a bit less ([ipfs/go-ipld-cbor#45](https://github.com/ipfs/go-ipld-cbor/pull/45))

github.com/ipfs/go-ipfs-cmds
  - check if we can decode an error before trying ([ipfs/go-ipfs-cmds#108](https://github.com/ipfs/go-ipfs-cmds/pull/108))
  - fix(option): print error message for error timeout option ([ipfs/go-ipfs-cmds#118](https://github.com/ipfs/go-ipfs-cmds/pull/118))
  - Create Jenkinsfile ([ipfs/go-ipfs-cmds#89](https://github.com/ipfs/go-ipfs-cmds/pull/89))
  - fix(add): refer to ipfs issue #5456 ([ipfs/go-ipfs-cmds#121](https://github.com/ipfs/go-ipfs-cmds/pull/121))
  - commands refactor 2.0 ([ipfs/go-ipfs-cmds#112](https://github.com/ipfs/go-ipfs-cmds/pull/112))
  - always assign keks to review new PRs ([ipfs/go-ipfs-cmds#123](https://github.com/ipfs/go-ipfs-cmds/pull/123))
  - extract go-ipfs-files ([ipfs/go-ipfs-cmds#125](https://github.com/ipfs/go-ipfs-cmds/pull/125))
  - split the value encoder and the error encoder ([ipfs/go-ipfs-cmds#128](https://github.com/ipfs/go-ipfs-cmds/pull/128))

github.com/ipfs/go-ipfs-cmdkit
  - all: gofmt ([ipfs/go-ipfs-cmdkit#22](https://github.com/ipfs/go-ipfs-cmdkit/pull/22))
  - add standard ci scripts ([ipfs/go-ipfs-cmdkit#23](https://github.com/ipfs/go-ipfs-cmdkit/pull/23))
  - only count size for regular files ([ipfs/go-ipfs-cmdkit#25](https://github.com/ipfs/go-ipfs-cmdkit/pull/25))
  - Create Jenkinsfile ([ipfs/go-ipfs-cmdkit#16](https://github.com/ipfs/go-ipfs-cmdkit/pull/16))
  - Feat: add WebFile File implementation. ([ipfs/go-ipfs-cmdkit#26](https://github.com/ipfs/go-ipfs-cmdkit/pull/26))
  - feat(type): fix issue #28 ([ipfs/go-ipfs-cmdkit#29](https://github.com/ipfs/go-ipfs-cmdkit/pull/29))
  - Extract files package ([ipfs/go-ipfs-cmdkit#31](https://github.com/ipfs/go-ipfs-cmdkit/pull/31))

github.com/ipfs/go-ds-badger
  - update protobuf ([ipfs/go-ds-badger#26](https://github.com/ipfs/go-ds-badger/pull/26))
  - exported type datastore => Datastore ([ipfs/go-ds-badger#1](https://github.com/ipfs/go-ds-badger/pull/1))
  - using exported Datastore type ([ipfs/go-ds-badger#2](https://github.com/ipfs/go-ds-badger/pull/2))
  - exported type datastore => Datastore ([ipfs/go-ds-badger#28](https://github.com/ipfs/go-ds-badger/pull/28))
  - Implement new TxDatastore and Txn interfaces ([ipfs/go-ds-badger#27](https://github.com/ipfs/go-ds-badger/pull/27))
  - Avoid discarding transaction too early in queries ([ipfs/go-ds-badger#31](https://github.com/ipfs/go-ds-badger/pull/31))
  - Ability to get entry expirations ([ipfs/go-ds-badger#32](https://github.com/ipfs/go-ds-badger/pull/32))
  - Update badger to 2.8.0 ([ipfs/go-ds-badger#33](https://github.com/ipfs/go-ds-badger/pull/33))
  - ds.NewTransaction() now returns an error parameter ([ipfs/go-ds-badger#36](https://github.com/ipfs/go-ds-badger/pull/36))
  - make has faster ([ipfs/go-ds-badger#37](https://github.com/ipfs/go-ds-badger/pull/37))
  - Implement GetSize and update badger ([ipfs/go-ds-badger#38](https://github.com/ipfs/go-ds-badger/pull/38))

github.com/ipfs/go-ipfs-addr
  - Remove dependency on libp2p-circuit ([ipfs/go-ipfs-addr#7](https://github.com/ipfs/go-ipfs-addr/pull/7))

github.com/ipfs/go-ipfs-chunker
  - return err when rabin min less than 16 ([ipfs/go-ipfs-chunker#3](https://github.com/ipfs/go-ipfs-chunker/pull/3))
  - switch to go-buffer-pool ([ipfs/go-ipfs-chunker#8](https://github.com/ipfs/go-ipfs-chunker/pull/8))
  - fix size-0 chunker bug ([ipfs/go-ipfs-chunker#9](https://github.com/ipfs/go-ipfs-chunker/pull/9))

github.com/ipfs/go-ipfs-routing
  - update protobuf ([ipfs/go-ipfs-routing#8](https://github.com/ipfs/go-ipfs-routing/pull/8))
  - Implement SearchValue ([ipfs/go-ipfs-routing#12](https://github.com/ipfs/go-ipfs-routing/pull/12))

github.com/ipfs/go-ipfs-blockstore
  - blockstore: Adding Stat method to map from Cid to BlockSize ([ipfs/go-ipfs-blockstore#5](https://github.com/ipfs/go-ipfs-blockstore/pull/5))
  - correctly convert the datastore not found errors ([ipfs/go-ipfs-blockstore#10](https://github.com/ipfs/go-ipfs-blockstore/pull/10))
  - Fix typo: Change 'should not' to 'should' ([ipfs/go-ipfs-blockstore#14](https://github.com/ipfs/go-ipfs-blockstore/pull/14))
  - fix test race condition ([ipfs/go-ipfs-blockstore#9](https://github.com/ipfs/go-ipfs-blockstore/pull/9))
  - make arccache.GetSize return ErrNotFound when not found ([ipfs/go-ipfs-blockstore#16](https://github.com/ipfs/go-ipfs-blockstore/pull/16))
  - use datastore.GetSize ([ipfs/go-ipfs-blockstore#17](https://github.com/ipfs/go-ipfs-blockstore/pull/17))

github.com/ipfs/go-ipns
  - update gogo protobuf ([ipfs/go-ipns#16](https://github.com/ipfs/go-ipns/pull/16))
  - use new ExtractPublicKey signature ([ipfs/go-ipns#17](https://github.com/ipfs/go-ipns/pull/17))

github.com/ipfs/go-bitswap
  - update gogo protobuf ([ipfs/go-bitswap#2](https://github.com/ipfs/go-bitswap/pull/2))
  - ci: add jenkins ([ipfs/go-bitswap#9](https://github.com/ipfs/go-bitswap/pull/9))
  - bitswap: Bitswap now sends multiple blocks per message ([ipfs/go-bitswap#5](https://github.com/ipfs/go-bitswap/pull/5))
  - reduce allocations ([ipfs/go-bitswap#12](https://github.com/ipfs/go-bitswap/pull/12))
  - buffer writes ([ipfs/go-bitswap#15](https://github.com/ipfs/go-bitswap/pull/15))
  - delay finding providers ([ipfs/go-bitswap#17](https://github.com/ipfs/go-bitswap/pull/17))
github.com/ipfs/go-blockservice
  - Avoid allocating a session unless we need it ([ipfs/go-blockservice#6](https://github.com/ipfs/go-blockservice/pull/6))

github.com/ipfs/go-cidutil
  - add a utility method for sorting CID slices ([ipfs/go-cidutil#5](https://github.com/ipfs/go-cidutil/pull/5))

github.com/ipfs/go-ipfs-config
  - Add pubsub configuration options ([ipfs/go-ipfs-config#3](https://github.com/ipfs/go-ipfs-config/pull/3))
  - add QUIC experiment ([ipfs/go-ipfs-config#4](https://github.com/ipfs/go-ipfs-config/pull/4))
  - Add Gateway.APICommands for /api allowlists ([ipfs/go-ipfs-config#10](https://github.com/ipfs/go-ipfs-config/pull/10))
  - allow multiple API/Gateway addresses ([ipfs/go-ipfs-config#11](https://github.com/ipfs/go-ipfs-config/pull/11))
  - Fix handling of null strings ([ipfs/go-ipfs-config#12](https://github.com/ipfs/go-ipfs-config/pull/12))
  - add experiment for p2p http proxy ([ipfs/go-ipfs-config#13](https://github.com/ipfs/go-ipfs-config/pull/13))
  - add message signing config options ([ipfs/go-ipfs-config#18](https://github.com/ipfs/go-ipfs-config/pull/18))

github.com/ipfs/go-merkledag
  - Add FetchGraphWithDepthLimit to specify depth-limited graph fetching. ([ipfs/go-merkledag#2](https://github.com/ipfs/go-merkledag/pull/2))
  - update gogo protobuf ([ipfs/go-merkledag#4](https://github.com/ipfs/go-merkledag/pull/4))
  - Update to use new Builder interface for creating CIDs. ([ipfs/go-merkledag#6](https://github.com/ipfs/go-merkledag/pull/6))
  - perf: avoid allocations when filtering nodes ([ipfs/go-merkledag#11](https://github.com/ipfs/go-merkledag/pull/11))

github.com/ipfs/go-mfs
  - fix(unixfs): issue #6 ([ipfs/go-mfs#7](https://github.com/ipfs/go-mfs/pull/7))
  - fix(type): issue #13 ([ipfs/go-mfs#14](https://github.com/ipfs/go-mfs/pull/14))

github.com/ipfs/go-path
  - fix: don't dag.Get in ResolveToLastNode when not needed ([ipfs/go-path#1](https://github.com/ipfs/go-path/pull/1))

github.com/ipfs/go-unixfs
  - update gogo protobuf ([ipfs/go-unixfs#6](https://github.com/ipfs/go-unixfs/pull/6))
  - Update to use new Builder interface for creating CIDs. ([ipfs/go-unixfs#7](https://github.com/ipfs/go-unixfs/pull/7))
  - nit: make dagTruncate a method on DagModifier ([ipfs/go-unixfs#13](https://github.com/ipfs/go-unixfs/pull/13))
  - fix(fsnode): issue #17 ([ipfs/go-unixfs#18](https://github.com/ipfs/go-unixfs/pull/18))
  - Use EnumerateChildrenAsync in for enumerating HAMT links ([ipfs/go-unixfs#19](https://github.com/ipfs/go-unixfs/pull/19))

## 0.4.17 2018-07-27

Ipfs 0.4.17 is a quick release to fix a major performance regression in bitswap
(mostly affecting go-ipfs -> js-ipfs transfers). However, while motivated by
this fix, this release contains a few other goodies that will excite some users.

The headline feature in this release is [urlstore][] support. Urlstore is a
generalization of the filestore backend that can fetch file blocks from remote
URLs on-demand instead of storing them in the local datastore.

Additionally, we've added support for extracting inline blocks from CIDs (blocks
inlined into CIDs using the identity hash function). However, go-ipfs won't yet
*create* such CIDs so you're unlikely to see any in the wild.

[urlstore]: https://github.com/ipfs/go-ipfs/blob/master/docs/experimental-features.md#ipfs-urlstore

Features:

* URLStore ([ipfs/go-ipfs#4896](https://github.com/ipfs/go-ipfs/pull/4896))
* Add trickle-dag support to the urlstore ([ipfs/go-ipfs#5245](https://github.com/ipfs/go-ipfs/pull/5245)).
* Allow specifying how the data field in the `object get` is encoded ([ipfs/go-ipfs#5139](https://github.com/ipfs/go-ipfs/pull/5139))
* Add a `-U` flag to `files ls` to disable sorting ([ipfs/go-ipfs#5219](https://github.com/ipfs/go-ipfs/pull/5219))
* Add an efficient `--size-only` flag to the `repo stat` ([ipfs/go-ipfs#5010](https://github.com/ipfs/go-ipfs/pull/5010))
* Inline blocks in CIDs ([ipfs/go-ipfs#5117](https://github.com/ipfs/go-ipfs/pull/5117))

Changes/Fixes:

* Make `ipfs files ls -l` correctly report the hash and size of files ([ipfs/go-ipfs#5045](https://github.com/ipfs/go-ipfs/pull/5045))
* Fix sorting of `files ls` ([ipfs/go-ipfs#5219](https://github.com/ipfs/go-ipfs/pull/5219))
* Improve prefetching in `ipfs cat` and related commands ([ipfs/go-ipfs#5162](https://github.com/ipfs/go-ipfs/pull/5162))
* Better error message when `ipfs cp` fails ([ipfs/go-ipfs#5218](https://github.com/ipfs/go-ipfs/pull/5218))
* Don't wait for the peer to close it's end of a bitswap stream before considering the block "sent" ([ipfs/go-ipfs#5258](https://github.com/ipfs/go-ipfs/pull/5258))
* Fix resolving links in sharded directories via the gateway ([ipfs/go-ipfs#5271](https://github.com/ipfs/go-ipfs/pull/5271))
* Fix building when there's a space in the current directory ([ipfs/go-ipfs#5261](https://github.com/ipfs/go-ipfs/pull/5261))

Documentation:

* Improve documentation about the bloomfilter config options ([ipfs/go-ipfs#4924](https://github.com/ipfs/go-ipfs/pull/4924))

General refactorings and internal bug fixes:

* Remove the `Offset()` method from the DAGReader ([ipfs/go-ipfs#5190](https://github.com/ipfs/go-ipfs/pull/5190))
* Fix TestLargeWriteChunks seek behavior ([ipfs/go-ipfs#5276](https://github.com/ipfs/go-ipfs/pull/5276))
* Add a build tag to disable dynamic plugins ([ipfs/go-ipfs#5274](https://github.com/ipfs/go-ipfs/pull/5274))
* Use FSNode instead of the Protobuf structure in PBDagReader ([ipfs/go-ipfs#5189](https://github.com/ipfs/go-ipfs/pull/5189))
* Remove support for non-directory MFS roots ([ipfs/go-ipfs#5170](https://github.com/ipfs/go-ipfs/pull/5170))
* Remove `UnixfsNode` from the balanced builder ([ipfs/go-ipfs#5118](https://github.com/ipfs/go-ipfs/pull/5118))
* Fix truncating files (internal) when already at the correct size ([ipfs/go-ipfs#5253](https://github.com/ipfs/go-ipfs/pull/5253))
* Fix `dagTruncate` (internal) to preserve the node type ([ipfs/go-ipfs#5216](https://github.com/ipfs/go-ipfs/pull/5216))
* Add an internal interface for unixfs directories ([ipfs/go-ipfs#5160](https://github.com/ipfs/go-ipfs/pull/5160))
* Refactor the CoreAPI path types and interfaces ([ipfs/go-ipfs#4672](https://github.com/ipfs/go-ipfs/pull/4672))
* Refactor `precalcNextBuf` in the dag reader ([ipfs/go-ipfs#5237](https://github.com/ipfs/go-ipfs/pull/5237))
* Update a bunch of dependencies that haven't been updated for a while ([ipfs/go-ipfs#5268](https://github.com/ipfs/go-ipfs/pull/5268))

## 0.4.16 2018-07-13

Ipfs 0.4.16 is a fairly small release in terms of changes to the ipfs codebase,
but it contains a huge amount of changes and improvements from the libraries we
depend on, notably libp2p.

This release includes small a repo migration to account for some changes to the
DHT. It should only take a second to run but, depending on your configuration,
you may need to run it manually.

You can run a migration by either:

1. Selecting "Yes" when the daemon prompts you to migrate.
2. Running the daemon with the `--migrate=true` flag.
3. Manually [running](https://github.com/ipfs/fs-repo-migrations/blob/master/run.md#running-repo-migrations) the migration.

### Libp2p

This version of ipfs contains the changes made in libp2p from v5.0.14 through
v6.0.5. In that time, we have made significant changes to the codebase to allow
for easier integration of future transports and modules along with the usual
performance and reliability improvements. You can find many of these
improvements in the libp2p 6.0 [release blog
post](https://ipfs.io/blog/39-go-libp2p-6-0-0/).

The primary motivation for this refactor was adding support for network
transports like QUIC that have built-in support for encryption, authentication,
and stream multiplexing. It will also allow us to plug-in new security
transports (like TLS) without hard-coding them.

For example, our [QUIC
transport](https://github.com/libp2p/go-libp2p-quic-transport) currently works,
and can be plugged into libp2p manually (though note that it is still
experimental, as the upstream spec is still in flux). Further work is needed to
make enabling this inside ipfs easy and not require recompilation.

On the user-visible side of things, we've improved our dialing logic and
timeouts. We now abort dials to local subnets after 5 seconds and abort all
dials if the TCP handshake takes longer than 5 seconds. This should
significantly improve performance in some cases as we limit the number of
concurrent dials and slow dials to non-responsive peers have been known to clog
the dialer, blocking dials to reachable peers. Importantly, this should improve
DHT performance as it tends to spend a disproportional amount of time connecting
to peers.

We have also made a few noticeable changes to the DHT: we've significantly
improved the chances of finding a value on the DHT, tightened up some of our
validation logic, and fixed some issues that should reduce traffic to nodes
running in dhtclient mode over time.

Of these, the first one will likely see the most impact. In the past, when
putting a value (e.g., an IPNS entry) into the DHT, we'd try to put the value to
K peers (where K for us is 20). However, we'd often fail to connect to many of
these peers so we'd end up putting the value to significantly fewer than K
peers. We now try to put the value to the K peers we can actually connect to.

Finally, we've fixed JavaScript interoperability in go-multiplex, the one stream
muxer that both go-libp2p and js-libp2p implement. This should significantly
improve go-libp2p and js-libp2p interoperability.

### Multiformats

We are also changing the way that people write 'ipfs' multiaddrs. Currently,
ipfs multiaddrs look something like
`/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ`.
However, calling them 'ipfs' multiaddrs is a bit misleading, as this is actually
the multiaddr of a libp2p peer that happens to run ipfs. Other protocols built
on libp2p right now still have to use multiaddrs that say 'ipfs', even if they
have nothing to do with ipfs. Therefore, we are renaming them to 'p2p'
multiaddrs. Moving forward, these addresses will be written as:
`/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ`.

This release adds support for *parsing* both types of addresses (`.../ipfs/...`
and `.../p2p/...`) into the same network format, and the network format is
remaining exactly the same. A future release will have the ipfs daemon switch to
*printing* out addresses this way once a large enough portion of the network
has upgraded.

N.B., these addresses are *not* related to IPFS *file* names (`/ipfs/Qm...`).
Disambiguating the two was yet another motivation to switch the protocol name to
`/p2p/`.

### IPFS

On the ipfs side of things, we've started embedding public keys inside IPNS
records and have enabled the Git plugin by default.

Embedding public keys inside IPNS records allows lookups to be faster as we only
need to fetch the record itself (and not the public key separately). It also
fixes an issue where DHT peers wouldn't store a record for a peer if they didn't
have their public key already. Combined with some of the DHT and dialing fixes,
this should improve the performance of IPNS (once a majority of the network
updates).

Second, our public builds now include the Git plugin (in past builds, you could
add it yourself, but doing so was not easy). With this, ipfs can ingest and
operate over Git repositories and commit graphs directly. For more information
on this, see [the go-ipld-git repo](https://github.com/ipfs/go-ipld-git).

Finally, we've included many smaller bugfixes, refactorings, improved
documentation, and a good bit more. For the full details, see the changelog
below.

## 0.4.16-rc3 2018-07-09
- Bugfixes
  - Fix dht commands when ipns over pubsub is enabled ([ipfs/go-ipfs#5200](https://github.com/ipfs/go-ipfs/pull/5200))
  - Fix content routing when ipns over pubsub is enabled ([ipfs/go-ipfs#5200](https://github.com/ipfs/go-ipfs/pull/5200))
  - Correctly handle multi-hop dnslink resolution ([ipfs/go-ipfs#5202](https://github.com/ipfs/go-ipfs/pull/5202))

## 0.4.16-rc2 2018-07-05
- Bugfixes
  - Fix usage of file name vs path name in adder ([ipfs/go-ipfs#5167](https://github.com/ipfs/go-ipfs/pull/5167))
  - Fix `ipfs update` working with migrations ([ipfs/go-ipfs#5194](https://github.com/ipfs/go-ipfs/pull/5194))
- Documentation
  - Grammar fix in fuse docs ([ipfs/go-ipfs#5164](https://github.com/ipfs/go-ipfs/pull/5164))

## 0.4.16-rc1 2018-06-27
- Features
  - Embed public keys inside ipns records, use for validation ([ipfs/go-ipfs#5079](https://github.com/ipfs/go-ipfs/pull/5079))
  - Preload git plugin by default ([ipfs/go-ipfs#4991](https://github.com/ipfs/go-ipfs/pull/4991))
- Improvements
  - Only resolve dnslinks once in the gateway ([ipfs/go-ipfs#4977](https://github.com/ipfs/go-ipfs/pull/4977))
  - Libp2p transport refactor update ([ipfs/go-ipfs#4817](https://github.com/ipfs/go-ipfs/pull/4817))
  - Improve swarm connect/disconnect commands ([ipfs/go-ipfs#5107](https://github.com/ipfs/go-ipfs/pull/5107))
- Documentation
  - Fix typo of sudo install command ([ipfs/go-ipfs#5001](https://github.com/ipfs/go-ipfs/pull/5001))
  - Fix experimental features Table of Contents ([ipfs/go-ipfs#4976](https://github.com/ipfs/go-ipfs/pull/4976))
  - Fix link to systemd init scripts in the README ([ipfs/go-ipfs#4968](https://github.com/ipfs/go-ipfs/pull/4968))
  - Add package overview comments to coreapi ([ipfs/go-ipfs#5108](https://github.com/ipfs/go-ipfs/pull/5108))
  - Add README to docs folder ([ipfs/go-ipfs#5095](https://github.com/ipfs/go-ipfs/pull/5095))
  - Add system requirements to README ([ipfs/go-ipfs#5137](https://github.com/ipfs/go-ipfs/pull/5137))
- Bugfixes
  - Fix goroutine leak in pin verify ([ipfs/go-ipfs#5011](https://github.com/ipfs/go-ipfs/pull/5011))
  - Fix commit string in version ([ipfs/go-ipfs#4982](https://github.com/ipfs/go-ipfs/pull/4982))
  - Fix `key rename` command output error ([ipfs/go-ipfs#4962](https://github.com/ipfs/go-ipfs/pull/4962))
  - Report error source when failing to construct private network ([ipfs/go-ipfs#4952](https://github.com/ipfs/go-ipfs/pull/4952))
  - Fix build on DragonFlyBSD ([ipfs/go-ipfs#5031](https://github.com/ipfs/go-ipfs/pull/5031))
  - Fix goroutine leak in dag put ([ipfs/go-ipfs#5016](https://github.com/ipfs/go-ipfs/pull/5016))
  - Fix goroutine leaks in refs.go ([ipfs/go-ipfs#5018](https://github.com/ipfs/go-ipfs/pull/5018))
  - Fix panic, Don't handle errors with fallthrough ([ipfs/go-ipfs#5072](https://github.com/ipfs/go-ipfs/pull/5072))
  - Fix how filestore is hooked up with caching ([ipfs/go-ipfs#5122](https://github.com/ipfs/go-ipfs/pull/5122))
  - Add record validation to offline routing ([ipfs/go-ipfs#5116](https://github.com/ipfs/go-ipfs/pull/5116))
  - Fix `ipfs update` working with migrations ([ipfs/go-ipfs#5194](https://github.com/ipfs/go-ipfs/pull/5194))
- General Changes and Refactorings
  - Remove leftover bits of dead code ([ipfs/go-ipfs#5022](https://github.com/ipfs/go-ipfs/pull/5022))
  - Remove fuse platform build constraints ([ipfs/go-ipfs#5033](https://github.com/ipfs/go-ipfs/pull/5033))
  - Warning when legacy NoSync setting is set ([ipfs/go-ipfs#5036](https://github.com/ipfs/go-ipfs/pull/5036))
  - Clean up and refactor namesys module ([ipfs/go-ipfs#5007](https://github.com/ipfs/go-ipfs/pull/5007))
  - When raw-leaves are used for empty files use 'Raw' nodes ([ipfs/go-ipfs#4693](https://github.com/ipfs/go-ipfs/pull/4693))
  - Update dist_root in build scripts ([ipfs/go-ipfs#5093](https://github.com/ipfs/go-ipfs/pull/5093))
  - Integrate `pb.Data` into `FSNode` to avoid duplicating fields ([ipfs/go-ipfs#5098](https://github.com/ipfs/go-ipfs/pull/5098))
  - Reduce log level when we can't republish ([ipfs/go-ipfs#5091](https://github.com/ipfs/go-ipfs/pull/5091))
  - Extract ipns record logic to go-ipns ([ipfs/go-ipfs#5124](https://github.com/ipfs/go-ipfs/pull/5124))
- Testing
  - Collect test times for sharness ([ipfs/go-ipfs#4959](https://github.com/ipfs/go-ipfs/pull/4959))
  - Fix sharness iptb connect timeout ([ipfs/go-ipfs#4966](https://github.com/ipfs/go-ipfs/pull/4966))
  - Add more timeouts to the jenkins pipeline ([ipfs/go-ipfs#4958](https://github.com/ipfs/go-ipfs/pull/4958))
  - Use go 1.10 on jenkins ([ipfs/go-ipfs#5009](https://github.com/ipfs/go-ipfs/pull/5009))
  - Speed up multinode sharness test ([ipfs/go-ipfs#4967](https://github.com/ipfs/go-ipfs/pull/4967))
  - Print out iptb logs on iptb test failure (for debugging CI) ([ipfs/go-ipfs#5069](https://github.com/ipfs/go-ipfs/pull/5069))
  - Disable the MacOS tests in jenkins ([ipfs/go-ipfs#5119](https://github.com/ipfs/go-ipfs/pull/5119))
  - Make republisher test robust against timing issues ([ipfs/go-ipfs#5125](https://github.com/ipfs/go-ipfs/pull/5125))
  - Archive sharness trash dirs in jenkins ([ipfs/go-ipfs#5071](https://github.com/ipfs/go-ipfs/pull/5071))
  - Fixup DHT sharness tests ([ipfs/go-ipfs#5114](https://github.com/ipfs/go-ipfs/pull/5114))
- Dependencies
  - Update go-ipld-git to fix mergetag resolving ([ipfs/go-ipfs#4988](https://github.com/ipfs/go-ipfs/pull/4988))
  - Fix duplicate /x/sys imports ([ipfs/go-ipfs#5068](https://github.com/ipfs/go-ipfs/pull/5068))
  - Update stream multiplexers ([ipfs/go-ipfs#5075](https://github.com/ipfs/go-ipfs/pull/5075))
  - Update dependencies: go-log, sys, go-crypto ([ipfs/go-ipfs#5100](https://github.com/ipfs/go-ipfs/pull/5100))
  - Explicitly import go-multiaddr-dns in config/bootstrap_peers ([ipfs/go-ipfs#5144](https://github.com/ipfs/go-ipfs/pull/5144))
  - Gx update with dht and dialing improvements ([ipfs/go-ipfs#5158](https://github.com/ipfs/go-ipfs/pull/5158))

## 0.4.15 2018-05-09

This release is significantly smaller than the last as much of the work on
improving our datastores, and other libraries libp2p has yet to be merged.
However, it still includes many welcome improvements.

As with 0.4.12 and 0.4.14 (0.4.13 was a patch), this release has a negative
diff-stat. Unfortunately, much of this code isn't actually going away but at
least it's being moved out into separate repositories.

Much of the work that made it into this release is under the hood. We've cleaned
up some code, extracted several packages into their own repositories, and made
some long neglected optimizations (e.g., handling of sharded directories).
Additionally, this release includes a bunch of tests for our CLI commands that
should help us avoid some of the issues we've seen in the past few releases.

More visibly, thanks to @djdv's efforts, this release includes some significant
Windows improvements (with more on the way). Specifically, this release includes
better handling of repo lockfiles (no more `ipfs repo fsck`), stdin command-line
support, and, last but not least, IPFS no longer writes random files with scary
garbage in the drive root. To read more about future windows improvements, take
a look at this [blog post](https://blog.ipfs.io/36-a-look-at-windows/).

To better support low-power devices, we've added a low-power config profile.
This can be enabled when initializing a repo by running `ipfs init` with the
`--profile=lowpower` flag or later by running `ipfs config profile apply lowpower`.

Finally, with this release we have begun distributing self-contained source
archives of go-ipfs and its dependencies. This should be a welcome improvement
for both packagers and those living in countries with harmonized internet
access.

- Features
  - Add options for record count and timeout for resolving DHT paths ([ipfs/go-ipfs#4733](https://github.com/ipfs/go-ipfs/pull/4733))
  - Add low power init profile ([ipfs/go-ipfs#4154](https://github.com/ipfs/go-ipfs/pull/4154))
  - Add Opentracing plugin support ([ipfs/go-ipfs#4506](https://github.com/ipfs/go-ipfs/pull/4506))
  - Add make target to build source tarballs ([ipfs/go-ipfs#4920](https://github.com/ipfs/go-ipfs/pull/4920))

- Improvements
  - Add BlockedFetched/Added/Removed events to Blockservice ([ipfs/go-ipfs#4649](https://github.com/ipfs/go-ipfs/pull/4649))
  - Improve performance of HAMT code ([ipfs/go-ipfs#4889](https://github.com/ipfs/go-ipfs/pull/4889))
  - Avoid unnecessarily resolving child nodes when listing a sharded directory ([ipfs/go-ipfs#4884](https://github.com/ipfs/go-ipfs/pull/4884))
  - Tar writer now supports sharded ipfs directories ([ipfs/go-ipfs#4873](https://github.com/ipfs/go-ipfs/pull/4873))
  - Infer type from CID when possible in `ipfs ls` ([ipfs/go-ipfs#4890](https://github.com/ipfs/go-ipfs/pull/4890))
  - Deduplicate keys in GetMany ([ipfs/go-ipfs#4888](https://github.com/ipfs/go-ipfs/pull/4888))

- Documentation
  - Fix spelling of retrieval ([ipfs/go-ipfs#4819](https://github.com/ipfs/go-ipfs/pull/4819))
  - Update broken links ([ipfs/go-ipfs#4798](https://github.com/ipfs/go-ipfs/pull/4798))
  - Remove roadmap.md ([ipfs/go-ipfs#4834](https://github.com/ipfs/go-ipfs/pull/4834))
  - Remove link to IPFS paper in contribute.md ([ipfs/go-ipfs#4812](https://github.com/ipfs/go-ipfs/pull/4812))
  - Fix broken todo link in readme.md ([ipfs/go-ipfs#4865](https://github.com/ipfs/go-ipfs/pull/4865))
  - Document ipns pubsub ([ipfs/go-ipfs#4903](https://github.com/ipfs/go-ipfs/pull/4903))
  - Fix missing profile docs ([ipfs/go-ipfs#4846](https://github.com/ipfs/go-ipfs/pull/4846))
  - Fix a few typos ([ipfs/go-ipfs#4835](https://github.com/ipfs/go-ipfs/pull/4835))
  - Fix typo in fsrepo error message ([ipfs/go-ipfs#4933](https://github.com/ipfs/go-ipfs/pull/4933))
  - Remove go-ipfs version from issue template ([ipfs/go-ipfs#4943](https://github.com/ipfs/go-ipfs/pull/4943))
  - Add docs for --profile=lowpower ([ipfs/go-ipfs#4970](https://github.com/ipfs/go-ipfs/pull/4970))
  - Improve Windows build documentation ([ipfs/go-ipfs#4691](https://github.com/ipfs/go-ipfs/pull/4691))

- Bugfixes
  - Check CIDs in base case when diffing nodes ([ipfs/go-ipfs#4767](https://github.com/ipfs/go-ipfs/pull/4767))
  - Support for CIDv1 with custom mhtype in `ipfs block put` ([ipfs/go-ipfs#4563](https://github.com/ipfs/go-ipfs/pull/4563))
  - Clean path in DagArchive ([ipfs/go-ipfs#4743](https://github.com/ipfs/go-ipfs/pull/4743))
  - Set the prefix for MFS root in `ipfs add --hash-only` ([ipfs/go-ipfs#4755](https://github.com/ipfs/go-ipfs/pull/4755))
  - Fix get output path ([ipfs/go-ipfs#4809](https://github.com/ipfs/go-ipfs/pull/4809))
  - Fix incorrect Read calls ([ipfs/go-ipfs#4792](https://github.com/ipfs/go-ipfs/pull/4792))
  - Use prefix in bootstrapWritePeers ([ipfs/go-ipfs#4832](https://github.com/ipfs/go-ipfs/pull/4832))
  - Fix mfs Directory.Path not working ([ipfs/go-ipfs#4844](https://github.com/ipfs/go-ipfs/pull/4844))
  - Remove header in `ipfs stats bw` if not polling ([ipfs/go-ipfs#4856](https://github.com/ipfs/go-ipfs/pull/4856))
  - Match Go's GOPATH defaults behaviour in build scripts ([ipfs/go-ipfs#4678](https://github.com/ipfs/go-ipfs/pull/4678))
  - Fix default-net profile not reverting bootstrap config ([ipfs/go-ipfs#4845](https://github.com/ipfs/go-ipfs/pull/4845))
  - Fix excess goroutines in bitswap caused by insecure CIDs ([ipfs/go-ipfs#4946](https://github.com/ipfs/go-ipfs/pull/4946))

- General Changes and Refactorings
  - Refactor trickle DAG builder ([ipfs/go-ipfs#4730](https://github.com/ipfs/go-ipfs/pull/4730))
  - Split the coreapi interface into multiple files ([ipfs/go-ipfs#4802](https://github.com/ipfs/go-ipfs/pull/4802))
  - Make `ipfs init` command use new cmds lib ([ipfs/go-ipfs#4732](https://github.com/ipfs/go-ipfs/pull/4732))
  - Extract thirdparty/tar package ([ipfs/go-ipfs#4857](https://github.com/ipfs/go-ipfs/pull/4857))
  - Reduce log level when for disconnected peers to info ([ipfs/go-ipfs#4811](https://github.com/ipfs/go-ipfs/pull/4811))
  - Only visit nodes in EnumerateChildrenAsync when asked ([ipfs/go-ipfs#4885](https://github.com/ipfs/go-ipfs/pull/4885))
  - Refactor coreapi options ([ipfs/go-ipfs#4807](https://github.com/ipfs/go-ipfs/pull/4807))
  - Fix error style for most errors ([ipfs/go-ipfs#4829](https://github.com/ipfs/go-ipfs/pull/4829))
  - Ensure `--help` always works, even with /dev/null stdin ([ipfs/go-ipfs#4849](https://github.com/ipfs/go-ipfs/pull/4849))
  - Deduplicate AddNodeLinkClean into AddNodeLink ([ipfs/go-ipfs#4940](https://github.com/ipfs/go-ipfs/pull/4940))
  - Remove some dead code ([ipfs/go-ipfs#4833](https://github.com/ipfs/go-ipfs/pull/4833))
  - Remove unused imports ([ipfs/go-ipfs#4955](https://github.com/ipfs/go-ipfs/pull/4955))
  - Fix go vet warnings ([ipfs/go-ipfs#4859](https://github.com/ipfs/go-ipfs/pull/4859))

- Testing
  - Generate JUnit test reports for sharness tests ([ipfs/go-ipfs#4530](https://github.com/ipfs/go-ipfs/pull/4530))
  - Fix t0063-daemon-init.sh by adding test profile to daemon ([ipfs/go-ipfs#4816](https://github.com/ipfs/go-ipfs/pull/4816))
  - Remove circular dependencies in merkledag package tests ([ipfs/go-ipfs#4704](https://github.com/ipfs/go-ipfs/pull/4704))
  - Check that all the commands fail when passed a bad flag ([ipfs/go-ipfs#4848](https://github.com/ipfs/go-ipfs/pull/4848))
  - Allow for some small margin of code coverage dropping on commit ([ipfs/go-ipfs#4867](https://github.com/ipfs/go-ipfs/pull/4867))
  - Add confirmation to archive-branches script ([ipfs/go-ipfs#4797](https://github.com/ipfs/go-ipfs/pull/4797))

- Dependencies
  - Update lock package ([ipfs/go-ipfs#4855](https://github.com/ipfs/go-ipfs/pull/4855))
  - Update to latest go-datastore. Remove thirdparty/datastore2 ([ipfs/go-ipfs#4742](https://github.com/ipfs/go-ipfs/pull/4742))
  - Extract fs lock into go-fs-lock ([ipfs/go-ipfs#4631](https://github.com/ipfs/go-ipfs/pull/4631))
  - Extract: exchange/interface.go, blocks/blocksutil, exchange/offline ([ipfs/go-ipfs#4912](https://github.com/ipfs/go-ipfs/pull/4912))
  - Remove unused lock dep ([ipfs/go-ipfs#4971](https://github.com/ipfs/go-ipfs/pull/4971))
  - Update iptb ([ipfs/go-ipfs#4965](https://github.com/ipfs/go-ipfs/pull/4965))
  - Update go-ipfs-cmds to fix stdin on windows ([ipfs/go-ipfs#4975](https://github.com/ipfs/go-ipfs/pull/4975))
  - Update go-ds-flatfs to fix windows corruption issue ([ipfs/go-ipfs#4872](https://github.com/ipfs/go-ipfs/pull/4872))

## 0.4.14 2018-03-22

Ipfs 0.4.14 is a big release with a large number of improvements and bugfixes.
It is also the first release of 2018, and our first release in over three
months. The release took longer than expected due to our refactoring and
extracting of our commands library. This refactor had two stages.  The first
round of the refactor disentangled the commands code from core ipfs code,
allowing us to move it out into a [separate
repository](https://github.com/ipfs/go-ipfs-cmds).  The code was previously
very entangled with the go-ipfs codebase and not usable for other projects.
The second round of the refactor had the goal of fixing several major issues
around streaming outputs, progress bars, and error handling. It also paved the
way for us to more easily provide an API over other transports, such as
websockets and unix domain sockets.  It took a while to flush out all the kinks
on such a massive change.  We're pretty sure we've got most of them, but if you
notice anything weird, please let us know.

Beyond that, we've added a new experimental way to use IPNS. With the new
pubsub IPNS resolver and publisher, you can subscribe to updates of an IPNS
entry, and the owner can publish out changes in real time. With this, IPNS can
become nearly instantaneous. To make use of this, simply start your ipfs daemon
with the `--enable-namesys-pubsub` option, and all IPNS resolution and
publishing will use pubsub. Note that resolving an IPNS name via pubsub without
someone publishing it via pubsub will result in a fallback to using the DHT.
Please give this a try and let us know how it goes!

Memory and CPU usage should see a noticeable improvement in this release. We
have spent considerable time fixing excess memory usage throughout the codebase
and down into libp2p. Fixes in peer tracking, bitswap allocation, pinning, and
many other places have brought down both peak and average memory usage. An
upgraded hashing library, base58 encoding library, and improved allocation
patterns all contribute to overall lower CPU usage across the board. See the
full changelist below for more memory and CPU usage improvements.

This release also brings the beginning of the ipfs 'Core API'. Once finished,
the Core API will be the primary way to interact with go-ipfs using go. Both
embedded nodes and nodes accessed over the http API will have the same
interface. Stay tuned for future updates and documentation.

These are only a sampling of the changes that made it into this release, the
full list (almost 100 PRs!) is below.

Finally, I'd like to thank everyone who contributed to this release, whether
you're just contributing a typo fix or driving new features. We are really
grateful to everyone who has spent their their time pushing ipfs forward.

SECURITY NOTE:

This release of ipfs disallows the usage of insecure hash functions and
lengths. Ipfs does not create these insecure objects for any purpose, but it
did allow manually creating them and fetching them from other peers. If you
currently have objects using insecure hashes in your local ipfs repo, please
remove them before updating.

#### Changes from rc2 to rc3
- Fix bug in stdin argument parsing ([ipfs/go-ipfs#4827](https://github.com/ipfs/go-ipfs/pull/4827))
- Revert commands back to sending a single response ([ipfs/go-ipfs#4822](https://github.com/ipfs/go-ipfs/pull/4822))

#### Changes from rc1 to rc2
- Fix issue in ipfs get caused by go1.10 changes ([ipfs/go-ipfs#4790](https://github.com/ipfs/go-ipfs/pull/4790))

- Features
  - Pubsub IPNS Publisher and Resolver (experimental) ([ipfs/go-ipfs#4047](https://github.com/ipfs/go-ipfs/pull/4047))
  - Implement coreapi Dag interface ([ipfs/go-ipfs#4471](https://github.com/ipfs/go-ipfs/pull/4471))
  - Add --offset flag to ipfs cat ([ipfs/go-ipfs#4538](https://github.com/ipfs/go-ipfs/pull/4538))
  - Command to apply config profile after init ([ipfs/go-ipfs#4195](https://github.com/ipfs/go-ipfs/pull/4195))
  - Implement coreapi Name and Key interfaces ([ipfs/go-ipfs#4477](https://github.com/ipfs/go-ipfs/pull/4477))
  - Add --length flag to ipfs cat ([ipfs/go-ipfs#4553](https://github.com/ipfs/go-ipfs/pull/4553))
  - Implement coreapi Object interface ([ipfs/go-ipfs#4492](https://github.com/ipfs/go-ipfs/pull/4492))
  - Implement coreapi Block interface ([ipfs/go-ipfs#4548](https://github.com/ipfs/go-ipfs/pull/4548))
  - Implement coreapi Pin interface ([ipfs/go-ipfs#4575](https://github.com/ipfs/go-ipfs/pull/4575))
  - Add a --with-local flag to ipfs files stat ([ipfs/go-ipfs#4638](https://github.com/ipfs/go-ipfs/pull/4638))
  - Disallow usage of blocks with insecure hashes ([ipfs/go-ipfs#4751](https://github.com/ipfs/go-ipfs/pull/4751))
- Improvements
  - Add uuid to event logs ([ipfs/go-ipfs#4392](https://github.com/ipfs/go-ipfs/pull/4392))
  - Add --quiet flag to object put ([ipfs/go-ipfs#4411](https://github.com/ipfs/go-ipfs/pull/4411))
  - Pinning memory improvements and fixes ([ipfs/go-ipfs#4451](https://github.com/ipfs/go-ipfs/pull/4451))
  - Update WebUI version ([ipfs/go-ipfs#4449](https://github.com/ipfs/go-ipfs/pull/4449))
  - Check strong and weak ETag validator ([ipfs/go-ipfs#3983](https://github.com/ipfs/go-ipfs/pull/3983))
  - Improve and refactor FD limit handling ([ipfs/go-ipfs#3801](https://github.com/ipfs/go-ipfs/pull/3801))
  - Support linking to non-dagpb objects in ipfs object patch ([ipfs/go-ipfs#4460](https://github.com/ipfs/go-ipfs/pull/4460))
  - Improve allocation patterns of slices in bitswap ([ipfs/go-ipfs#4458](https://github.com/ipfs/go-ipfs/pull/4458))
  - Secio handshake now happens synchronously ([libp2p/go-libp2p-secio#25](https://github.com/libp2p/go-libp2p-secio/pull/25))
  - Don't block closing connections on pending writes ([libp2p/go-msgio#7](https://github.com/libp2p/go-msgio/pull/7))
  - Improve memory usage of multiaddr parsing ([multiformats/go-multiaddr#56](https://github.com/multiformats/go-multiaddr/pull/56))
  - Don't lock up 256KiB buffers when adding small files ([ipfs/go-ipfs#4508](https://github.com/ipfs/go-ipfs/pull/4508))
  - Clear out memory after reads from the dagreader ([ipfs/go-ipfs#4525](https://github.com/ipfs/go-ipfs/pull/4525))
  - Improve error handling in ipfs ping ([ipfs/go-ipfs#4546](https://github.com/ipfs/go-ipfs/pull/4546))
  - Allow install.sh to be run without being the script dir ([ipfs/go-ipfs#4547](https://github.com/ipfs/go-ipfs/pull/4547))
  - Much faster base58 encoding ([libp2p/go-libp2p-peer#24](https://github.com/libp2p/go-libp2p-peer/pull/24))
  - Use faster sha256 and blake2b libs ([multiformats/go-multihash#63](https://github.com/multiformats/go-multihash/pull/63))
  - Greatly improve peerstore memory usage ([libp2p/go-libp2p-peerstore#22](https://github.com/libp2p/go-libp2p-peerstore/pull/22))
  - Improve dht memory usage and peer tracking ([libp2p/go-libp2p-kad-dht#111](https://github.com/libp2p/go-libp2p-kad-dht/pull/111))
  - New libp2p metrics lib with lower overhead ([libp2p/go-libp2p-metrics#8](https://github.com/libp2p/go-libp2p-metrics/pull/8))
  - Fix memory leak that occurred when dialing many peers ([libp2p/go-libp2p-swarm#51](https://github.com/libp2p/go-libp2p-swarm/pull/51))
  - Wire up new dag interfaces to make sessions easier ([ipfs/go-ipfs#4641](https://github.com/ipfs/go-ipfs/pull/4641))
- Documentation
  - Correct StorageMax config description ([ipfs/go-ipfs#4388](https://github.com/ipfs/go-ipfs/pull/4388))
  - Add how to download IPFS with IPFS doc ([ipfs/go-ipfs#4390](https://github.com/ipfs/go-ipfs/pull/4390))
  - Document gx release checklist item ([ipfs/go-ipfs#4480](https://github.com/ipfs/go-ipfs/pull/4480))
  - Add some documentation to CoreAPI ([ipfs/go-ipfs#4493](https://github.com/ipfs/go-ipfs/pull/4493))
  - Add interop tests to the release checklist ([ipfs/go-ipfs#4501](https://github.com/ipfs/go-ipfs/pull/4501))
  - Add badgerds to experimental-features ToC ([ipfs/go-ipfs#4537](https://github.com/ipfs/go-ipfs/pull/4537))
  - Fix typos and inconsistencies in commands documentation ([ipfs/go-ipfs#4552](https://github.com/ipfs/go-ipfs/pull/4552))
  - Add a document to help troubleshoot data transfers ([ipfs/go-ipfs#4332](https://github.com/ipfs/go-ipfs/pull/4332))
  - Add a bunch of documentation on public interfaces ([ipfs/go-ipfs#4599](https://github.com/ipfs/go-ipfs/pull/4599))
  - Expand the issue template and remove the severity field ([ipfs/go-ipfs#4624](https://github.com/ipfs/go-ipfs/pull/4624))
  - Add godocs for importers module ([ipfs/go-ipfs#4640](https://github.com/ipfs/go-ipfs/pull/4640))
  - Document make targets ([ipfs/go-ipfs#4653](https://github.com/ipfs/go-ipfs/pull/4653))
  - Add godocs for merkledag module ([ipfs/go-ipfs#4665](https://github.com/ipfs/go-ipfs/pull/4665))
  - Add godocs for unixfs module ([ipfs/go-ipfs#4664](https://github.com/ipfs/go-ipfs/pull/4664))
  - Add sharding to experimental features list ([ipfs/go-ipfs#4569](https://github.com/ipfs/go-ipfs/pull/4569))
  - Add godocs for routing module ([ipfs/go-ipfs#4676](https://github.com/ipfs/go-ipfs/pull/4676))
  - Add godocs for path module ([ipfs/go-ipfs#4689](https://github.com/ipfs/go-ipfs/pull/4689))
  - Add godocs for pin module ([ipfs/go-ipfs#4696](https://github.com/ipfs/go-ipfs/pull/4696))
  - Update link to filestore experimental status ([ipfs/go-ipfs#4557](https://github.com/ipfs/go-ipfs/pull/4557))
- Bugfixes
  - Remove trailing slash in ipfs get paths, fixes #3729 ([ipfs/go-ipfs#4365](https://github.com/ipfs/go-ipfs/pull/4365))
  - fix deadlock in bitswap sessions ([ipfs/go-ipfs#4407](https://github.com/ipfs/go-ipfs/pull/4407))
  - Fix two race conditions (and possibly go routine leaks) in commands ([ipfs/go-ipfs#4406](https://github.com/ipfs/go-ipfs/pull/4406))
  - Fix output delay in ipfs pubsub sub ([ipfs/go-ipfs#4402](https://github.com/ipfs/go-ipfs/pull/4402))
  - Use correct context in AddWithContext ([ipfs/go-ipfs#4433](https://github.com/ipfs/go-ipfs/pull/4433))
  - Fix various IPNS republisher issues ([ipfs/go-ipfs#4440](https://github.com/ipfs/go-ipfs/pull/4440))
  - Fix error handling in commands add and get ([ipfs/go-ipfs#4454](https://github.com/ipfs/go-ipfs/pull/4454))
  - Fix hamt (sharding) delete issue ([ipfs/go-ipfs#4398](https://github.com/ipfs/go-ipfs/pull/4398))
  - More correctly check for reuseport support ([libp2p/go-reuseport#40](https://github.com/libp2p/go-reuseport/pull/40))
  - Fix goroutine leak in websockets transport ([libp2p/go-ws-transport#21](https://github.com/libp2p/go-ws-transport/pull/21))
  - Update badgerds to fix i386 windows build ([ipfs/go-ipfs#4464](https://github.com/ipfs/go-ipfs/pull/4464))
  - Only construct bitswap event loggable if necessary ([ipfs/go-ipfs#4533](https://github.com/ipfs/go-ipfs/pull/4533))
  - Ensure that flush on the mfs root flushes its directory ([ipfs/go-ipfs#4509](https://github.com/ipfs/go-ipfs/pull/4509))
  - Fix deferred unlock of pin lock in AddR ([ipfs/go-ipfs#4562](https://github.com/ipfs/go-ipfs/pull/4562))
  - Fix iOS builds ([ipfs/go-ipfs#4610](https://github.com/ipfs/go-ipfs/pull/4610))
  - Calling repo gc now frees up space with badgerds ([ipfs/go-ipfs#4578](https://github.com/ipfs/go-ipfs/pull/4578))
  - Fix leak in bitswap sessions shutdown ([ipfs/go-ipfs#4658](https://github.com/ipfs/go-ipfs/pull/4658))
  - Fix make on windows ([ipfs/go-ipfs#4682](https://github.com/ipfs/go-ipfs/pull/4682))
  - Ignore invalid key files in keystore directory ([ipfs/go-ipfs#4700](https://github.com/ipfs/go-ipfs/pull/4700))
- General Changes and Refactorings
  - Extract and refactor commands library ([ipfs/go-ipfs#3856](https://github.com/ipfs/go-ipfs/pull/3856))
  - Remove all instances of `Default(false)` ([ipfs/go-ipfs#4042](https://github.com/ipfs/go-ipfs/pull/4042))
  - Build for all supported platforms when testing ([ipfs/go-ipfs#4445](https://github.com/ipfs/go-ipfs/pull/4445))
  - Refine gateway and namesys logging ([ipfs/go-ipfs#4428](https://github.com/ipfs/go-ipfs/pull/4428))
  - Demote bitswap error to an info ([ipfs/go-ipfs#4472](https://github.com/ipfs/go-ipfs/pull/4472))
  - Extract posinfo package to github.com/ipfs/go-ipfs-posinfo ([ipfs/go-ipfs#4669](https://github.com/ipfs/go-ipfs/pull/4669))
  - Move signature verification to ipns validator ([ipfs/go-ipfs#4628](https://github.com/ipfs/go-ipfs/pull/4628))
  - Extract importers/chunk module as go-ipfs-chunker ([ipfs/go-ipfs#4661](https://github.com/ipfs/go-ipfs/pull/4661))
  - Extract go-detect-race from Godeps ([ipfs/go-ipfs#4686](https://github.com/ipfs/go-ipfs/pull/4686))
  - Extract flags, delay, ds-help ([ipfs/go-ipfs#4685](https://github.com/ipfs/go-ipfs/pull/4685))
  - Extract routing package to go-ipfs-routing ([ipfs/go-ipfs#4703](https://github.com/ipfs/go-ipfs/pull/4703))
  - Extract blocks/blockstore package to go-ipfs-blockstore ([ipfs/go-ipfs#4707](https://github.com/ipfs/go-ipfs/pull/4707))
  - Add exchange.SessionExchange interface for exchanges that support sessions ([ipfs/go-ipfs#4709](https://github.com/ipfs/go-ipfs/pull/4709))
  - Extract thirdparty/pq to go-ipfs-pq ([ipfs/go-ipfs#4711](https://github.com/ipfs/go-ipfs/pull/4711))
  - Separate "path" from "path/resolver" ([ipfs/go-ipfs#4713](https://github.com/ipfs/go-ipfs/pull/4713))
- Testing
  - Increase verbosity of t0088-repo-stat-symlink.sh test ([ipfs/go-ipfs#4434](https://github.com/ipfs/go-ipfs/pull/4434))
  - Make repo size test pass deterministically ([ipfs/go-ipfs#4443](https://github.com/ipfs/go-ipfs/pull/4443))
  - Always set IPFS_PATH in test-lib.sh ([ipfs/go-ipfs#4469](https://github.com/ipfs/go-ipfs/pull/4469))
  - Fix sharness docker ([ipfs/go-ipfs#4489](https://github.com/ipfs/go-ipfs/pull/4489))
  - Fix loops in sharness tests to fail the test if the inner command fails ([ipfs/go-ipfs#4482](https://github.com/ipfs/go-ipfs/pull/4482))
  - Improve bitswap tests, fix race conditions ([ipfs/go-ipfs#4499](https://github.com/ipfs/go-ipfs/pull/4499))
  - Fix circleci cache directory list ([ipfs/go-ipfs#4564](https://github.com/ipfs/go-ipfs/pull/4564))
  - Only run the build test on test_go_expensive ([ipfs/go-ipfs#4645](https://github.com/ipfs/go-ipfs/pull/4645))
  - Fix go test on Windows ([ipfs/go-ipfs#4632](https://github.com/ipfs/go-ipfs/pull/4632))
  - Fix some tests on FreeBSD ([ipfs/go-ipfs#4662](https://github.com/ipfs/go-ipfs/pull/4662))

## 0.4.13 2017-11-16

Ipfs 0.4.13 is a patch release that fixes two high priority issues that were
discovered in the 0.4.12 release.

Bugfixes:
  - Fix periodic bitswap deadlock ([ipfs/go-ipfs#4386](https://github.com/ipfs/go-ipfs/pull/4386))
  - Fix badgerds crash on startup ([ipfs/go-ipfs#4384](https://github.com/ipfs/go-ipfs/pull/4384))


## 0.4.12 2017-11-09

Ipfs 0.4.12 brings with it many important fixes for the huge spike in network
size we've seen this past month. These changes include the Connection Manager,
faster batching in `ipfs add`, libp2p fixes that reduce CPU usage, and a bunch
of new documentation.

The most critical change is the 'Connection Manager': it allows an ipfs node to
maintain a limited set of connections to other peers in the network. By default
(and with no config changes required by the user), ipfs nodes will now try to
maintain between 600 and 900 open connections. These limits are still likely
higher than needed, and future releases may lower the default recommendation,
but for now we want to make changes gradually. The rationale for this selection
of numbers is as follows:

- The DHT routing table for a large network may rise to around 400 peers
- Bitswap connections tend to be separate from the DHT
- PubSub connections also generally are another distinct set of peers
  (including js-ipfs nodes)

Because of this, we selected 600 as a 'LowWater' number, and 900 as a
'HighWater' number to avoid having to clear out connections too frequently.
You can configure different numbers as you see fit via the `Swarm.ConnMgr`
field in your ipfs config file. See
[here](https://github.com/ipfs/go-ipfs/blob/master/docs/config.md#connmgr) for
more details.

Disk utilization during `ipfs add` has been optimized for large files by doing
batch writes in parallel. Previously, when adding a large file, users might have
noticed that the add progressed by about 8MB at a time, with brief pauses in between.
This was caused by quickly filling up the batch, then blocking while it was
writing to disk. We now write to disk in the background while continuing to add
the remainder of the file.

Other changes in this release have noticeably reduced memory consumption and CPU
usage. This was done by optimising some frequently called functions in libp2p
that were expensive in terms of both CPU usage and memory allocations. We also
lowered the yamux accept buffer sizes which were raised over a year ago to
combat a separate bug that has since been fixed.

And finally, thank you to everyone who filed bugs, tested out the release candidates,
filed pull requests, and contributed in any other way to this release!

- Features
  - Implement Connection Manager ([ipfs/go-ipfs#4288](https://github.com/ipfs/go-ipfs/pull/4288))
  - Support multiple files in dag put ([ipfs/go-ipfs#4254](https://github.com/ipfs/go-ipfs/pull/4254))
  - Add 'raw' support to the dag put command ([ipfs/go-ipfs#4285](https://github.com/ipfs/go-ipfs/pull/4285))
- Improvements
  - Parallelize dag batch flushing ([ipfs/go-ipfs#4296](https://github.com/ipfs/go-ipfs/pull/4296))
  - Update go-peerstream to improve CPU usage ([ipfs/go-ipfs#4323](https://github.com/ipfs/go-ipfs/pull/4323))
  - Add full support for CidV1 in Files API and Dag Modifier ([ipfs/go-ipfs#4026](https://github.com/ipfs/go-ipfs/pull/4026))
  - Lower yamux accept buffer size ([ipfs/go-ipfs#4326](https://github.com/ipfs/go-ipfs/pull/4326))
  - Optimise `ipfs pin update` command ([ipfs/go-ipfs#4348](https://github.com/ipfs/go-ipfs/pull/4348))
- Documentation
  - Add some docs on plugins ([ipfs/go-ipfs#4255](https://github.com/ipfs/go-ipfs/pull/4255))
  - Add more info about private network bootstrap ([ipfs/go-ipfs#4270](https://github.com/ipfs/go-ipfs/pull/4270))
  - Add more info about `ipfs add` chunker option ([ipfs/go-ipfs#4306](https://github.com/ipfs/go-ipfs/pull/4306))
  - Remove cruft in readme and mention discourse forum ([ipfs/go-ipfs#4345](https://github.com/ipfs/go-ipfs/pull/4345))
  - Add note about updating before reporting issues ([ipfs/go-ipfs#4361](https://github.com/ipfs/go-ipfs/pull/4361))
- Bugfixes
  - Fix FreeBSD build issues ([ipfs/go-ipfs#4275](https://github.com/ipfs/go-ipfs/pull/4275))
  - Don't crash when Datastore.StorageMax is not defined ([ipfs/go-ipfs#4246](https://github.com/ipfs/go-ipfs/pull/4246))
  - Do not call 'Connect' on NewStream in bitswap ([ipfs/go-ipfs#4317](https://github.com/ipfs/go-ipfs/pull/4317))
  - Filter out "" from active peers in bitswap sessions ([ipfs/go-ipfs#4316](https://github.com/ipfs/go-ipfs/pull/4316))
  - Fix "seeker can't seek" on specific files ([ipfs/go-ipfs#4320](https://github.com/ipfs/go-ipfs/pull/4320))
  - Do not set "gecos" field in Dockerfile ([ipfs/go-ipfs#4331](https://github.com/ipfs/go-ipfs/pull/4331))
  - Handle sym links in when calculating repo size ([ipfs/go-ipfs#4305](https://github.com/ipfs/go-ipfs/pull/4305))
- General Changes and Refactorings
  - Fix indent in sharness tests ([ipfs/go-ipfs#4212](https://github.com/ipfs/go-ipfs/pull/4212))
  - Remove supernode routing ([ipfs/go-ipfs#4302](https://github.com/ipfs/go-ipfs/pull/4302))
  - Extract go-ipfs-addr ([ipfs/go-ipfs#4340](https://github.com/ipfs/go-ipfs/pull/4340))
  - Remove dead code and config files ([ipfs/go-ipfs#4357](https://github.com/ipfs/go-ipfs/pull/4357))
  - Update badgerds to 1.0 ([ipfs/go-ipfs#4327](https://github.com/ipfs/go-ipfs/pull/4327))
  - Wrap help descriptions under 80 chars ([ipfs/go-ipfs#4121](https://github.com/ipfs/go-ipfs/pull/4121))
- Testing
  - Make sharness t0180-p2p less racy ([ipfs/go-ipfs#4310](https://github.com/ipfs/go-ipfs/pull/4310))


### 0.4.11 2017-09-14

Ipfs 0.4.11 is a larger release that brings many long-awaited features and
performance improvements. These include new datastore options, more efficient
bitswap transfers, greatly improved resource consumption, circuit relay
support, ipld plugins, and more! Take a look at the full changelog below for a
detailed list of every change.

The ipfs datastore has, until now, been a combination of leveldb and a custom
git-like storage backend called 'flatfs'. This works well enough for the
average user, but different ipfs usecases demand different backend
configurations. To address this, we have changed the configuration file format
for datastores to be a modular way of specifying exactly how you want the
datastore to be structured. You will now be able to configure ipfs to use
flatfs, leveldb, badger, an in-memory datastore, and more to suit your needs.
See the new [datastore
documentation](https://github.com/ipfs/go-ipfs/blob/master/docs/datastores.md)
for more information.

Bitswap received some much needed attention during this release cycle. The
concept of 'Bitswap Sessions' allows bitswap to associate requests for
different blocks to the same underlying session, and from that infer better
ways of requesting that data. In more concrete terms, parts of the ipfs
codebase that take advantage of sessions (currently, only `ipfs pin add`) will
cause much less extra traffic than before. This is done by making optimistic
guesses about which nodes might be providing given blocks and not sending
wantlist updates to every connected bitswap partner, as well as searching the
DHT for providers less frequently. In future releases we will migrate over more
ipfs commands to take advantage of bitswap sessions. As nodes update to this
and future versions, expect to see idle bandwidth usage on the ipfs network
go down noticeably.

The never ending effort to reduce resource consumption had a few important
updates this release. First, the bitswap sessions changes discussed above will
help with improving bandwidth usage. Aside from that there are two important
libp2p updates that improved things significantly. The first was a fix to a bug
in the dial limiter code that was causing it to not limit outgoing dials
correctly. This resulted in ipfs running out of file descriptors very
frequently (as well as incurring a decent amount of excess outgoing bandwidth),
this has now been fixed. Users who previously received "too many open files"
errors should see this much less often in 0.4.11. The second change was a
memory leak in the DHT that was identified and fixed. Streams being tracked in
a map in the DHT weren't being cleaned up after the peer disconnected leading
to the multiplexer session not being cleaned up properly. This issue has been
resolved, and now memory usage appears to be stable over time. There is still a
lot of work to be done improving memory usage, but we feel this is a solid
victory.

It is often said that NAT traversal is the hardest problem in peer to peer
technology, we tend to agree with this. In an effort to provide a more
ubiquitous p2p mesh, we have implemented a relay mechanism that allows willing
peers to relay traffic for other peers who might not otherwise be able to
communicate with each other.  This feature is still pretty early, and currently
users have to manually connect through a relay. The next step in this endeavour
is automatic relaying, and research for this is currently in progress. We
expect that when it lands, it will improve the perceived performance of ipfs by
spending less time attempting connections to hard to reach nodes. A short guide
on using the circuit relay feature can be found
[here](https://github.com/ipfs/go-ipfs/blob/master/docs/experimental-features.md#circuit-relay).

The last feature we want to highlight (but by no means the last feature in this
release) is our new plugin system. There are many different workflows and
usecases that ipfs should be able to support, but not everyone wants to be able
to use every feature. We could simply merge in all these features, but that
causes problems for several reasons: first off, the size of the ipfs binary
starts to get very large very quickly. Second, each of these different pieces
needs to be maintained and updated independently, which would cause significant
churn in the codebase. To address this, we have come up with a system that
allows users to install plugins to the vanilla ipfs daemon that augment its
capabilities. The first of these plugins are a [git
plugin](https://github.com/ipfs/go-ipfs/blob/master/plugin/plugins/git/git.go)
that allows ipfs to natively address git objects and an [ethereum
plugin](https://github.com/ipfs/go-ipld-eth) that lets ipfs ingest and operate
on all ethereum blockchain data. Soon to come are plugins for the bitcoin and
zcash data formats. In the future, we will be adding plugins for other things
like datastore backends and specialized libp2p network transports.
You can read more on this topic in [Plugin docs](docs/plugins.md)

In order to simplify its integration with fs-repo-migrations, we've switched
the ipfs/go-ipfs docker image from a musl base to a glibc base. For most users
this will not be noticeable, but if you've been building your own images based
off this image, you'll have to update your dockerfile. We recommend a
multi-stage dockerfile, where the build stage is based off of a regular Debian or
other glibc-based image, and the assembly stage is based off of the ipfs/go-ipfs
image, and you copy build artifacts from the build stage to the assembly
stage. Note, if you are using the docker image and see a deprecation message,
please update your usage. We will stop supporting the old method of starting
the dockerfile in the next release.

Finally, I would like to thank all of our contributors, users, supporters, and
friends for helping us along the way. Ipfs would not be where it is without
you.


- Features
  - Add `--pin` option to `ipfs dag put` ([ipfs/go-ipfs#4004](https://github.com/ipfs/go-ipfs/pull/4004))
  - Add `--pin` option to `ipfs object put` ([ipfs/go-ipfs#4095](https://github.com/ipfs/go-ipfs/pull/4095))
  - Implement `--profile` option on `ipfs init` ([ipfs/go-ipfs#4001](https://github.com/ipfs/go-ipfs/pull/4001))
  - Add CID Codecs to `ipfs block put` ([ipfs/go-ipfs#4022](https://github.com/ipfs/go-ipfs/pull/4022))
  - Bitswap sessions ([ipfs/go-ipfs#3867](https://github.com/ipfs/go-ipfs/pull/3867))
  - Create plugin API and loader, add ipld-git plugin ([ipfs/go-ipfs#4033](https://github.com/ipfs/go-ipfs/pull/4033))
  - Make announced swarm addresses configurable ([ipfs/go-ipfs#3948](https://github.com/ipfs/go-ipfs/pull/3948))
  - Reprovider strategies ([ipfs/go-ipfs#4113](https://github.com/ipfs/go-ipfs/pull/4113))
  - Circuit Relay integration ([ipfs/go-ipfs#4091](https://github.com/ipfs/go-ipfs/pull/4091))
  - More configurable datastore configs ([ipfs/go-ipfs#3575](https://github.com/ipfs/go-ipfs/pull/3575))
  - Add experimental support for badger datastore ([ipfs/go-ipfs#4007](https://github.com/ipfs/go-ipfs/pull/4007))
- Improvements
  - Add better support for Raw Nodes in MFS and elsewhere ([ipfs/go-ipfs#3996](https://github.com/ipfs/go-ipfs/pull/3996))
  - Added file size to response of `ipfs add` command ([ipfs/go-ipfs#4082](https://github.com/ipfs/go-ipfs/pull/4082))
  - Add /dnsaddr bootstrap nodes ([ipfs/go-ipfs#4127](https://github.com/ipfs/go-ipfs/pull/4127))
  - Do not publish public keys extractable from ID ([ipfs/go-ipfs#4020](https://github.com/ipfs/go-ipfs/pull/4020))
- Documentation
  - Adding documentation that PubSub Sub can be encoded. ([ipfs/go-ipfs#3909](https://github.com/ipfs/go-ipfs/pull/3909))
  - Add Comms items from js-ipfs, including blog ([ipfs/go-ipfs#3936](https://github.com/ipfs/go-ipfs/pull/3936))
  - Add Developer Certificate of Origin ([ipfs/go-ipfs#4006](https://github.com/ipfs/go-ipfs/pull/4006))
  - Add `transports.md` document ([ipfs/go-ipfs#4034](https://github.com/ipfs/go-ipfs/pull/4034))
  - Add `experimental-features.md` document ([ipfs/go-ipfs#4036](https://github.com/ipfs/go-ipfs/pull/4036))
  - Update release docs ([ipfs/go-ipfs#4165](https://github.com/ipfs/go-ipfs/pull/4165))
  - Add documentation for datastore configs ([ipfs/go-ipfs#4223](https://github.com/ipfs/go-ipfs/pull/4223))
  - General update and clean-up of docs ([ipfs/go-ipfs#4222](https://github.com/ipfs/go-ipfs/pull/4222))
- Bugfixes
  - Fix shutdown check in t0023 ([ipfs/go-ipfs#3969](https://github.com/ipfs/go-ipfs/pull/3969))
  - Fix pinning of unixfs sharded directories ([ipfs/go-ipfs#3975](https://github.com/ipfs/go-ipfs/pull/3975))
  - Show escaped url in gateway 404 message ([ipfs/go-ipfs#4005](https://github.com/ipfs/go-ipfs/pull/4005))
  - Fix early opening of bitswap message sender ([ipfs/go-ipfs#4069](https://github.com/ipfs/go-ipfs/pull/4069))
  - Fix determination of 'root' node in dag put ([ipfs/go-ipfs#4072](https://github.com/ipfs/go-ipfs/pull/4072))
  - Fix bad multipart message panic in gateway ([ipfs/go-ipfs#4053](https://github.com/ipfs/go-ipfs/pull/4053))
  - Add blocks to the blockstore before returning them from blockservice sessions ([ipfs/go-ipfs#4169](https://github.com/ipfs/go-ipfs/pull/4169))
  - Various fixes for /ipfs fuse code ([ipfs/go-ipfs#4194](https://github.com/ipfs/go-ipfs/pull/4194))
  - Fix memory leak in dht stream tracking ([ipfs/go-ipfs#4251](https://github.com/ipfs/go-ipfs/pull/4251))
- General Changes and Refactorings
  - Require go 1.8 ([ipfs/go-ipfs#4044](https://github.com/ipfs/go-ipfs/pull/4044))
  - Change IPFS to use the new pluggable Block to IPLD decoding framework. ([ipfs/go-ipfs#4060](https://github.com/ipfs/go-ipfs/pull/4060))
  - Remove tour command from ipfs ([ipfs/go-ipfs#4123](https://github.com/ipfs/go-ipfs/pull/4123))
  - Add support for Go 1.9 ([ipfs/go-ipfs#4156](https://github.com/ipfs/go-ipfs/pull/4156))
  - Remove some dead code ([ipfs/go-ipfs#4204](https://github.com/ipfs/go-ipfs/pull/4204))
  - Switch docker image from musl to glibc ([ipfs/go-ipfs#4219](https://github.com/ipfs/go-ipfs/pull/4219))

### 0.4.10 - 2017-06-27

Ipfs 0.4.10 is a patch release that contains several exciting new features,
bugfixes and general improvements. Including new commands, easier corruption
recovery, and a generally cleaner codebase.

The `ipfs pin` command has two new subcommands, `verify` and `update`. `ipfs
pin verify` is used to scan the repo for pinned object graphs and check their
integrity. Any issues are reported back with helpful error text to make error
recovery simpler.  This subcommand was added to help recover from datastore
corruptions, particularly if using the experimental filestore and accidentally
deleting tracked files.
`ipfs pin update` was added to make the task of keeping a large, frequently
changing object graph pinned. Previously users had to call `ipfs pin rm` on the
old pin, and `ipfs pin add` on the new one. The 'new' `ipfs pin add` call would
be very expensive as it would need to verify the entirety of the graph again.
The `ipfs pin update` command takes shortcuts, portions of the graph that were
covered under the old pin are assumed to be fine, and the command skips
checking them.

Next up, we have finally implemented an `ipfs shutdown` command so users can
shut down their ipfs daemons via the API. This is especially useful on
platforms that make it difficult to control processes (Android, for example),
and is also useful when needing to shut down a node remotely and you do not
have access to the machine itself.

`ipfs add` has gained a new flag; the `--hash` flag allows you to select which
hash function to use and we have given it the ability to select `blake2b-256`.
This pushes us one step closer to shifting over to using blake2b as the
default. Blake2b is significantly faster than sha2-256, and also is conjectured
to provide superior security.

We have also finally implemented a very early (and experimental) `ipfs p2p`.
This command and its subcommands will allow you to open up arbitrary streams to
other ipfs peers through libp2p. The interfaces are a little bit clunky right
now, but shouldn't get in the way of anyone wanting to try building a fully
peer to peer application on top of ipfs and libp2p. For more info on this
command, to ask questions, or to provide feedback, head over to the [feedback
issue](https://github.com/ipfs/go-ipfs/issues/3994) for the command.

A few other subcommands and flags were added around the API, as well as many
other requested improvements. See below for the full list of changes.


- Features
  - Add support for specifying the hash function in `ipfs add` ([ipfs/go-ipfs#3919](https://github.com/ipfs/go-ipfs/pull/3919))
  - Implement `ipfs key {rm, rename}` ([ipfs/go-ipfs#3892](https://github.com/ipfs/go-ipfs/pull/3892))
  - Implement `ipfs shutdown` command ([ipfs/go-ipfs#3884](https://github.com/ipfs/go-ipfs/pull/3884))
  - Implement `ipfs pin update` ([ipfs/go-ipfs#3846](https://github.com/ipfs/go-ipfs/pull/3846))
  - Implement `ipfs pin verify` ([ipfs/go-ipfs#3843](https://github.com/ipfs/go-ipfs/pull/3843))
  - Implemented experimental p2p commands ([ipfs/go-ipfs#3943](https://github.com/ipfs/go-ipfs/pull/3943))
- Improvements
  - Add MaxStorage field to output of "repo stat" ([ipfs/go-ipfs#3915](https://github.com/ipfs/go-ipfs/pull/3915))
  - Add Suborigin header to gateway responses ([ipfs/go-ipfs#3914](https://github.com/ipfs/go-ipfs/pull/3914))
  - Add "--file-order" option to "filestore ls" and "verify" ([ipfs/go-ipfs#3938](https://github.com/ipfs/go-ipfs/pull/3938))
  - Allow selecting ipns keys by Peer ID ([ipfs/go-ipfs#3882](https://github.com/ipfs/go-ipfs/pull/3882))
  - Don't redirect to trailing slash in gateway for `go get` ([ipfs/go-ipfs#3963](https://github.com/ipfs/go-ipfs/pull/3963))
  - Add 'ipfs dht findprovs --num-providers' to allow choosing number of providers to find ([ipfs/go-ipfs#3966](https://github.com/ipfs/go-ipfs/pull/3966))
  - Make sure all keystore keys get republished ([ipfs/go-ipfs#3951](https://github.com/ipfs/go-ipfs/pull/3951))
- Documentation
  - Adding documentation on PubSub encodings ([ipfs/go-ipfs#3909](https://github.com/ipfs/go-ipfs/pull/3909))
  - Change 'neccessary' to 'necessary' ([ipfs/go-ipfs#3941](https://github.com/ipfs/go-ipfs/pull/3941))
  - README.md: add Nix to the linux package managers ([ipfs/go-ipfs#3939](https://github.com/ipfs/go-ipfs/pull/3939))
  - More verbose errors in filestore ([ipfs/go-ipfs#3964](https://github.com/ipfs/go-ipfs/pull/3964))
- Bugfixes
  - Fix typo in message when file size check fails ([ipfs/go-ipfs#3895](https://github.com/ipfs/go-ipfs/pull/3895))
  - Clean up bitswap ledgers when disconnecting ([ipfs/go-ipfs#3437](https://github.com/ipfs/go-ipfs/pull/3437))
  - Make odds of 'process added after close' panic less likely ([ipfs/go-ipfs#3940](https://github.com/ipfs/go-ipfs/pull/3940))
- General Changes and Refactorings
  - Remove 'ipfs diag net' from codebase ([ipfs/go-ipfs#3916](https://github.com/ipfs/go-ipfs/pull/3916))
  - Update to dht code with provide announce option ([ipfs/go-ipfs#3928](https://github.com/ipfs/go-ipfs/pull/3928))
  - Apply the megacheck code vetting tool ([ipfs/go-ipfs#3949](https://github.com/ipfs/go-ipfs/pull/3949))
  - Expose port 8081 in docker container for /ws listener ([ipfs/go-ipfs#3954](https://github.com/ipfs/go-ipfs/pull/3954))

### 0.4.9 - 2017-04-30

Ipfs 0.4.9 is a maintenance release that contains several useful bugfixes and
improvements. Notably, `ipfs add` has gained the ability to select which CID
version will be output. The common ipfs hash that looks like this:
`QmRjNgF2mRLDT8AzCPsQbw1EYF2hDTFgfUmJokJPhCApYP` is a multihash. Multihashes
allow us to specify the hashing algorithm that was used to verify the data, but
it doesn't give us any indication of what format that data might be. To address
that issue, we are adding another couple of bytes to the prefix that will allow us
to indicate the format of the data referenced by the hash. This new format is
called a Content ID, or CID for short. The previous bare multihashes will still
be fully supported throughout the entire application as CID version 0. The new
format with the type information will be CID version 1. To give an example,
the content referenced by the hash above is "Hello Ipfs!". That same content,
in the same format (dag-protobuf) using CIDv1 is
`zb2rhkgXZVkT2xvDiuUsJENPSbWJy7fdYnsboLBzzEjjZMRoG`.

CIDv1 hashes are supported in ipfs versions back to 0.4.5. Nodes running 0.4.4
and older will not be able to load content via CIDv1 and we recommend that they
update to a newer version.

There are many other use cases for CIDs. Plugins can be written to
allow ipfs to natively address content from any other merkletree based system,
such as git, bitcoin, zcash and ethereum -- a few systems we've already started work on.

Aside from the CID flag, there were many other changes as noted below:

- Features
  - Add support for using CidV1 in 'ipfs add' ([ipfs/go-ipfs#3743](https://github.com/ipfs/go-ipfs/pull/3743))
- Improvements
  - Use CID as an ETag strong validator ([ipfs/go-ipfs#3869](https://github.com/ipfs/go-ipfs/pull/3869))
  - Update go-multihash with keccak and bitcoin hashes ([ipfs/go-ipfs#3833](https://github.com/ipfs/go-ipfs/pull/3833))
  - Update go-is-domain to contain new gTLD ([ipfs/go-ipfs#3873](https://github.com/ipfs/go-ipfs/pull/3873))
  - Periodically flush cached directories during ipfs add ([ipfs/go-ipfs#3888](https://github.com/ipfs/go-ipfs/pull/3888))
  - improved gateway directory listing for sharded nodes ([ipfs/go-ipfs#3897](https://github.com/ipfs/go-ipfs/pull/3897))
- Documentation
  - Change issue template to use Severity instead of Priority ([ipfs/go-ipfs#3834](https://github.com/ipfs/go-ipfs/pull/3834))
  - Fix link to commit hook script in contribute.md ([ipfs/go-ipfs#3863](https://github.com/ipfs/go-ipfs/pull/3863))
  - Fix install_unsupported for openbsd, add docs ([ipfs/go-ipfs#3880](https://github.com/ipfs/go-ipfs/pull/3880))
- Bugfixes
  - Fix wanlist typo in prometheus metric name ([ipfs/go-ipfs#3841](https://github.com/ipfs/go-ipfs/pull/3841))
  - Fix `make install` not using ldflags for git hash ([ipfs/go-ipfs#3838](https://github.com/ipfs/go-ipfs/pull/3838))
  - Fix `make install` not installing dependencies ([ipfs/go-ipfs#3848](https://github.com/ipfs/go-ipfs/pull/3848))
  - Fix erroneous Cache-Control: immutable on dir listings ([ipfs/go-ipfs#3870](https://github.com/ipfs/go-ipfs/pull/3870))
  - Fix bitswap accounting of 'BytesSent' in ledger ([ipfs/go-ipfs#3876](https://github.com/ipfs/go-ipfs/pull/3876))
  - Fix gateway handling of sharded directories ([ipfs/go-ipfs#3889](https://github.com/ipfs/go-ipfs/pull/3889))
  - Fix sharding memory growth, and fix resolver for unixfs paths ([ipfs/go-ipfs#3890](https://github.com/ipfs/go-ipfs/pull/3890))
- General Changes and Refactorings
  - Use ctx var consistently in daemon.go ([ipfs/go-ipfs#3864](https://github.com/ipfs/go-ipfs/pull/3864))
  - Handle 404 correctly in dist_get tool ([ipfs/go-ipfs#3879](https://github.com/ipfs/go-ipfs/pull/3879))
- Testing
  - Fix go fuse tests ([ipfs/go-ipfs#3840](https://github.com/ipfs/go-ipfs/pull/3840))

### 0.4.8 - 2017-03-29

Ipfs 0.4.8 brings with it several improvements, bugfixes, documentation
improvements, and the long awaited directory sharding code.

Currently, when too many items are added into a unixfs directory, the object
gets too large and you may experience issues. To pervent this problem, and
generally make working really large directories more efficient, we have
implemented a HAMT structure for unixfs. To enable this feature, run:
```
ipfs config --json Experimental.ShardingEnabled true
```

And restart your daemon if it was running.

Note: With this setting enabled, the hashes of any newly added directories will
be different than they previously were, as the new code will use the sharded
HAMT structure for all directories. Also, nodes running ipfs 0.4.7 and earlier
will not be able to access directories created with this option.

That said, please do give it a try, let us know how it goes, and then take a
look at all the other cool things added in 0.4.8 below.

- Features
	- Implement unixfs directory sharding ([ipfs/go-ipfs#3042](https://github.com/ipfs/go-ipfs/pull/3042))
	- Add DisableNatPortMap option ([ipfs/go-ipfs#3798](https://github.com/ipfs/go-ipfs/pull/3798))
	- Basic Filestore utilty commands ([ipfs/go-ipfs#3653](https://github.com/ipfs/go-ipfs/pull/3653))
- Improvements
	- More Robust GC ([ipfs/go-ipfs#3712](https://github.com/ipfs/go-ipfs/pull/3712))
	- Automatically fix permissions for docker volumes ([ipfs/go-ipfs#3744](https://github.com/ipfs/go-ipfs/pull/3744))
	- Core API refinements and efficiency improvements ([ipfs/go-ipfs#3493](https://github.com/ipfs/go-ipfs/pull/3493))
	- Improve IsPinned() lookups for indirect pins ([ipfs/go-ipfs#3809](https://github.com/ipfs/go-ipfs/pull/3809))
- Documentation
	- Improve 'name' and 'key' helptexts ([ipfs/go-ipfs#3806](https://github.com/ipfs/go-ipfs/pull/3806))
	- Update link to paper in dev.md ([ipfs/go-ipfs#3812](https://github.com/ipfs/go-ipfs/pull/3812))
	- Add test to enforce helptext on commands ([ipfs/go-ipfs#2648](https://github.com/ipfs/go-ipfs/pull/2648))
- Bugfixes
	- Remove bloom filter check on Put call in blockstore ([ipfs/go-ipfs#3782](https://github.com/ipfs/go-ipfs/pull/3782))
	- Re-add the GOPATH checking functionality ([ipfs/go-ipfs#3787](https://github.com/ipfs/go-ipfs/pull/3787))
	- Use fsrepo.IsInitialized to test for initialization ([ipfs/go-ipfs#3805](https://github.com/ipfs/go-ipfs/pull/3805))
	- Return 404 Not Found for failed path resolutions ([ipfs/go-ipfs#3777](https://github.com/ipfs/go-ipfs/pull/3777))
	- Fix 'dist\_get' failing without failing ([ipfs/go-ipfs#3818](https://github.com/ipfs/go-ipfs/pull/3818))
	- Update iptb with fix for t0130 hanging issue ([ipfs/go-ipfs#3823](https://github.com/ipfs/go-ipfs/pull/3823))
	- fix hidden file detection on windows ([ipfs/go-ipfs#3829](https://github.com/ipfs/go-ipfs/pull/3829))
- General Changes and Refactorings
	- Fix multiple govet warnings ([ipfs/go-ipfs#3824](https://github.com/ipfs/go-ipfs/pull/3824))
	- Make Golint happy in the blocks submodule ([ipfs/go-ipfs#3827](https://github.com/ipfs/go-ipfs/pull/3827))
- Testing
	- Enable codeclimate for automated linting and vetting ([ipfs/go-ipfs#3821](https://github.com/ipfs/go-ipfs/pull/3821))
	- Fix EOF test failure with Multipart.Read ([ipfs/go-ipfs#3804](https://github.com/ipfs/go-ipfs/pull/3804))

### 0.4.7 - 2017-03-15

Ipfs 0.4.7 contains several exciting new features!
First off, The long awaited filestore feature has been merged, allowing users
the option to not have ipfs store chunked copies of added files in the
blockstore, pushing to burden of ensuring those files are not changed to the
user. The filestore feature is currently still experimental, and must be
enabled in your config with:
```
ipfs config --json Experimental.FilestoreEnabled true
```
before it can be used. Please see [this issue](https://github.com/ipfs/go-ipfs/issues/3397#issuecomment-284337564) for more details.

Next up, We have merged initial support for ipfs 'Private Networks'. This
feature allows users to run ipfs in a mode that will only connect to other
peers in the private network. This feature, like the filestore is being
released experimentally, but if you're interested please try it out.
Instructions for setting it up can be found
[here](https://github.com/ipfs/go-ipfs/issues/3397#issuecomment-284341649).

This release also enables support for the 'mplex' stream muxer by default. This
stream multiplexing protocol was available previously via the
`--enable-mplex-experiment` daemon flag, but has now graduated to being 'less
experimental' and no longer requires the flag to use it.

Aside from those, we have a good number of bugfixes, perf improvements and new
tests. Heres a list of highlights:

- Features
	- Implement basic filestore 'no-copy' functionality ([ipfs/go-ipfs#3629](https://github.com/ipfs/go-ipfs/pull/3629))
	- Add support for private ipfs networks ([ipfs/go-ipfs#3697](https://github.com/ipfs/go-ipfs/pull/3697))
	- Enable 'mplex' stream muxer by default ([ipfs/go-ipfs#3725](https://github.com/ipfs/go-ipfs/pull/3725))
	- Add `--quieter` option to `ipfs add` ([ipfs/go-ipfs#3770](https://github.com/ipfs/go-ipfs/pull/3770))
	- Report progress during `pin add` via `--progress` ([ipfs/go-ipfs#3671](https://github.com/ipfs/go-ipfs/pull/3671))
- Improvements
	- Allow `ipfs get` to handle content added with raw leaves option ([ipfs/go-ipfs#3757](https://github.com/ipfs/go-ipfs/pull/3757))
	- Fix accuracy of progress bar on `ipfs get` ([ipfs/go-ipfs#3758](https://github.com/ipfs/go-ipfs/pull/3758))
	- Limit number of objects in batches to prevent too many fds issue ([ipfs/go-ipfs#3756](https://github.com/ipfs/go-ipfs/pull/3756))
	- Add more info to bitswap stat ([ipfs/go-ipfs#3635](https://github.com/ipfs/go-ipfs/pull/3635))
	- Add multiple performance metrics ([ipfs/go-ipfs#3615](https://github.com/ipfs/go-ipfs/pull/3615))
	- Make `dist_get` fall back to other downloaders if one fails ([ipfs/go-ipfs#3692](https://github.com/ipfs/go-ipfs/pull/3692))
- Documentation
	- Add Arch Linux install instructions to readme ([ipfs/go-ipfs#3742](https://github.com/ipfs/go-ipfs/pull/3742))
	- Improve release checklist document ([ipfs/go-ipfs#3717](https://github.com/ipfs/go-ipfs/pull/3717))
- Bugfixes
	- Fix drive root parsing on windows ([ipfs/go-ipfs#3328](https://github.com/ipfs/go-ipfs/pull/3328))
	- Fix panic in ipfs get when passing no parameters to API ([ipfs/go-ipfs#3768](https://github.com/ipfs/go-ipfs/pull/3768))
	- Fix breakage of `ipfs pin add` api output ([ipfs/go-ipfs#3760](https://github.com/ipfs/go-ipfs/pull/3760))
	- Fix issue in DHT queries that was causing poor record replication ([ipfs/go-ipfs#3748](https://github.com/ipfs/go-ipfs/pull/3748))
	- Fix `ipfs mount` crashing if no name was published before ([ipfs/go-ipfs#3728](https://github.com/ipfs/go-ipfs/pull/3728))
	- Add `self` key to the `ipfs key list` listing ([ipfs/go-ipfs#3734](https://github.com/ipfs/go-ipfs/pull/3734))
	- Fix panic when shutting down `ipfs daemon` pre gateway setup ([ipfs/go-ipfs#3723](https://github.com/ipfs/go-ipfs/pull/3723))
- General Changes and Refactorings
	- Refactor `EnumerateChildren` to avoid need for bestEffort parameter ([ipfs/go-ipfs#3700](https://github.com/ipfs/go-ipfs/pull/3700))
	- Update fuse dependency, fixing several issues ([ipfs/go-ipfs#3727](https://github.com/ipfs/go-ipfs/pull/3727))
	- Add `install_unsupported` makefile target for 'exotic' systems ([ipfs/go-ipfs#3719](https://github.com/ipfs/go-ipfs/pull/3719))
	- Deprecate implicit daemon argument in Dockerfile ([ipfs/go-ipfs#3685](https://github.com/ipfs/go-ipfs/pull/3685))
- Testing
	- Add test to ensure helptext is under 80 columns wide ([ipfs/go-ipfs#3774](https://github.com/ipfs/go-ipfs/pull/3774))
	- Add unit tests for auto migration code ([ipfs/go-ipfs#3618](https://github.com/ipfs/go-ipfs/pull/3618))
	- Fix iptb stop issue in sharness tests  ([ipfs/go-ipfs#3714](https://github.com/ipfs/go-ipfs/pull/3714))


### 0.4.6 - 2017-02-21

Ipfs 0.4.6 contains several bugfixes related to migrations and also contains a
few other improvements to other parts of the codebase. Notably:

- The default config will now contain some ipv6 addresses for bootstrap nodes.
- `ipfs pin add` should be faster and consume less memory.
- Pinning thousands of files no longer causes superlinear usage of storage space.

- Improvements
	- Make pinset sharding deterministic ([ipfs/go-ipfs#3640](https://github.com/ipfs/go-ipfs/pull/3640))
	- Update to go-multihash with blake2 ([ipfs/go-ipfs#3649](https://github.com/ipfs/go-ipfs/pull/3649))
	- Pass cids instead of nodes around in EnumerateChildrenAsync ([ipfs/go-ipfs#3598](https://github.com/ipfs/go-ipfs/pull/3598))
	- Add /ip6 bootstrap nodes ([ipfs/go-ipfs#3523](https://github.com/ipfs/go-ipfs/pull/3523))
	- Add sub-object support to `dag get` command ([ipfs/go-ipfs#3687](https://github.com/ipfs/go-ipfs/pull/3687))
	- Add half-closed streams support to multiplex experiment ([ipfs/go-ipfs#3695](https://github.com/ipfs/go-ipfs/pull/3695))
- Documentation
	- Add the snap installation instructions ([ipfs/go-ipfs#3663](https://github.com/ipfs/go-ipfs/pull/3663))
	- Add closed PRs, Issues throughput ([ipfs/go-ipfs#3602](https://github.com/ipfs/go-ipfs/pull/3602))
- Bugfixes
	- Fix auto-migration on docker nodes ([ipfs/go-ipfs#3698](https://github.com/ipfs/go-ipfs/pull/3698))
	- Update flatfs to v1.1.2, fixing directory fd issue ([ipfs/go-ipfs#3711](https://github.com/ipfs/go-ipfs/pull/3711))
- General Changes and Refactorings
	- Remove `FindProviders` from routing mocks ([ipfs/go-ipfs#3617](https://github.com/ipfs/go-ipfs/pull/3617))
	- Use Marshalers instead of PostRun to process `block rm` output ([ipfs/go-ipfs#3708](https://github.com/ipfs/go-ipfs/pull/3708))
- Testing
	- Makefile rework and sharness test coverage ([ipfs/go-ipfs#3504](https://github.com/ipfs/go-ipfs/pull/3504))
	- Print out all daemon stderr files when iptb stop fails ([ipfs/go-ipfs#3701](https://github.com/ipfs/go-ipfs/pull/3701))
	- Add tests for recursively pinning a dag ([ipfs/go-ipfs#3691](https://github.com/ipfs/go-ipfs/pull/3691))
	- Fix lack of commit hash during build ([ipfs/go-ipfs#3705](https://github.com/ipfs/go-ipfs/pull/3705))

### 0.4.5 - 2017-02-11

#### Changes from rc3 to rc4
- Update to fixed webui. ([ipfs/go-ipfs#3669](https://github.com/ipfs/go-ipfs/pull/3669))

#### Changes from rc2 to rc3
- Fix handling of null arrays in cbor ipld objects.  ([ipfs/go-ipfs#3666](https://github.com/ipfs/go-ipfs/pull/3666))
- Add env var to enable yamux debug logging.  ([ipfs/go-ipfs#3668](https://github.com/ipfs/go-ipfs/pull/3668))
- Fix libc check during auto-migrations.  ([ipfs/go-ipfs#3665](https://github.com/ipfs/go-ipfs/pull/3665))

#### Changes from rc1 to rc2
- Fixed json output of ipld objects in `ipfs dag get` ([ipfs/go-ipfs#3655](https://github.com/ipfs/go-ipfs/pull/3655))

#### Changes since 0.4.4

- Notable changes
	- IPLD and CIDs
	  - Rework go-ipfs to use Content IDs  ([ipfs/go-ipfs#3187](https://github.com/ipfs/go-ipfs/pull/3187))  ([ipfs/go-ipfs#3290](https://github.com/ipfs/go-ipfs/pull/3290))
	  - Turn merkledag.Node into an interface ([ipfs/go-ipfs#3301](https://github.com/ipfs/go-ipfs/pull/3301))
	  - Implement cbor ipld nodes  ([ipfs/go-ipfs#3325](https://github.com/ipfs/go-ipfs/pull/3325))
	  - Allow cid format selection in block put command  ([ipfs/go-ipfs#3324](https://github.com/ipfs/go-ipfs/pull/3324))  ([ipfs/go-ipfs#3483](https://github.com/ipfs/go-ipfs/pull/3483))
	  - Bitswap protocol extension to handle cids  ([ipfs/go-ipfs#3297](https://github.com/ipfs/go-ipfs/pull/3297))
	  - Add dag get to read-only api  ([ipfs/go-ipfs#3499](https://github.com/ipfs/go-ipfs/pull/3499))
	- Raw Nodes
	  - Implement 'Raw Node' node type for addressing raw data  ([ipfs/go-ipfs#3307](https://github.com/ipfs/go-ipfs/pull/3307))
	  - Optimize DagService GetLinks for Raw Nodes.  ([ipfs/go-ipfs#3351](https://github.com/ipfs/go-ipfs/pull/3351))
	- Experimental PubSub
	  - Added a very basic pubsub implementation  ([ipfs/go-ipfs#3202](https://github.com/ipfs/go-ipfs/pull/3202))
	- Core API
	  - gateway: use core api for serving GET/HEAD/POST  ([ipfs/go-ipfs#3244](https://github.com/ipfs/go-ipfs/pull/3244))

- Improvements
	- Disable auto-gc check in 'ipfs cat'  ([ipfs/go-ipfs#3100](https://github.com/ipfs/go-ipfs/pull/3100))
	- Add `bitswap ledger` command  ([ipfs/go-ipfs#2852](https://github.com/ipfs/go-ipfs/pull/2852))
	- Add `ipfs block rm` command.  ([ipfs/go-ipfs#2962](https://github.com/ipfs/go-ipfs/pull/2962))
	- Add config option to disable bandwidth metrics   ([ipfs/go-ipfs#3381](https://github.com/ipfs/go-ipfs/pull/3381))
	- Add experimental dht 'client mode' flag  ([ipfs/go-ipfs#3269](https://github.com/ipfs/go-ipfs/pull/3269))
	- Add config option to set reprovider interval  ([ipfs/go-ipfs#3101](https://github.com/ipfs/go-ipfs/pull/3101))
	- Add `ipfs dht provide` command  ([ipfs/go-ipfs#3106](https://github.com/ipfs/go-ipfs/pull/3106))
	- Add stream info to `ipfs swarm peers -v`  ([ipfs/go-ipfs#3352](https://github.com/ipfs/go-ipfs/pull/3352))
	- Add option to enable go-multiplex experiment  ([ipfs/go-ipfs#3447](https://github.com/ipfs/go-ipfs/pull/3447))
	- Basic Keystore implementation  ([ipfs/go-ipfs#3472](https://github.com/ipfs/go-ipfs/pull/3472))
	- Make `ipfs add --local` not send providers messages  ([ipfs/go-ipfs#3102](https://github.com/ipfs/go-ipfs/pull/3102))
	- Fix bug in `ipfs tar add` that buffered input in memory  ([ipfs/go-ipfs#3334](https://github.com/ipfs/go-ipfs/pull/3334))
	- Make blockstore retry operations on temporary errors  ([ipfs/go-ipfs#3091](https://github.com/ipfs/go-ipfs/pull/3091))
	- Don't hold the PinLock in adder when not pinning.  ([ipfs/go-ipfs#3222](https://github.com/ipfs/go-ipfs/pull/3222))
	- Validate repo/api file and improve error message  ([ipfs/go-ipfs#3219](https://github.com/ipfs/go-ipfs/pull/3219))
	- no longer hard code gomaxprocs  ([ipfs/go-ipfs#3357](https://github.com/ipfs/go-ipfs/pull/3357))
	- Updated Bash complete script  ([ipfs/go-ipfs#3377](https://github.com/ipfs/go-ipfs/pull/3377))
	- Remove expensive debug statement in blockstore AllKeysChan  ([ipfs/go-ipfs#3384](https://github.com/ipfs/go-ipfs/pull/3384))
	- Remove GC timeout, fix GC tests  ([ipfs/go-ipfs#3494](https://github.com/ipfs/go-ipfs/pull/3494))
	- Fix `ipfs pin add` resource consumption  ([ipfs/go-ipfs#3495](https://github.com/ipfs/go-ipfs/pull/3495))  ([ipfs/go-ipfs#3571](https://github.com/ipfs/go-ipfs/pull/3571))
	- Add IPNS entry to DHT cache after publish  ([ipfs/go-ipfs#3501](https://github.com/ipfs/go-ipfs/pull/3501))
	- Add in `--routing=none` daemon option  ([ipfs/go-ipfs#3605](https://github.com/ipfs/go-ipfs/pull/3605))

- Bitswap
	- Don't re-provide blocks we've provided very recently  ([ipfs/go-ipfs#3105](https://github.com/ipfs/go-ipfs/pull/3105))
	- Add a deadline to sendmsg calls ([ipfs/go-ipfs#3445](https://github.com/ipfs/go-ipfs/pull/3445))
	- cleanup bitswap and handle message send failure slightly better  ([ipfs/go-ipfs#3408](https://github.com/ipfs/go-ipfs/pull/3408))
	- Increase wantlist resend delay to one minute  ([ipfs/go-ipfs#3448](https://github.com/ipfs/go-ipfs/pull/3448))
	- Fix issue where wantlist fullness wasn't included in messages  ([ipfs/go-ipfs#3461](https://github.com/ipfs/go-ipfs/pull/3461))
	- Only pass keys down newBlocks chan in bitswap   ([ipfs/go-ipfs#3271](https://github.com/ipfs/go-ipfs/pull/3271))

- Bugfixes
	- gateway: fix --writable flag  ([ipfs/go-ipfs#3206](https://github.com/ipfs/go-ipfs/pull/3206))
	- Fix relative seek in unixfs not expanding file properly   ([ipfs/go-ipfs#3095](https://github.com/ipfs/go-ipfs/pull/3095))
	- Update multicodec service names for ipfs services  ([ipfs/go-ipfs#3132](https://github.com/ipfs/go-ipfs/pull/3132))
	- dht: add missing protocol ID to newStream call  ([ipfs/go-ipfs#3203](https://github.com/ipfs/go-ipfs/pull/3203))
	- Return immediately on namesys error  ([ipfs/go-ipfs#3345](https://github.com/ipfs/go-ipfs/pull/3345))
	- Improve osxfuse handling  ([ipfs/go-ipfs#3098](https://github.com/ipfs/go-ipfs/pull/3098))  ([ipfs/go-ipfs#3413](https://github.com/ipfs/go-ipfs/pull/3413))
	- commands: fix opt.Description panic when desc was empty  ([ipfs/go-ipfs#3521](https://github.com/ipfs/go-ipfs/pull/3521))
	- Fixes #3133: Properly handle release candidates in version comparison  ([ipfs/go-ipfs#3136](https://github.com/ipfs/go-ipfs/pull/3136))
	- Don't drop error in readStreamedJson.  ([ipfs/go-ipfs#3276](https://github.com/ipfs/go-ipfs/pull/3276))
	- Error out on invalid `--routing` option  ([ipfs/go-ipfs#3482](https://github.com/ipfs/go-ipfs/pull/3482))
	- Respect contexts when returning diagnostics responses  ([ipfs/go-ipfs#3353](https://github.com/ipfs/go-ipfs/pull/3353))
	- Fix json marshalling of pbnode  ([ipfs/go-ipfs#3507](https://github.com/ipfs/go-ipfs/pull/3507))

- General changes and refactorings
	- Disable Suborigins the spec changed and our impl conflicts  ([ipfs/go-ipfs#3519](https://github.com/ipfs/go-ipfs/pull/3519))
	- Avoid sending provide messages for pinsets  ([ipfs/go-ipfs#3103](https://github.com/ipfs/go-ipfs/pull/3103))
	- Refactor cli handling to expose argument parsing functionality  ([ipfs/go-ipfs#3308](https://github.com/ipfs/go-ipfs/pull/3308))
	- Create a FilestoreNode object to carry PosInfo  ([ipfs/go-ipfs#3314](https://github.com/ipfs/go-ipfs/pull/3314))
	- Print 'n/a' instead of zero latency in `ipfs swarm peers`  ([ipfs/go-ipfs#3491](https://github.com/ipfs/go-ipfs/pull/3491))
	- Add DAGService.GetLinks() method to optimize traversals.  ([ipfs/go-ipfs#3255](https://github.com/ipfs/go-ipfs/pull/3255))
	- Make path resolver no longer require whole IpfsNode for construction  ([ipfs/go-ipfs#3321](https://github.com/ipfs/go-ipfs/pull/3321))
	- Distinguish between Offline and Local Modes of daemon operation.  ([ipfs/go-ipfs#3259](https://github.com/ipfs/go-ipfs/pull/3259))
	- Separate out the GC Locking from the Blockstore interface.  ([ipfs/go-ipfs#3348](https://github.com/ipfs/go-ipfs/pull/3348))
	- Avoid unnecessary allocs in datastore key handling  ([ipfs/go-ipfs#3407](https://github.com/ipfs/go-ipfs/pull/3407))
	- Use NextSync method for datastore queries ([ipfs/go-ipfs#3386](https://github.com/ipfs/go-ipfs/pull/3386))
	- Switch unixfs.Metadata.MimeType to optional ([ipfs/go-ipfs#3458](https://github.com/ipfs/go-ipfs/pull/3458))
	- Fix path parsing in `ipfs name publish`   ([ipfs/go-ipfs#3592](https://github.com/ipfs/go-ipfs/pull/3592))
	- Fix inconsistent `ipfs stats bw` formatting  ([ipfs/go-ipfs#3554](https://github.com/ipfs/go-ipfs/pull/3554))
	- Set the libp2p agent version based on version string  ([ipfs/go-ipfs#3569](https://github.com/ipfs/go-ipfs/pull/3569))

- Cross Platform Changes
	- Fix 'dist_get' script on BSDs.  ([ipfs/go-ipfs#3264](https://github.com/ipfs/go-ipfs/pull/3264))
	- ulimit: Tune resource limits on BSDs  ([ipfs/go-ipfs#3374](https://github.com/ipfs/go-ipfs/pull/3374))

- Metrics
	- Introduce go-metrics-interface  ([ipfs/go-ipfs#3189](https://github.com/ipfs/go-ipfs/pull/3189))
	- Fix metrics injection  ([ipfs/go-ipfs#3315](https://github.com/ipfs/go-ipfs/pull/3315))

- Misc
	- Bump Go requirement to 1.7  ([ipfs/go-ipfs#3111](https://github.com/ipfs/go-ipfs/pull/3111))
	- Merge 0.4.3 release candidate changes back into master  ([ipfs/go-ipfs#3248](https://github.com/ipfs/go-ipfs/pull/3248))
	- Add security@ipfs.io GPG key to assets  ([ipfs/go-ipfs#2997](https://github.com/ipfs/go-ipfs/pull/2997))
	- Improve makefiles  ([ipfs/go-ipfs#2999](https://github.com/ipfs/go-ipfs/pull/2999))  ([ipfs/go-ipfs#3265](https://github.com/ipfs/go-ipfs/pull/3265))
	- Refactor install.sh script  ([ipfs/go-ipfs#3194](https://github.com/ipfs/go-ipfs/pull/3194))
	- Add test check for go code formatting  ([ipfs/go-ipfs#3421](https://github.com/ipfs/go-ipfs/pull/3421))
	- bin: dist_get script: prevents get_go_vars() returns same values twice  ([ipfs/go-ipfs#3079](https://github.com/ipfs/go-ipfs/pull/3079))

- Dependencies
	- Update libp2p to have fixed spdystream dep  ([ipfs/go-ipfs#3210](https://github.com/ipfs/go-ipfs/pull/3210))
	- Update libp2p and dht packages  ([ipfs/go-ipfs#3263](https://github.com/ipfs/go-ipfs/pull/3263))
	- Update to libp2p 4.0.1 and propogate other changes  ([ipfs/go-ipfs#3284](https://github.com/ipfs/go-ipfs/pull/3284))
	- Update to libp2p 4.0.4  ([ipfs/go-ipfs#3361](https://github.com/ipfs/go-ipfs/pull/3361))
	- Update go-libp2p across codebase  ([ipfs/go-ipfs#3406](https://github.com/ipfs/go-ipfs/pull/3406))
	- Update to go-libp2p 4.1.0  ([ipfs/go-ipfs#3373](https://github.com/ipfs/go-ipfs/pull/3373))
	- Update deps for libp2p 3.4.0  ([ipfs/go-ipfs#3110](https://github.com/ipfs/go-ipfs/pull/3110))
	- Update go-libp2p-swarm with deadlock fixes  ([ipfs/go-ipfs#3339](https://github.com/ipfs/go-ipfs/pull/3339))
	- Update to new cid and ipld node packages  ([ipfs/go-ipfs#3326](https://github.com/ipfs/go-ipfs/pull/3326))
	- Update to newer ipld node interface with Copy and better Tree  ([ipfs/go-ipfs#3391](https://github.com/ipfs/go-ipfs/pull/3391))
	- Update experimental go-multiplex to 0.2.6  ([ipfs/go-ipfs#3475](https://github.com/ipfs/go-ipfs/pull/3475))
	- Rework routing interfaces to make separation easier  ([ipfs/go-ipfs#3107](https://github.com/ipfs/go-ipfs/pull/3107))
	- Update to dht code with fixed GetClosestPeers  ([ipfs/go-ipfs#3346](https://github.com/ipfs/go-ipfs/pull/3346))
	- Move go-is-domain to gx  ([ipfs/go-ipfs#3077](https://github.com/ipfs/go-ipfs/pull/3077))
	- Extract thirdparty/loggables and thirdparty/peerset  ([ipfs/go-ipfs#3204](https://github.com/ipfs/go-ipfs/pull/3204))
	- Completely remove go-key dep  ([ipfs/go-ipfs#3439](https://github.com/ipfs/go-ipfs/pull/3439))
	- Remove randbo dep, its no longer needed  ([ipfs/go-ipfs#3118](https://github.com/ipfs/go-ipfs/pull/3118))
	- Update libp2p for identify configuration updates  ([ipfs/go-ipfs#3539](https://github.com/ipfs/go-ipfs/pull/3539))
	- Use newer flatfs sharding scheme  ([ipfs/go-ipfs#3608](https://github.com/ipfs/go-ipfs/pull/3608))

- Testing
	- fix test_fsh arg quoting in ipfs-test-lib  ([ipfs/go-ipfs#3085](https://github.com/ipfs/go-ipfs/pull/3085))
	- 100% coverage for blocks/blocksutil  ([ipfs/go-ipfs#3090](https://github.com/ipfs/go-ipfs/pull/3090))
	- 100% coverage on blocks/set  ([ipfs/go-ipfs#3084](https://github.com/ipfs/go-ipfs/pull/3084))
	- 81% coverage on blockstore  ([ipfs/go-ipfs#3074](https://github.com/ipfs/go-ipfs/pull/3074))
	- 80% coverage of unixfs/mod  ([ipfs/go-ipfs#3096](https://github.com/ipfs/go-ipfs/pull/3096))
	- 82% coverage on blocks  ([ipfs/go-ipfs#3086](https://github.com/ipfs/go-ipfs/pull/3086))
	- 87% coverage on unixfs   ([ipfs/go-ipfs#3492](https://github.com/ipfs/go-ipfs/pull/3492)) 
	- Improve coverage on routing/offline  ([ipfs/go-ipfs#3516](https://github.com/ipfs/go-ipfs/pull/3516))
	- Add test for flags package   ([ipfs/go-ipfs#3449](https://github.com/ipfs/go-ipfs/pull/3449))
	- improve test coverage on merkledag package  ([ipfs/go-ipfs#3113](https://github.com/ipfs/go-ipfs/pull/3113))
	- 80% coverage of unixfs/io ([ipfs/go-ipfs#3097](https://github.com/ipfs/go-ipfs/pull/3097))
	- Accept more than one digit in repo version tests  ([ipfs/go-ipfs#3130](https://github.com/ipfs/go-ipfs/pull/3130))
	- Fix typo in hash in t0050  ([ipfs/go-ipfs#3170](https://github.com/ipfs/go-ipfs/pull/3170))
	- fix bug in pinsets and add a stress test for the scenario  ([ipfs/go-ipfs#3273](https://github.com/ipfs/go-ipfs/pull/3273))  ([ipfs/go-ipfs#3302](https://github.com/ipfs/go-ipfs/pull/3302))
	- Report coverage to codecov  ([ipfs/go-ipfs#3473](https://github.com/ipfs/go-ipfs/pull/3473))
	- Add test for 'ipfs config replace'  ([ipfs/go-ipfs#3073](https://github.com/ipfs/go-ipfs/pull/3073))
	- Fix netcat on macOS not closing socket when the stdin sends EOF  ([ipfs/go-ipfs#3515](https://github.com/ipfs/go-ipfs/pull/3515))

- Documentation
	- Update dns help with a correct domain name  ([ipfs/go-ipfs#3087](https://github.com/ipfs/go-ipfs/pull/3087))
	- Add period to `ipfs pin rm`  ([ipfs/go-ipfs#3088](https://github.com/ipfs/go-ipfs/pull/3088))
	- Make all Taglines use imperative mood  ([ipfs/go-ipfs#3041](https://github.com/ipfs/go-ipfs/pull/3041))
	- Document listing commands better  ([ipfs/go-ipfs#3083](https://github.com/ipfs/go-ipfs/pull/3083))
	- Add notes to readme on building for uncommon systems  ([ipfs/go-ipfs#3051](https://github.com/ipfs/go-ipfs/pull/3051))
	- Add branch naming conventions doc  ([ipfs/go-ipfs#3035](https://github.com/ipfs/go-ipfs/pull/3035))
	- Replace <default> keyword with <<default>>  ([ipfs/go-ipfs#3129](https://github.com/ipfs/go-ipfs/pull/3129))
	- Fix Add() docs regarding pinning  ([ipfs/go-ipfs#3513](https://github.com/ipfs/go-ipfs/pull/3513))
	- Add sudo to install commands.  ([ipfs/go-ipfs#3201](https://github.com/ipfs/go-ipfs/pull/3201))
	- Add docs for `"commands".Command.Run`  ([ipfs/go-ipfs#3382](https://github.com/ipfs/go-ipfs/pull/3382))
	- Put config keys in proper case  ([ipfs/go-ipfs#3365](https://github.com/ipfs/go-ipfs/pull/3365))
	- Fix link in `ipfs stats bw` help message  ([ipfs/go-ipfs#3620](https://github.com/ipfs/go-ipfs/pull/3620))


### 0.4.4 - 2016-10-11

This release contains an important hotfix for a bug we discovered in how pinning works.
If you had a large number of pins, new pins would overwrite existing pins.
Apart from the hotfix, this release is equal to the previous release 0.4.3.

- Fix bug in pinsets fanout, and add stress test. (@whyrusleeping, [ipfs/go-ipfs#3273](https://github.com/ipfs/go-ipfs/pull/3273))

We published a [detailed account of the bug and fix in a blog post](https://ipfs.io/blog/21-go-ipfs-0-4-4-released/).

### 0.4.3 - 2016-09-20

There have been no changes since the last release candidate 0.4.3-rc4. \o/

### 0.4.3-rc4 - 2016-09-09

This release candidate fixes issues in Bitswap and the `ipfs add` command, and improves testing.
We plan for this to be the last release candidate before the release of go-ipfs v0.4.3.

With this release candidate, we're also moving go-ipfs to Go 1.7, which we expect will yield improvements in runtime performance, memory usage, build time and size of the release binaries.

- Require Go 1.7. (@whyrusleeping, @Kubuxu, @lgierth, [ipfs/go-ipfs#3163](https://github.com/ipfs/go-ipfs/pull/3163))
  - For this purpose, switch Docker image from Alpine 3.4 to Alpine Edge.
- Fix cancellation of Bitswap `wantlist` entries. (@whyrusleeping, [ipfs/go-ipfs#3182](https://github.com/ipfs/go-ipfs/pull/3182))
- Fix clearing of `active` state of Bitswap provider queries. (@whyrusleeping, [ipfs/go-ipfs#3169](https://github.com/ipfs/go-ipfs/pull/3169))
- Fix a panic in the DHT code. (@Kubuxu, [ipfs/go-ipfs#3200](https://github.com/ipfs/go-ipfs/pull/3200))
- Improve handling of `Identity` field in `ipfs config` command. (@Kubuxu, @whyrusleeping, [ipfs/go-ipfs#3141](https://github.com/ipfs/go-ipfs/pull/3141))
- Fix explicit adding of symlinked files and directories. (@kevina, [ipfs/go-ipfs#3135](https://github.com/ipfs/go-ipfs/pull/3135))
- Fix bash auto-completion of `ipfs daemon --unrestricted-api` option. (@lgierth, [ipfs/go-ipfs#3159](https://github.com/ipfs/go-ipfs/pull/3159))
- Introduce a new timeout tool for tests to avoid licensing issues. (@Kubuxu, [ipfs/go-ipfs#3152](https://github.com/ipfs/go-ipfs/pull/3152))
- Improve output for migrations of fs-repo. (@lgierth, [ipfs/go-ipfs#3158](https://github.com/ipfs/go-ipfs/pull/3158))
- Fix info notice of commands taking input from stdin. (@Kubuxu, [ipfs/go-ipfs#3134](https://github.com/ipfs/go-ipfs/pull/3134))
- Bring back a few tests for stdin handling of `ipfs cat` and `ipfs add`. (@Kubuxu, [ipfs/go-ipfs#3144](https://github.com/ipfs/go-ipfs/pull/3144))
- Improve sharness tests for `ipfs repo verify` command. (@whyrusleeping, [ipfs/go-ipfs#3148](https://github.com/ipfs/go-ipfs/pull/3148))
- Improve sharness tests for CORS headers on the gateway. (@Kubuxu, [ipfs/go-ipfs#3142](https://github.com/ipfs/go-ipfs/pull/3142))
- Improve tests for pinning within `ipfs files`. (@kevina, [ipfs/go-ipfs#3151](https://github.com/ipfs/go-ipfs/pull/3151))
- Improve tests for the automatic raising of file descriptor limits. (@whyrusleeping, [ipfs/go-ipfs#3149](https://github.com/ipfs/go-ipfs/pull/3149))

### 0.4.3-rc3 - 2016-08-11

This release candidate fixes a panic that occurs when input from stdin was
expected, but none was given: [ipfs/go-ipfs#3050](https://github.com/ipfs/go-ipfs/pull/3050)

### 0.4.3-rc2 - 2016-08-04

This release includes bugfixes and fixes for regressions that were introduced
between 0.4.2 and 0.4.3-rc1.

- Regressions
  - Fix daemon panic when there is no multipart input provided over the HTTP API.
  (@whyrusleeping, [ipfs/go-ipfs#2989](https://github.com/ipfs/go-ipfs/pull/2989))
  - Fix `ipfs refs --edges` not printing edges.
  (@Kubuxu, [ipfs/go-ipfs#3007](https://github.com/ipfs/go-ipfs/pull/3007))
  - Fix progress option for `ipfs add` defaulting to true on the HTTP API.
  (@whyrusleeping, [ipfs/go-ipfs#3025](https://github.com/ipfs/go-ipfs/pull/3025))
  - Fix erroneous printing of stdin reading message.
  (@whyrusleeping, [ipfs/go-ipfs#3033](https://github.com/ipfs/go-ipfs/pull/3033))
  - Fix panic caused by passing `--mount` and `--offline` flags to `ipfs daemon`.
  (@Kubuxu, [ipfs/go-ipfs#3022](https://github.com/ipfs/go-ipfs/pull/3022))
  - Fix symlink path resolution on windows.
  (@Kubuxu, [ipfs/go-ipfs#3023](https://github.com/ipfs/go-ipfs/pull/3023))
  - Add in code to prevent issue 3032 from crashing the daemon.
  (@whyrusleeping, [ipfs/go-ipfs#3037](https://github.com/ipfs/go-ipfs/pull/3037))


### 0.4.3-rc1 - 2016-07-23

This is a maintenance release which comes with a couple of nice enhancements, and improves the performance of Storage, Bitswap, as well as Content and Peer Routing. It also introduces a handful of new commands and options, and fixes a good bunch of bugs.

This is the first Release Candidate. Unless there are vulnerabilities or regressions discovered, the final 0.4.3 release will happen about one week from now.

- Security Vulnerability

  - The `master` branch if go-ipfs suffered from a vulnerability for about 3 weeks. It allowed an attacker to use an iframe to request malicious HTML and JS from the API of a local go-ipfs node. The attacker could then gain unrestricted access to the node's API, and e.g. extract the private key. We fixed this issue by reintroducing restrictions on which particular objects can be loaded through the API (@lgierth, [ipfs/go-ipfs#2949](https://github.com/ipfs/go-ipfs/pull/2949)), and by completely excluding the private key from the API (@Kubuxu, [ipfs/go-ipfs#2957](https://github.com/ipfs/go-ipfs/pull/2957)). We will also work on more hardening of the API in the next release.
  - **The previous release 0.4.2 is not vulnerable. That means if you're using official binaries from [dist.ipfs.io](https://dist.ipfs.io) you're not affected.** If you're running go-ipfs built from the `master` branch between June 17th ([ipfs/go-ipfs@1afebc21](https://github.com/ipfs/go-ipfs/commit/1afebc21f324982141ca8a29710da0d6f83ca804)) and July 7th ([ipfs/go-ipfs@39bef0d5](https://github.com/ipfs/go-ipfs/commit/39bef0d5b01f70abf679fca2c4d078a2d55620e2)), please update to v0.4.3-rc1 immediately.
  - We are grateful to the group of independent researchers who made us aware of this vulnerability. We wanna use this opportunity to reiterate that we're very happy about any additional review of pull requests and releases. You can contact us any time at security@ipfs.io (GPG [4B9665FB 92636D17 7C7A86D3 50AAE8A9 59B13AF3](https://pgp.mit.edu/pks/lookup?op=get&search=0x50AAE8A959B13AF3)).

- Notable changes

  - Improve Bitswap performance. (@whyrusleeping, [ipfs/go-ipfs#2727](https://github.com/ipfs/go-ipfs/pull/2727), [ipfs/go-ipfs#2798](https://github.com/ipfs/go-ipfs/pull/2798))
  - Improve Content Routing and Peer Routing performance. (@whyrusleeping, [ipfs/go-ipfs#2817](https://github.com/ipfs/go-ipfs/pull/2817), [ipfs/go-ipfs#2841](https://github.com/ipfs/go-ipfs/pull/2841))
  - Improve datastore, blockstore, and dagstore performance. (@kevina, @Kubuxu, @whyrusleeping [ipfs/go-datastore#43](https://github.com/ipfs/go-datastore/pull/43), [ipfs/go-ipfs#2885](https://github.com/ipfs/go-ipfs/pull/2885), [ipfs/go-ipfs#2961](https://github.com/ipfs/go-ipfs/pull/2961), [ipfs/go-ipfs#2953](https://github.com/ipfs/go-ipfs/pull/2953), [ipfs/go-ipfs#2960](https://github.com/ipfs/go-ipfs/pull/2960))
  - Content Providers are now stored on disk to gain savings on process memory. (@whyrusleeping, [ipfs/go-ipfs#2804](https://github.com/ipfs/go-ipfs/pull/2804), [ipfs/go-ipfs#2860](https://github.com/ipfs/go-ipfs/pull/2860))
  - Migrations of the fs-repo (usually stored at `~/.ipfs`) now run automatically. If there's a TTY available, you'll get prompted when running `ipfs daemon`, and in addition you can use the `--migrate=true` or `--migrate=false` options to avoid the prompt. (@whyrusleeping, @lgierth, [ipfs/go-ipfs#2939](https://github.com/ipfs/go-ipfs/pull/2939))
  - The internal naming of blocks in the blockstore has changed, which requires a migration of the fs-repo, from version 3 to 4. (@whyrusleeping, [ipfs/go-ipfs#2903](https://github.com/ipfs/go-ipfs/pull/2903))
  - We now automatically raise the file descriptor limit to 1024 if neccessary. (@whyrusleeping, [ipfs/go-ipfs#2884](https://github.com/ipfs/go-ipfs/pull/2884), [ipfs/go-ipfs#2891](https://github.com/ipfs/go-ipfs/pull/2891))
  - After a long struggle with deadlocks and hanging connections, we've decided to disable the uTP transport by default for now. (@whyrusleeping, [ipfs/go-ipfs#2840](https://github.com/ipfs/go-ipfs/pull/2840), [ipfs/go-libp2p-transport@88244000](https://github.com/ipfs/go-libp2p-transport/commit/88244000f0ce8851ffcfbac746ebc0794b71d2a4))
  - There is now documentation for the configuration options in `docs/config.md`. (@whyrusleeping, [ipfs/go-ipfs#2974](https://github.com/ipfs/go-ipfs/pull/2974))
  - All commands now sanely handle the combination of stdin and optional flags in certain edge cases. (@lgierth, [ipfs/go-ipfs#2952](https://github.com/ipfs/go-ipfs/pull/2952))

- New Features

  - Add `--offline` option to `ipfs daemon` command, which disables all swarm networking. (@Kubuxu, [ipfs/go-ipfs#2696](https://github.com/ipfs/go-ipfs/pull/2696), [ipfs/go-ipfs#2867](https://github.com/ipfs/go-ipfs/pull/2867))
  - Add `Datastore.HashOnRead` option for verifying block hashes on read access. (@Kubuxu, [ipfs/go-ipfs#2904](https://github.com/ipfs/go-ipfs/pull/2904))
  - Add `Datastore.BloomFilterSize` option for tuning the blockstore's new lookup bloom filter. (@Kubuxu, [ipfs/go-ipfs#2973](https://github.com/ipfs/go-ipfs/pull/2973))

- Bugfixes

  - Fix publishing of local IPNS entries, and more. (@whyrusleeping, [ipfs/go-ipfs#2943](https://github.com/ipfs/go-ipfs/pull/2943))
  - Fix progress bars in `ipfs add` and `ipfs get`. (@whyrusleeping, [ipfs/go-ipfs#2893](https://github.com/ipfs/go-ipfs/pull/2893), [ipfs/go-ipfs#2948](https://github.com/ipfs/go-ipfs/pull/2948))
  - Make sure files added through `ipfs files` are pinned and don't get GC'd. (@kevina, [ipfs/go-ipfs#2872](https://github.com/ipfs/go-ipfs/pull/2872))
  - Fix copying into directory using `ipfs files cp`. (@whyrusleeping, [ipfs/go-ipfs#2977](https://github.com/ipfs/go-ipfs/pull/2977))
  - Fix `ipfs version --commit` with Docker containers. (@lgierth, [ipfs/go-ipfs#2734](https://github.com/ipfs/go-ipfs/pull/2734))
  - Run `ipfs diag` commands in the daemon instead of the CLI. (@Kubuxu, [ipfs/go-ipfs#2761](https://github.com/ipfs/go-ipfs/pull/2761))
  - Fix protobuf encoding on the API and in commands. (@stebalien, [ipfs/go-ipfs#2516](https://github.com/ipfs/go-ipfs/pull/2516))
  - Fix goroutine leak in `/ipfs/ping` protocol handler. (@whyrusleeping, [ipfs/go-libp2p#58](https://github.com/ipfs/go-libp2p/pull/58))
  - Fix `--flags` option on `ipfs commands`. (@Kubuxu, [ipfs/go-ipfs#2773](https://github.com/ipfs/go-ipfs/pull/2773))
  - Fix the error channels in `namesys`. (@whyrusleeping, [ipfs/go-ipfs#2788](https://github.com/ipfs/go-ipfs/pull/2788))
  - Fix consumptions of observed swarm addresses. (@whyrusleeping, [ipfs/go-libp2p#63](https://github.com/ipfs/go-libp2p/pull/63), [ipfs/go-ipfs#2771](https://github.com/ipfs/go-ipfs/issues/2771))
  - Fix a rare DHT panic. (@whyrusleeping, [ipfs/go-ipfs#2856](https://github.com/ipfs/go-ipfs/pull/2856))
  - Fix go-ipfs/js-ipfs interoperability issues in SPDY. (@whyrusleeping, [whyrusleeping/go-smux-spdystream@fae17783](https://github.com/whyrusleeping/go-smux-spdystream/commit/fae1778302a9e029bb308cf71cf33f857f2d89e8))
  - Fix a logging race condition during shutdown. (@Kubuxu, [ipfs/go-log#3](https://github.com/ipfs/go-log/pull/3))
  - Prevent DHT connection hangs. (@whyrusleeping, [ipfs/go-ipfs#2826](https://github.com/ipfs/go-ipfs/pull/2826), [ipfs/go-ipfs#2863](https://github.com/ipfs/go-ipfs/pull/2863))
  - Fix NDJSON output of `ipfs refs local`. (@Kubuxu, [ipfs/go-ipfs#2812](https://github.com/ipfs/go-ipfs/pull/2812))
  - Fix race condition in NAT detection. (@whyrusleeping, [ipfs/go-libp2p#69](https://github.com/ipfs/go-libp2p/pull/69))
  - Fix error messages. (@whyrusleeping, @Kubuxu, [ipfs/go-ipfs#2905](https://github.com/ipfs/go-ipfs/pull/2905), [ipfs/go-ipfs#2928](https://github.com/ipfs/go-ipfs/pull/2928))

- Enhancements

  - Increase maximum object size on `ipfs put` from 1 MiB to 2 MiB. The maximum object size on the wire including all framing is 4 MiB. (@kpcyrd, [ipfs/go-ipfs#2980](https://github.com/ipfs/go-ipfs/pull/2980))
  - Add CORS headers to the Gateway's default config. (@Kubuxu, [ipfs/go-ipfs#2778](https://github.com/ipfs/go-ipfs/pull/2778))
  - Clear the dial backoff for a peer when using `ipfs swarm connect`. (@whyrusleeping, [ipfs/go-ipfs#2941](https://github.com/ipfs/go-ipfs/pull/2941))
  - Allow passing options to daemon in Docker container. (@lgierth, [ipfs/go-ipfs#2955](https://github.com/ipfs/go-ipfs/pull/2955))
  - Add `-v/--verbose` to `ìpfs swarm peers` command. (@csasarak, [ipfs/go-ipfs#2713](https://github.com/ipfs/go-ipfs/pull/2713))
  - Add `--format`, `--hash`, and `--size` options to `ipfs files stat` command. (@Kubuxu, [ipfs/go-ipfs#2706](https://github.com/ipfs/go-ipfs/pull/2706))
  - Add `--all` option to `ipfs version` command. (@Kubuxu, [ipfs/go-ipfs#2790](https://github.com/ipfs/go-ipfs/pull/2790))
  - Add `ipfs repo version` command. (@pfista, [ipfs/go-ipfs#2598](https://github.com/ipfs/go-ipfs/pull/2598))
  - Add `ipfs repo verify` command. (@whyrusleeping, [ipfs/go-ipfs#2924](https://github.com/ipfs/go-ipfs/pull/2924), [ipfs/go-ipfs#2951](https://github.com/ipfs/go-ipfs/pull/2951))
  - Add `ipfs stats repo` and `ipfs stats bitswap` command aliases. (@pfista, [ipfs/go-ipfs#2810](https://github.com/ipfs/go-ipfs/pull/2810))
  - Add success indication to responses of `ipfs ping` command. (@Kubuxu, [ipfs/go-ipfs#2813](https://github.com/ipfs/go-ipfs/pull/2813))
  - Save changes made via `ipfs swarm filter` to the config file. (@yuvallanger, [ipfs/go-ipfs#2880](https://github.com/ipfs/go-ipfs/pull/2880))
  - Expand `ipfs_p2p_peers` metric to include libp2p transport. (@lgierth, [ipfs/go-ipfs#2728](https://github.com/ipfs/go-ipfs/pull/2728))
  - Rework `ipfs files add` internals to avoid caching and prevent memory leaks. (@whyrusleeping, [ipfs/go-ipfs#2795](https://github.com/ipfs/go-ipfs/pull/2795))
  - Support `GOPATH` with multiple path components. (@karalabe, @lgierth, @djdv, [ipfs/go-ipfs#2808](https://github.com/ipfs/go-ipfs/pull/2808), [ipfs/go-ipfs#2862](https://github.com/ipfs/go-ipfs/pull/2862), [ipfs/go-ipfs#2975](https://github.com/ipfs/go-ipfs/pull/2975))

- General Codebase

  - Take steps towards the `filestore` datastore. (@kevina, [ipfs/go-ipfs#2792](https://github.com/ipfs/go-ipfs/pull/2792), [ipfs/go-ipfs#2634](https://github.com/ipfs/go-ipfs/pull/2634))
  - Update recommended Golang version to 1.6.2 (@Kubuxu, [ipfs/go-ipfs#2724](https://github.com/ipfs/go-ipfs/pull/2724))
  - Update to Gx 0.8.0 and Gx-Go 1.2.1, which is faster and less noisy. (@whyrusleeping, [ipfs/go-ipfs#2979](https://github.com/ipfs/go-ipfs/pull/2979))
  - Use `go4.org/lock` instead of `camlistore/lock` for locking. (@whyrusleeping, [ipfs/go-ipfs#2887](https://github.com/ipfs/go-ipfs/pull/2887))
  - Manage `go.uuid`, `hamming`, `backoff`, `proquint`, `pb`, `go-context`, `cors`, `go-datastore` packages with Gx. (@Kubuxu, [ipfs/go-ipfs#2733](https://github.com/ipfs/go-ipfs/pull/2733), [ipfs/go-ipfs#2736](https://github.com/ipfs/go-ipfs/pull/2736), [ipfs/go-ipfs#2757](https://github.com/ipfs/go-ipfs/pull/2757), [ipfs/go-ipfs#2825](https://github.com/ipfs/go-ipfs/pull/2825), [ipfs/go-ipfs#2838](https://github.com/ipfs/go-ipfs/pull/2838))
  - Clean up the gateway's surface. (@lgierth, [ipfs/go-ipfs#2874](https://github.com/ipfs/go-ipfs/pull/2874))
  - Simplify the API gateway's access restrictions. (@lgierth, [ipfs/go-ipfs#2949](https://github.com/ipfs/go-ipfs/pull/2949), [ipfs/go-ipfs#2956](https://github.com/ipfs/go-ipfs/pull/2956))
  - Update docker image to Alpine Linux 3.4 and remove Go version constraint. (@lgierth, [ipfs/go-ipfs#2901](https://github.com/ipfs/go-ipfs/pull/2901), [ipfs/go-ipfs#2929](https://github.com/ipfs/go-ipfs/pull/2929))
  - Clarify `Dockerfile` and `Dockerfile.fast`. (@lgierth, [ipfs/go-ipfs#2796](https://github.com/ipfs/go-ipfs/pull/2796))
  - Simplify resolution of Git commit refs in Dockerfiles. (@lgierth, [ipfs/go-ipfs#2754](https://github.com/ipfs/go-ipfs/pull/2754))
  - Consolidate `--verbose` description across commands. (@Kubuxu, [ipfs/go-ipfs#2746](https://github.com/ipfs/go-ipfs/pull/2746))
  - Allow setting position of default values in command option descriptions. (@Kubuxu, [ipfs/go-ipfs#2744](https://github.com/ipfs/go-ipfs/pull/2744))
  - Set explicit default values for boolean command options. (@RichardLitt, [ipfs/go-ipfs#2657](https://github.com/ipfs/go-ipfs/pull/2657))
  - Autogenerate command synopsises. (@Kubuxu, [ipfs/go-ipfs#2785](https://github.com/ipfs/go-ipfs/pull/2785))
  - Fix and improve lots of documentation. (@RichardLitt, [ipfs/go-ipfs#2741](https://github.com/ipfs/go-ipfs/pull/2741), [ipfs/go-ipfs#2781](https://github.com/ipfs/go-ipfs/pull/2781))
  - Improve command descriptions to fit a width of 78 characters. (@RichardLitt, [ipfs/go-ipfs#2779](https://github.com/ipfs/go-ipfs/pull/2779), [ipfs/go-ipfs#2780](https://github.com/ipfs/go-ipfs/pull/2780), [ipfs/go-ipfs#2782](https://github.com/ipfs/go-ipfs/pull/2782))
  - Fix filename conflict in the debugging guide. (@Kubuxu, [ipfs/go-ipfs#2752](https://github.com/ipfs/go-ipfs/pull/2752))
  - Decapitalize log messages, according to Golang style guides. (@RichardLitt, [ipfs/go-ipfs#2853](https://github.com/ipfs/go-ipfs/pull/2853))
  - Add Github Issues HowTo guide. (@RichardLitt, @chriscool, [ipfs/go-ipfs#2889](https://github.com/ipfs/go-ipfs/pull/2889), [ipfs/go-ipfs#2895](https://github.com/ipfs/go-ipfs/pull/2895))
  - Add Github Issue template. (@chriscool, [ipfs/go-ipfs#2786](https://github.com/ipfs/go-ipfs/pull/2786))
  - Apply standard-readme to the README file. (@RichardLitt, [ipfs/go-ipfs#2883](https://github.com/ipfs/go-ipfs/pull/2883))
  - Fix issues pointed out by `govet`. (@Kubuxu, [ipfs/go-ipfs#2854](https://github.com/ipfs/go-ipfs/pull/2854))
  - Clarify `ipfs get` error message. (@whyrusleeping, [ipfs/go-ipfs#2886](https://github.com/ipfs/go-ipfs/pull/2886))
  - Remove dead code. (@whyrusleeping, [ipfs/go-ipfs#2819](https://github.com/ipfs/go-ipfs/pull/2819))
  - Add changelog for v0.4.3. (@lgierth, [ipfs/go-ipfs#2984](https://github.com/ipfs/go-ipfs/pull/2984))

- Tests & CI

  - Fix flaky `ipfs mount` sharness test by using the `iptb` tool. (@noffle, [ipfs/go-ipfs#2707](https://github.com/ipfs/go-ipfs/pull/2707))
  - Fix flaky IP port selection in tests. (@Kubuxu, [ipfs/go-ipfs#2855](https://github.com/ipfs/go-ipfs/pull/2855))
  - Fix CLI tests on OSX by resolving /tmp symlink. (@Kubuxu, [ipfs/go-ipfs#2926](https://github.com/ipfs/go-ipfs/pull/2926))
  - Fix flaky GC test by running the daemon in offline mode. (@Kubuxu, [ipfs/go-ipfs#2908](https://github.com/ipfs/go-ipfs/pull/2908))
  - Add tests for `ipfs add` with hidden files. (@Kubuxu, [ipfs/go-ipfs#2756](https://github.com/ipfs/go-ipfs/pull/2756))
  - Add test to make sure the body of HEAD responses is empty. (@Kubuxu, [ipfs/go-ipfs#2775](https://github.com/ipfs/go-ipfs/pull/2775))
  - Add test to catch misdials. (@Kubuxu, [ipfs/go-ipfs#2831](https://github.com/ipfs/go-ipfs/pull/2831))
  - Mark flaky tests for `ipfs dht query` as known failure. (@noffle, [ipfs/go-ipfs#2720](https://github.com/ipfs/go-ipfs/pull/2720))
  - Remove failing blockstore-without-context test. (@Kubuxu, [ipfs/go-ipfs#2857](https://github.com/ipfs/go-ipfs/pull/2857))
  - Fix `--version` tests for versions with a suffix like `-dev` or `-rc1`. (@lgierth, [ipfs/go-ipfs#2937](https://github.com/ipfs/go-ipfs/pull/2937))
  - Make sharness tests work in cases where go-ipfs is symlinked into GOPATH. (@lgierth, [ipfs/go-ipfs#2937](https://github.com/ipfs/go-ipfs/pull/2937))
  - Add variable delays to blockstore mocks. (@rikonor, [ipfs/go-ipfs#2871](https://github.com/ipfs/go-ipfs/pull/2871))
  - Disable Travis CI email notifications. (@Kubuxu, [ipfs/go-ipfs#2896](https://github.com/ipfs/go-ipfs/pull/2896))


### 0.4.2 - 2016-05-17

This is a patch release which fixes performance and networking bugs in go-libp2p,
You should see improvements in CPU and RAM usage, as well as speed of object lookups.
There are also a few other nice improvements.

* Notable Fixes
  * Set a deadline for dialing attempts. This prevents a node from accumulating
    failed connections. (@whyrusleeping)
  * Avoid unnecessary string/byte conversions in go-multihash. (@whyrusleeping)
  * Fix a deadlock around the yamux stream muxer. (@whyrusleeping)
  * Fix a bug that left channels open, causing hangs. (@whyrusleeping)
  * Fix a bug around yamux which caused connection hangs. (@whyrusleeping)
  * Fix a crash caused by nil multiaddrs. (@whyrusleeping)

* Enhancements
  * Add NetBSD support. (@erde74)
  * Set Cache-Control: immutable on /ipfs responses. (@kpcyrd)
  * Have `ipfs init` optionally accept a default configuration from stdin. (@sivachandran)
  * Add `ipfs log ls` command for listing logging subsystems. (@hsanjuan)
  * Allow bitswap to read multiple messages per stream. (@whyrusleeping)
  * Remove `make toolkit_upgrade` step. (@chriscool)

* Documentation
  * Add a debug-guidelines document. (@richardlitt)
  * Update the contribute document. (@richardlitt)
  * Fix documentation of many `ipfs` commands. (@richardlitt)
  * Fall back to ShortDesc if LongDesc is missing. (@Kubuxu)

* Removals
  * Remove -f option from `ipfs init` command. (@whyrusleeping)

* Bugfixes
  * Fix `ipfs object patch` argument handling and validation. (@jbenet)
  * Fix `ipfs config edit` command by running it client-side. (@Kubuxu)
  * Set default value for `ipfs refs` arguments. (@richardlitt)
  * Fix parsing of incorrect command and argument permutations. (@thomas-gardner)
  * Update Dockerfile to latest go1.5.4-r0. (@chriscool)
  * Allow passing IPFS_LOGGING to Docker image. (@lgierth)
  * Fix dot path parsing on Windows. (@djdv)
  * Fix formatting of `ipfs log ls` output. (@richardlitt)

* General Codebase
  * Refactor Makefile. (@kevina)
  * Wire context into bitswap requests more deeply. (@whyrusleeping)
  * Use gx for iptb. (@chriscool)
  * Update gx and gx-go. (@chriscool)
  * Make blocks.Block an interface. (@kevina)
  * Silence check for Docker existance. (@chriscool)
  * Add dist_get script for fetching tools from dist.ipfs.io. (@whyrusleeping)
  * Add proper defaults to all `ipfs` commands. (@richardlitt)
  * Remove dead `count` option from `ipfs pin ls`. (@richardlitt)
  * Initialize pin mode strings only once. (@chriscool)
  * Add changelog for v0.4.2. (@lgierth)
  * Specify a dist.ipfs.io hash for tool downloads instead of trusting DNS. (@lgierth)

* CI
  * Fix t0170-dht sharness test. (@chriscool)
  * Increase timeout in t0060-daemon sharness test. (@Kubuxu)
  * Have CircleCI use `make deps` instead of `gx` directly. (@whyrusleeping)


### 0.4.1 - 2016-04-25

This is a patch release that fixes a few bugs, and adds a few small (but not
insignificant) features. The primary reason for this release is the listener
hang bugfix that was shipped in the 0.4.0 release.

* Features
  * implemented ipfs object diff (@whyrusleeping)
  * allow promises (used in get, refs) to fail (@whyrusleeping)

* Tool changes
  * Adds 'toolkit_upgrade' to the makefile help target (@achin)

* General Codebase
  * Use extracted go-libp2p-crypto, -secio, -peer packages (@lgierth)
  * Update go-libp2p (@lgierth)
  * Fix package manifest fields (@lgierth)
  * remove incfusever dead-code (@whyrusleeping)
  * remove a ton of unused godeps (@whyrusleeping)
  * metrics: add prometheus back (@lgierth)
  * clean up dead code and config fields (@whyrusleeping)
  * Add log events when blocks are added/removed from the blockstore (@michealmure)
  * repo: don't create logs directory, not used any longer (@lgierth)

* Bugfixes
  * fixed ipfs name resolve --local multihash error (@pfista)
  * ipfs patch commands won't return null links field anymore (@whyrusleeping)
  * Make non recursive resolve print the result (@Kubuxu)
  * Output dirs on ipfs add -rn (@noffle)
  * update libp2p dep to fix hanging listeners problem (@whyrusleeping)
  * Fix Swarm.AddrFilters config setting with regard to `/ip6` addresses (@lgierth)
  * fix dht command key escaping (@whyrusleeping)

* Testing
  * Adds tests to make sure 'object patch' writes. (@noffle)
  * small sharness test for promise failure checking (@whyrusleeping)
  * sharness/Makefile: clean all BINS when cleaning (@chriscool)

* Documentation
  * Fix disconnect argument description (@richardlitt)
  * Added a note about swarm disconnect (@richardlitt)
  * Also fixed syntax for comment (@richardlitt)
  * Alphabetized swarm subcmds (@richardlitt)
  * Added note to ipfs stats bw interval option (@richardlitt)
  * Small syntax changes to repo stat man (@richardlitt)
  * update log command help text (@pfista)
  * Added a long description to add (@richardlitt)
  * Edited object patch set-data doc (@richardlitt)
  * add roadmap.md (@Jeromy)
  * Adds files api cmd to helptext (@noffle)


### 0.4.0 - 2016-04-05

This is a major release with plenty of new features and bugfixes.
It also includes breaking changes which make it incompatible with v0.3.x
on the networking layer.

* Major Changes
  * Multistream
    * The addition of multistream is a breaking change on the networking layer,
      but gives IPFS implementations the ability to mix and match different
      stream multiplexers, e.g. yamux, spdystream, or muxado.
      This adds a ton of flexibility on one of the lower layers of the protocol,
      and will help us avoid further breaking protocol changes in the future.
  * Files API
    * The new `files` command and API allow a program to interact with IPFS
      using familiar filesystem operations, namely: creating directories,
      reading, writing, and deleting files, listing out different directories,
      and so on. This feature enables any other application that uses a
      filesystem-like backend for storage, to use IPFS as its storage driver
      without having change the application logic at all.
  * Gx
    * go-ipfs now uses [gx](https://github.com/whyrusleeping/gx) to manage its
      dependencies. This means that under the hood, go-ipfs's dependencies are
      backed by IPFS itself! It also means that go-ipfs is no longer installed
      using `go get`. Use `make install` instead.
* New Features
  * Web UI
    * Update to new version which is compatible with 0.4.0. (@dignifiedquire)
  * Networking
    * Implement uTP transport. (@whyrusleeping)
    * Allow multiple addresses per configured bootstrap node. (@whyrusleeping)
  * IPNS
    * Improve IPNS resolution performance. (@whyrusleeping)
    * Have dnslink prefer `TXT _dnslink.example.com`, allows usage of CNAME records. (@Kubuxu)
    * Prevent `ipfs name publish` when `/ipns` is mounted. (@noffle)
  * Repo
    * Improve performance of `ipfs add`. (@whyrusleeping)
    * Add `Datastore.NoSync` config option for flatfs. (@rht)
    * Implement mark-and-sweep GC. (@whyrusleeping)
    * Allow for GC during `ipfs add`. (@whyrusleeping)
    * Add `ipfs repo stat` command. (@tmg, @diasdavid)
  * General
    * Add support for HTTP OPTIONS requests. (@lidel)
    * Add `ipfs diag cmds` to view active API requests (@whyrusleeping)
    * Add an `IPFS_LOW_MEM` environment variable which relaxes Bitswap's memory usage. (@whyrusleeping)
    * The Docker image now lives at `ipfs/go-ipfs` and has been completely reworked. (@lgierth)
* Security fixes
  * The gateway path prefix added in v0.3.10 was vulnerable to cross-site
    scripting attacks. This release introduces a configurable list of allowed
    path prefixes. It's called `Gateway.PathPrefixes` and takes a list of
    strings, e.g. `["/blog", "/foo/bar"]`. The v0.3.x line will not receive any
    further updates, so please update to v0.4.0 as soon as possible. (@lgierth)
* Incompatible Changes
  * Install using `make install` instead of `go get` (@whyrusleeping)
  * Rewrite pinning to store pins in IPFS objects. (@tv42)
  * Bump fs-repo version to 3. (@whyrusleeping)
  * Use multistream muxer (@whyrusleeping)
  * The default for `--type` in `ipfs pin ls` is now `all`. (@chriscool)
* Bug Fixes
  * Remove msgio double wrap. (@jbenet)
  * Buffer msgio. (@whyrusleeping)
  * Perform various fixes to the FUSE code. (@tv42)
  * Compute `ipfs add` size in background to not stall add operation. (@whyrusleeping)
  * Add option to have `ipfs add` include top-level hidden files. (@noffle)
  * Fix CORS checks on the API. (@rht)
  * Fix `ipfs update` error message. (@tomgg)
  * Resolve paths in `ipfs pin rm` without network lookup. (@noffle)
  * Detect FUSE unmounts and track mount state. (@noffle)
  * Fix go1.6rc2 panic caused by CloseNotify being called from wrong goroutine. (@rwcarlsen)
  * Bump DHT kvalue from 10 to 20. (@whyrusleeping)
  * Put public key and IPNS entry to DHT in parallel. (@whyrusleeping)
  * Fix panic in CLI argument parsing. (@whyrusleeping)
  * Fix range error by using larger-than-zero-length buffer. (@noffle)
  * Fix yamux hanging issue by increasing AcceptBacklog. (@whyrusleeping)
  * Fix double Transport-Encoding header bug. (@whyrusleeping)
  * Fix uTP panic and file descriptor leak. (@whyrusleeping)
* Tool Changes
  * Add `--pin` option to `ipfs add`, which defaults to `true` and allows `--pin=false`. (@eminence)
  * Add arguments to `ipfs pin ls`. (@chriscool)
  * Add `dns` and `resolve` commands to read-only API. (@Kubuxu)
  * Add option to display headers for `ipfs object links`. (@palkeo)
* General Codebase Changes
  * Check Golang version in Makefile. (@chriscool)
  * Improve Makefile. (@tomgg)
  * Remove dead Jenkins CI code. (@lgierth)
  * Add locking interface to blockstore. (@whyrusleeping)
  * Add Merkledag FetchGraph and EnumerateChildren. (@whyrusleeping)
  * Rename Lock/RLock to GCLock/PinLock. (@jbenet)
  * Implement pluggable datastore types. (@tv42)
  * Record datastore metrics for non-default datastores. (@tv42)
  * Allow multistream to have zero-rtt stream opening. (@whyrusleeping)
  * Refactor `ipnsfs` into a more generic and well tested `mfs`. (@whyrusleeping)
  * Grab more peers if bucket doesn't contain enough. (@whyrusleeping)
  * Use CloseNotify in gateway. (@whyrusleeping)
  * Flatten multipart file transfers. (@whyrusleeping)
  * Send updated DHT record fixes to peers who sent outdated records. (@whyrusleeping)
  * Replace go-psutil with go-sysinfo. (@whyrusleeping)
  * Use ServeContent for index.html. (@AtnNn)
  * Refactor `object patch` API to not store data in URL. (@whyrusleeping)
  * Use mfs for `ipfs add`. (@whyrusleeping)
  * Add `Server` header to API responses. (@Kubuxu)
  * Wire context directly into HTTP requests. (@rht)
  * Wire context directly into GetDAG operations within GC. (@rht)
  * Vendor libp2p using gx. (@whyrusleeping)
  * Use gx vendored packages instead of Godeps. (@whyrusleeping)
  * Simplify merkledag package interface to ease IPLD inclusion. (@mildred)
  * Add default option value support to commands lib. (@whyrusleeping)
  * Refactor merkledag fetching methods. (@whyrusleeping)
  * Use net/url to escape paths within Web UI. (@noffle)
  * Deprecated key.Pretty(). (@MichealMure)
* Documentation
  * Fix and update help text for **every** `ipfs` command. (@RichardLitt)
  * Change sample API origin settings from wildcard (`*`) to `example.com`. (@Kubuxu)
  * Improve documentation of installation process in README. (@whyrusleeping)
  * Improve windows.md. (@chriscool)
  * Clarify instructions for installing from source. (@noffle)
  * Make version checking more robust. (@jedahan)
  * Assert the source code is located within GOPATH. (@whyrusleeping)
  * Remove mentions of `/dns` from `ipfs dns` command docs. (@lgierth)
* Testing
  * Refactor iptb tests. (@chriscool)
  * Improve t0240 sharness test. (@chriscool)
  * Make bitswap tests less flaky. (@whyrusleeping)
  * Use TCP port zero for ipfs daemon in sharness tests. (@whyrusleeping)
  * Improve sharness tests on AppVeyor. (@chriscool)
  * Add a pause to fix timing on t0065. (@whyrusleeping)
  * Add support for arbitrary TCP ports to t0060-daemon.sh. (@noffle)
  * Make t0060 sharness test use TCP port zero. (@whyrusleeping)
  * Randomized ipfs stress testing via randor (@dignifiedquire)
  * Stress test pinning and migrations (@whyrusleeping)

### 0.3.11 - 2016-01-12

This is the final ipfs version before the transition to v0.4.0.
It introduces a few stability improvements, bugfixes, and increased
test coverage.

* Features
  * Add 'get' and 'patch' to the allowed gateway commands (@whyrusleeping)
  * Updated webui version (@dignifiedquire)

* BugFixes
  * Fix path parsing for add command (@djdv)
  * namesys: Make paths with multiple segments work. Fixes #2059 (@Kubuxu)
  * Fix up panic catching in http handler funcs (@whyrusleeping)
  * Add correct access control headers to the default api config (@dignifiedquire)
  * Fix closenotify by not sending empty file set (@whyrusleeping)

* Tool Changes
  * Have install.sh use the full path to ipfs binary if detected (@jedahan)
  * Install daemon system-wide if on El Capitan (@jedahan)
  * makefile: add -ldflags to install and nofuse tasks (@lgierth)

* General Codebase
  * Clean up http client code (@whyrusleeping)
  * Move api version check to header (@rht)

* Documentation
  * Improved release checklist (@jbenet)
  * Added quotes around command in long description (@RichardLitt)
  * Added a shutdown note to daemon description (@RichardLitt)

* Testing
  * t0080: improve last tests (@chriscool)
  * t0080: improve 'ipfs refs --unique' test (@chriscool)
  * Fix t.Fatal usage in goroutines (@chriscool)
  * Add docker testing support to sharness (@chriscool)
  * sharness: add t0300-docker-image.sh (@chriscool)
  * Included more namesys tests. (@Kubuxu)
  * Add sharness test to verify requests look good (@whyrusleeping)
  * Re-enable ipns sharness test now that iptb is fixed (@whyrusleeping)
  * Force use of ipv4 in test (@whyrusleeping)
  * Travis-CI: use go 1.5.2 (@jbenet)

### 0.3.10 - 2015-12-07

This patch update introduces the 'ipfs update' command which will be used for
future ipfs updates along with a few other bugfixes and documentation
improvements.


* Features
  * support for 'ipfs update' to call external binary (@whyrusleeping)
  * cache ipns entries to speed things up a little (@whyrusleeping)
  * add option to version command to print repo version (@whyrusleeping)
  * Add in some more notifications to help profile queries (@whyrusleeping)
  * gateway: add path prefix for directory listings (@lgierth)
  * gateway: add CurrentCommit to /version (@lgierth)

* BugFixes
  * set data and links nil if not present (@whyrusleeping)
  * fix log hanging issue, and implement close-notify for commands (@whyrusleeping)
  * fix dial backoff (@whyrusleeping)
  * proper ndjson implementation (@whyrusleeping)
  * seccat: fix secio context (@lgierth)
  * Add newline to end of the output for a few commands. (@nham)
  * Add fixed period repo GC + test (@rht)

* Tool Changes
  * Allow `ipfs cat` on ipns path (@rht)

* General Codebase
  * rewrite of backoff mechanism (@whyrusleeping)
  * refactor net code to use transports, in rough accordance with libp2p (@whyrusleeping)
  * disable building fuse stuff on windows (@whyrusleeping)
  * repo: remove Log config (@lgierth)
  * commands: fix description of --api (@lgierth)

* Documentation
  * --help: Add a note on using IPFS_PATH to the footer of the helptext.  (@sahib)
  * Moved email juan to ipfs/contribute (@richardlitt)
  * Added commit sign off section (@richardlitt)
  * Added a security section (@richardlitt)
  * Moved TODO doc to issue #1929 (@richardlitt)

* Testing
  * gateway: add tests for /version (@lgierth)
  * Add gc auto test (@rht)
  * t0020: cleanup dir with bad perms (@chriscool)

Note: this commit introduces fixed-period repo gc, which will trigger gc
after a fixed period of time. This feature is introduced now, disabled by
default, and can be enabled with `ipfs daemon --enable-gc`. If all goes well,
in the future, it will be enabled by default.

### 0.3.9 - 2015-10-30

This patch update includes a good number of bugfixes, notably, it fixes
builds on windows, and puts newlines between streaming json objects for a
proper ndjson format.

* Features
  * Writable gateway enabled again (@cryptix)

* Bugfixes
  * fix windows builds (@whyrusleeping)
  * content type on command responses default to text (@whyrusleeping)
  * add check to makefile to ensure windows builds don't fail silently (@whyrusleeping)
  * put newlines between streaming json output objects (@whyrusleeping)
  * fix streaming output to flush per write (@whyrusleeping)
  * purposely fail builds pre go1.5 (@whyrusleeping)
  * fix ipfs id <self> (@whyrusleeping)
  * fix a few race conditions in mocknet (@whyrusleeping)
  * fix makefile failing when not in a git repo (@whyrusleeping)
  * fix cli flag orders (long, short) (@rht)
  * fix races in http cors (@miolini)
  * small webui update (some bugfixes) (@jbenet)

* Tool Changes
  * make swarm connect return an error when it fails (@whyrusleeping)
  * Add short flag for `ipfs ls --headers` (v for verbose) (@rht)

* General Codebase
  * bitswap: clean log printf and humanize dup data count (@cryptix)
  * config: update pluto's peerID (@lgierth)
  * config: update bootstrap list hostname (@lgierth)

* Documentation
  * Pared down contribute to link to new go guidelines (@richardlitt)

* Testing
  * t0010: add tests for 'ipfs commands --flags' (@chriscool)
  * ipns_test: fix namesys.NewNameSystem() call (@chriscool)
  * t0060: fail if no nc (@chriscool)

### 0.3.8 - 2015-10-09

This patch update includes changes to make ipns more consistent and reliable,
symlink support in unixfs, mild performance improvements, new tooling features,
a plethora of bugfixes, and greatly improved tests.

NOTICE: Version 0.3.8 also requires golang version 1.5.1 or higher.

* Bugfixes
  * refactor ipns to be more consistent and reliable (@whyrusleeping)
  * fix 'ipfs refs' json output (@whyrusleeping)
  * fix setting null config maps (@rht)
  * fix output of dht commands (@whyrusleeping)
  * fix NAT spam dialing (@whyrusleeping)
  * fix random panics on 32 bit systems (@whyrusleeping)
  * limit total number of network fd's (@whyrusleeping)
  * fix http api content type (@WeMeetAgain)
  * fix writing of api file for port zero daemons (@whyrusleeping)
  * windows connection refused fixes (@mjanczyk)
  * use go1.5's built in trailers, no more failures (@whyrusleeping)
  * fix random bitswap hangs (@whyrusleeping)
  * rate limit fd usage (@whyrusleeping)
  * fix panic in bitswap ratelimiting (@whyrusleeping)

* Tool Changes
  * --empty-repo option for init (@prusnak)
  * implement symlinks (@whyrusleeping)
  * improve cmds lib files processing (@rht)
  * properly return errors through commands (@whyrusleeping)
  * bitswap unwant command (@whyrusleeping)
  * tar add/cat commands (@whyrusleeping)
  * fix gzip compression in get (@klauspost)
  * bitswap stat logs wasted bytes (@whyrusleeping)
  * resolve command now uses core.Resolve (@rht)
  * add `--local` flag to 'name resolve' (@whyrusleeping)
  * add `ipfs diag sys` command for debugging help (@whyrusleeping)

* General Codebase
  * improvements to dag editor (@whyrusleeping)
  * swarm IPv6 in default config (Baptiste Jonglez)
  * improve dir listing css (@rht)
  * removed elliptic.P224 usage (@prusnak)
  * improve bitswap providing speed (@jbenet)
  * print panics that occur in cmds lib (@whyrusleeping)
  * ipfs api check test fixes (@rht)
  * update peerstream and datastore (@whyrusleeping)
  * cleaned up tar-reader code (@jbenet)
  * write context into coreunix.Cat (@rht)
  * move assets to separate repo (@rht)
  * fix proc/ctx wiring in bitswap (@jbenet)
  * rabin fingerprinting chunker (@whyrusleeping)
  * better notification on daemon ready (@rht)
  * coreunix cat cleanup (@rht)
  * extract logging into go-log (@whyrusleeping)
  * blockservice.New no longer errors (@whyrusleeping)
  * refactor ipfs get (@rht)
  * readonly api on gateway (@rht)
  * cleanup context usage all over (@rht)
  * add xml decoding to 'object put' (@ForrestWeston)
  * replace nodebuilder with NewNode method (@whyrusleeping)
  * add metrics to http handlers (@lgierth)
  * rm blockservice workers (@whyrusleeping)
  * decompose maybeGzWriter (@rht)
  * makefile sets git commit sha on build (@CaioAlonso)

* Documentation
  * add contribute file (@RichardLitt)
  * add go devel guide to contribute.md (@whyrusleeping)

* Testing
  * fix mock notifs test (@whyrusleeping)
  * test utf8 with object cmd (@chriscool)
  * make mocknet conn close idempotent (@jbenet)
  * fix fuse tests (@pnelson)
  * improve sharness test quoting (@chriscool)
  * sharness tests for chunker and add-cat (@rht)
  * generalize peerid check in sharness (@chriscool)
  * test_cmp argument cleanup (@chriscool)

### 0.3.7 - 2015-08-02

This patch update fixes a problem we introduced in 0.3.6 and did not
catch: the webui failed to work with out-of-the-box CORS configs.
This has been fixed and now should work correctly. @jbenet

### 0.3.6 - 2015-07-30

This patch improves the resource consumption of go-ipfs,
introduces a few new options on the CLI, and also
fixes (yet again) windows builds.

* Resource consumption:
  * fixed goprocess memory leak @rht
  * implement batching on datastore @whyrusleeping
  * Fix bitswap memory leak @whyrusleeping
  * let bitswap ignore temporary write errors @whyrusleeping
  * remove logging to disk in favor of api endpoint @whyrusleeping
  * --only-hash option for add to skip writing to disk @whyrusleeping

* Tool changes
  * improved `ipfs daemon` output with all addresses @jbenet
  * improved `ipfs id -f` output, added `<addrs>` and  `\n \t` support @jbenet
  * `ipfs swarm addrs local` now shows the local node's addrs @jbenet
  * improved config json parsing @rht
  * improved Dockerfile to use alpine linux @Luzifer @lgierth
  * improved bash completion @MichaelMure
  * Improved 404 for gateway @cryptix
  * add unixfs ls to list correct filesizes @wking
  * ignore hidden files by default @gatesvp
  * global --timeout flag @whyrusleeping
  * fix random API failures by closing resp bodies @whyrusleeping
  * ipfs swarm filters @whyrusleeping
  * api returns errors in http trailers @whyrusleeping @jbenet
  * `ipfs patch` learned to create intermediate nodes @whyrusleeping
  * `ipfs object stat` now shows Hash @whyrusleeping
  * `ipfs cat` now clears progressbar on exit @rht
  * `ipfs add -w -r <dir>` now wraps directories @jbenet
  * `ipfs add -w <file1> <file2>` now wraps with one dir @jbenet
  * API + Gateway now support arbitrary HTTP Headers from config @jbenet
  * API now supports CORS properly from config @jbenet
  * **Deprecated:** `API_ORIGIN` env var (use config, see `ipfs daemon --help`) @jbenet

* General Codebase
  * `nofuse` tag for windows @Luzifer
  * improved `ipfs add` code @gatesvp
  * started requiring license trailers @chriscool @jbenet
  * removed CtxCloser for goprocess @rht
  * remove deadcode @lgierth @whyrusleeping
  * reduced number of logging libs to 2 (soon to be 1) @rht
  * dial address filtering @whyrusleeping
  * prometheus metrics @lgierth
  * new index page for gateway @krl @cryptix
  * move ping to separate protocol @whyrusleeping
  * add events to bitswap for a dashboard @whyrusleeping
  * add latency and bandwidth options to mocknet @heems
  * levenshtein distance cmd autosuggest @sbruce
  * refactor/cleanup of cmds http handler @whyrusleeping
  * cmds http stream reports errors in trailers @whyrusleeping

* Bugfixes
  * fixed path resolution and validation @rht
  * fixed `ipfs get -C` output and progress bar @rht
  * Fixed install pkg dist bug @jbenet @Luzifer
  * Fix `ipfs get` silent failure   @whyrusleeping
  * `ipfs get` tarx no longer times out @jbenet
  * `ipfs refs -r -u` is now correct @gatesvp
  * Fix `ipfs add -w -r <dir>` wrapping bugs @jbenet
  * Fixed FUSE unmount failures @jbenet
  * Fixed `ipfs log tail` command (api + cli) @whyrusleeping

* Testing
  * sharness updates @chriscool
  * ability to disable secio for testing @jbenet
  * fixed many random test failures, more reliable CI @whyrusleeping
  * Fixed racey notifier failures @whyrusleeping
  * `ipfs refs -r -u` test cases @jbenet
  * Fix failing pinning test @jbenet
  * Better CORS + Referer tests @jbenet
  * Added reversible gc test @rht
  * Fixed bugs in FUSE IPNS tests @whyrusleeping
  * Fixed bugs in FUSE IPFS tests @jbenet
  * Added `random-files` tool for easier sharness tests @jbenet

* Documentation
  * Add link to init system examples @slang800
  * Add CORS documentation to daemon init @carver  (Note: this will change soon)

### 0.3.5 - 2015-06-11

This patch improves overall stability and performance

* added 'object patch' and 'object new' commands @whyrusleeping
* improved symmetric NAT avoidance @jbenet
* move util.Key to blocks.Key @whyrusleeping
* fix memory leak in provider store @whyrusleeping
* updated webui to 0.2.0 @krl
* improved bitswap performance @whyrusleeping
* update fuse lib @cryptix
* fix path resolution @wking
* implement test_seq() in sharness @chriscool
* improve parsing of stdin for commands @chriscool
* fix 'ipfs refs' failing silently @whyrusleeping
* fix serial dialing bug @jbenet
* improved testing @chriscool @rht @jbenet
* fixed domain resolving @luzifer
* fix parsing of unwanted stdin @lgierth
* added CORS handlers to gateway @NodeGuy
* added `ipfs daemon --unrestricted-api` option @krl
* general cleanup of dependencies

### 0.3.4 - 2015-05-10

* fix ipns append bug @whyrusleeping
* fix out of memory panic @whyrusleeping
* add in expvar metrics @tv42
* bitswap improvements @whyrusleeping
* fix write-cache in blockstore @tv42
* vendoring cleanup @cryptix
* added `launchctl` plist for OSX @grncdr
* improved Dockerfile, changed root and mount paths @ehd
* improved `pin ls` output to show types @vitorbaptista

### 0.3.3 - 2015-04-28

This patch update fixes various issues, in particular:
- windows support (0.3.0 had broken it)
- commandline parses spaces correctly.

* much improved commandline parsing by @AtnNn
* improved dockerfile by @luzifer
* add cmd cleanup by @wking
* fix flatfs windows support by @tv42 and @gatesvp
* test case improvements by @chriscool
* ipns resolution timeout bug fix by @whyrusleeping
* new cluster tests with iptb by @whyrusleeping
* fix log callstack printing bug by @whyrusleeping
* document bash completion by @dylanPowers

### 0.3.2 - 2015-04-22

This patch update implements multicast dns as well as fxing a few test issues.

* implement mdns peer discovery @whyrusleeping
* fix mounting issues in sharness tests @chriscool

### 0.3.1 - 2015-04-21

This patch update fixes a few bugs:

* harden shutdown logic by @torarnv
* daemon locking fixes by @travisperson
* don't re-add entire dirs by @whyrusleeping
* tests now wait for graceful shutdown by @jbenet
* default key size is now 2048 by @jbenet

### 0.3.0 - 2015-04-20

We've just released version 0.3.0, which contains many
performance improvements, bugfixes, and new features.
Perhaps the most noticeable change is moving block storage
from leveldb to flat files in the filesystem.

What to expect:

* _much faster_ performance

* Repo format 2
  * moved default location from ~/.go-ipfs -> ~/.ipfs
  * renamed lock filename daemon.lock -> repo.lock
  * now using a flat-file datastore for local blocks

* Fixed lots of bugs
  * proper ipfs-path in various commands
  * fixed two pinning bugs (recursive pins)
  * increased yamux streams window (for speed)
  * increased bitswap workers (+ env var)
  * fixed memory leaks
  * ipfs add error returns
  * daemon exit bugfix
  * set proper UID and GID on fuse mounts

* Gateway
  * Added support for HEAD requests

* configuration
  * env var to turn off SO_REUSEPORT: IPFS_REUSEPORT=false
  * env var to increase bitswap workers: IPFS_BITSWAP_TASK_WORKERS=n

* other
  * bash completion is now available
  * ipfs stats bw -- bandwidth meetering

And many more things.

### 0.2.3 - 2015-03-01

* Alpha Release

### 2015-01-31:

* bootstrap addresses now have .../ipfs/... in format
  config file Bootstrap field changed accordingly. users
  can upgrade cleanly with:

      ipfs bootstrap >boostrap_peers
      ipfs bootstrap rm --all
      <install new ipfs>
      <manually add .../ipfs/... to addrs in bootstrap_peers>
      ipfs bootstrap add <bootstrap_peers
