[Home](../README.md) »

[Technical Overview](intro.md)&nbsp; | &nbsp;[Use Cases](../README.md#use-cases)&nbsp; | &nbsp;[Command-Line Interface](cli-tour.md)&nbsp; | &nbsp;[Go bindings Tour](go-tour.md) | &nbsp;[Path Syntax](spelling.md)&nbsp; | &nbsp;**FAQ**&nbsp;
<br><br>
# Frequently Asked Questions

### Decentralized like BitTorrent?

No, decentralized like Git.

Specifically, Noms isn't itself a peer-to-peer network. If you can get two instances to share data, somehow, then they can synchronize. Noms doesn't define how this should happen though.

Currently, instances mainly share data via either HTTP/DNS or a filesystem. But it should be easy to add other mechanisms. For example, it seems like Noms could run well on top of BitTorrent, or IPFS. You should [look into it](https://github.com/attic-labs/noms/issues/2123).

### Isn't it wasteful to store every version?

Noms deduplicates chunks of data that are identical within one database. So if multiple versions of one dataset share a lot of data, or if the same data is present in multiple datasets, Noms only stores one copy.

That said, it is definitely possible to have write patterns that defeat this. Deduplication is done at the chunk level, and chunks are currently set to an average size of 4KB. So if you change about 1 byte in every 4096 in a single commit, and those changed bytes are well-distributed throughout the dataset, then we will end up making a complete copy of the dataset.

### Is there a way to not store the entire history?

Theoretically, definitely. In Git, for example, the concept of "shallow clones" exists, and we could do something similar in Noms. This has not been implemented yet.

### How does Noms handle conflicts?

Noms provides several built-in policies that can automatically merge common cases of conflicts. For example concurrent edits to sets are always mergeable and concurrent edits to different keys in a map or struct are also mergeable.

The conflict resolution system is pluggable so new policies that are application-specific can be added. However, it's possible to build surprisingly complex applications with just the built-in policies.

### Why don't you just use CRDTs?

[Convergent (or Commutative) Replicated Data Types (CRDTs)](http://hal.upmc.fr/inria-00555588/document) are a class of distributed data structures that provably converge to some agreed-upon state with no synchronization. Stated differently: CRDTs define a merge policy that is commutative over all their operations.

CRDTs are nice because they require no custom conflict/merge code from the developer.

Noms defines a set of intutive built-in merge policies for its core datatypes. For example, the default policy makes all operations on Noms Sets commute (add wins in the case of concurrent remove/add). This means that with the default policy, Noms Sets are a CRDT.

If your application uses only operations on Noms datatypes that can be merged with whatever merge policy you are using, then your schema is a CRDT. It's possible to build surprisingly complex applications this way with just the default policy.

Noms also allows you to provide your own custom policy. If your policy commutes, then the resulting datatype will be a CRDT.

However, it would be nice if application developers could more easily opt-in to using only mergeable operations, thereby enforcing that their schema is a CRDT, and providing confidence that custom merge logic doesn't need to be implemented.

More generally, perhaps there could be a way to test that all possible conflict cases have been handled by the developer. This would allow developers to implement their own custom CRDTs. This is something we'd like to research in the future.

### Why don't you support Windows?

We are a tiny team and we all personally use Macs as our development machines, and we use Linux in production. These two platforms are very close to identical, and so we can generally test on Mac and assume it will work on Linux. Adding Windows would add significant complexity to our code and build processes which we're not willing to take on.

### But you'll accept patches for Windows, right?

No, because then we'll have to maintain those patches.

### Are there any workaround for Windows?

You can use it in a virtual machine. We have also heard Noms works OK with gitbash or cygwin, but that's coincidence.

### Why is it called Noms?

1. It's insert-only. OMNOMNOM.
2. It's content addressed. Every value has its own hash, or [name](http://dictionary.reverso.net/french-english/nom).

### Are you sure Noms doesn't stand for something?

Pretty sure. But if you like, you can pretend it stands for Non-Mutable Store.
