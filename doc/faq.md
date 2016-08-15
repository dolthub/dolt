# Frequently Asked Questions

### How is this a database? There's not even a query language!

First, we do plan to have some sort query language one day. But such a thing is sort of like the last step of a long journey that we are just at the beginning of. Right now we are working on the underlying primitives that would allow a query system to be built and work well.

Second, the term *database* isn't well-defined. There are a lot of different systems that call themselves databases.

We like to think the word has a certain heft, and certain expectations that go along with it: A database should deal with lots of small, structured records well. A database should never lose or corrupt data. A database should support efficient queries on different attributes.

Noms can and should be a database, not merely a datastore. We're not quite there yet, but we're off to a good start, and we do think we have the [necessary tools](https://github.com/attic-labs/noms/blob/master/doc/intro.md#prolly-trees-probabilistic-b-trees).

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

Currently, Noms doesn't handle them at all. But in the near future, we plan to add [automatic merge policies](https://github.com/attic-labs/noms/issues/148) for several common cases of conflicts.

Similar to how Git uses simple heuristics to successfully merge many classes of edits to same same file/directory, we believe that we can use some simple heuristics to merge Noms data. In fact, we think we can even do better than Git, because in general Noms data is more granular than data in Git. We have structs, sets, lists, and maps, where Git just has directories and files.

Whether to use an auto-merge policy will be something that developers choose on a case-by-case basis and can always opt-out of, or combine with custom code, for more complex scenarios.

### Why don't you just use CRDTs?

We want Noms to provide traditional data structures that programmers are familiar with, even if that means conflict resolution must be done manually in some cases. We think we can provide a few options of conflict resolution policies for each core data structure kind that should meet most needs.

[Convergent (or Commutative) Replicated Data Types (CRDTs)](http://hal.upmc.fr/inria-00555588/document) are a class of distributed data structures that provably converge to some agreed-upon state with no synchronization. State-Based CRDTs operate by having nodes send the entirety of their local state to one another, which is something that Noms is designed to be very good at, so the inclination to put the two together is understandable. That said, the semantics of CRDT are generally unfamiliar to most users; e.g. a Set in which elements can only be added, removed, and then never added again (barring some synchronization-requiring “garbage collection” step). That said, we're still interested in perhaps using some form of sequence CRDT for merging lists -- this is still under investigation.

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

### You don't know what you're doing!

Not a question. But, fair enough.
