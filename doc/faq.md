# Frequently Asked Questions

### How is this a database? There's not even a query language!

First, we do plan to have one someday, but a general query system is sort of like the last step of a long journey that we are just at the beginning of. Right now we are working on the underlying primitives that would allow a query system to be built and work well.

Second, the term *database* isn't well-defined. There are a lot of different systems that call themselves databases.

We like to think the word has a certain heft, and certain expectations that go along with it: A database should deal with lots of small, structured records well. A database should never lose or corrupt data. A database should support efficient queries on different attributes.

Noms can and should be a database, not merely a datastore. We're not quite there yet, but we're off to a good start, and we do think we have the [necessary tools](https://github.com/attic-labs/noms/blob/master/doc/intro.md#prolly-trees-probabilistic-b-trees).

### Decentralized like BitTorrent?

No, decentralized like Git.

Specifically, Noms isn't itself a peer-to-peer network. If you can get two instances to share data, somehow, then they can synchronize. Noms doesn't define how this should happen though. 

Currently, instances mainly share data via either HTTP/DNS or a filesystem. But it should be easy to add other mechanisms. For example, it seems like Noms could run well on top of BitTorrent, or IPFS. You should [look into it](https://github.com/attic-labs/noms/issues/2123).

### Why is it called Noms?

1. It's insert-only. OMNOMNOM.
2. It's content addressed. Every value has its own hash, or [name](http://dictionary.reverso.net/french-english/nom).

### Are you sure Noms doesn't stand for something?

Pretty sure. But if you like, you can pretend it stands for Non-Mutable Store.

### You don't know what you're doing!

Not a question. But, fair enough.
