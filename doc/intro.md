# Introduction to Noms

Most conventional database systems share two central properties:

1. Data is modeled as a single point-in-time. Once a transaction commits, the previous state of the database is either lost, or available only as a fallback by reconstructing from transaction logs.

2. Data is modeled as a single source of truth. Even large-scale distributed databases which are internally a fault-tolerant network of nodes, present the abstraction to clients of being a single logical master, with which clients must coordinate in order to change state.

Noms blends the properties of decentralized systems, such as [Git](https://git-scm.com/), with properties of traditional databases in order to create a general-purpose decentralized database, in which:

1. Any peer’s state is as valid as any other.

2. All commits of the database are retained and available at any time.

3. Any peer is free to move forward independently of communication from any other—while retaining the ability to reconcile changes at some point in the future.

4. The basic properties of structured databases (efficient queries, updates, and range scans) are retained.

5. Diffs between any two sets of data can be computed efficiently.

6. Synchronization between disconnected copies of the database can be performed efficiently and correctly.

## Basics

As in Git, [Bitcoin](https://bitcoin.org/en/), [Ethereum](https://www.ethereum.org/), [IPFS](https://ipfs.io/), [Camlistore](https://camlistore.org/), [bup](https://bup.github.io/), and other systems, Noms models data as a [directed acyclic graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) of nodes in which every node has a _hash_. A node's hash is derived from the values encoded in the node and (transitively) from the values encoded in all nodes which are reachable from that node.

In other words, a Noms database is a single large [Merkle DAG](https://github.com/jbenet/random-ideas/issues/20).

When two nodes have the same hash, they represent identical logical values and the respective subgraph of nodes reachable from each are topologically equivalent. Importantly, in Noms, the reverse is also true: a single logical value has one and only one hash. When two nodes have differnet hashes, they represent different logical values.

Noms extends the ideas of prior systems to enable efficiently computing and reconciling differences, synchronizing state, and building indexes over large-scale, structured data.

## Databases and Datasets

A _database_ is the top-level abstraction in Noms.

A database has two responsibilities: it provides storage of [content-addressed](https://en.wikipedia.org/wiki/Content-addressable_storage) chunks of data, and it keeps track of zero or more _datasets_.

A Noms database can be implemented on top of any underlying storage system that provides key/value storage with at least optional optimistic concurrency. We only use optimistic concurrency to store the current value of each dataset. Chunks themselves are immutable.

We have implementations of Noms databases on top of our own file-backed store [Noms Block Store (NBS)](https://github.com/attic-labs/noms/tree/master/go/nbs) (usually used locally), our own [HTTP protocol](https://github.com/attic-labs/noms/blob/master/go/datas/database_server.go) (used for working with a remote database), [Amazon DynamoDB](https://aws.amazon.com/dynamodb/), and [memory](https://github.com/attic-labs/noms/blob/master/go/chunks/memory_store.go) (mainly used for testing).

Here's an example of creating an http-backed database using the [Go Noms SDK](go-tour.md):

```go
package main

import (
  "fmt"
  "os"

  "github.com/attic-labs/noms/go/spec"
)

func main() {
  sp, err := spec.ForDatabase("http://localhost:8000")
  if err != nil {
    fmt.Fprintf(os.Stderr, "Could not access database: %s\n", err)
    return
  }
  defer sp.Close()
}
```

A dataset is nothing more than a named pointer into the DAG. Consider the following command to copy the dataset named `foo` to the dataset named `bar` within a database:

```
noms sync http://localhost:8000::foo http://localhost:8000::bar
```

This command is trivial and causes basically zero IO. Noms first resolves the dataset name `foo` in `http://localhost:8000`. This results in a hash. Noms then checks whether that hash exists in the destination database (which in this case is the same as the source database), finds that it does, and then adds a new dataset pointing at that chunk.

Syncs across database can be efficient by the same logic if the destination database already has all or most of the chunks required chunks.

## Time

All data in Noms is immutable. Once a piece of data is stored, it is never changed. To represent state changes, Noms uses a progression of `Commit`  structures.

[TODO - diagram]

As in Git, Commits typically have one _parent_, which is the previous commit in time. But in the cases of merges, a Noms commit can have multiple parents.

### Chunks

When a value is stored in Noms, it is stored as one or more chunks of data.  Chunk boundaries are typically created implicitly, as a way to store large collections efficiently (see [Prolly Trees](#prolly-trees-probabilistic-b-trees)). Programmers can also create explicit chunk boundaries using the `Ref` type (see [Types](#types )).

[TODO - Diagram]

Every chunk encodes a single logical value (which may be a component of another value and/or be composed of sub-values). Chunks are [addressed](https://en.wikipedia.org/wiki/Content-addressable_storage) in the Noms persistence layer by the hash of the value they encode.

## Types

Noms is a typed system, meaning that every Noms value is classified into one of the following _types_:

* `Boolean`
* `Number` (arbitrary precision decimal)
* `String` (utf8-encoded)
* `Blob` (raw binary data)
* User-defined structs
* `Set<T>`
* `List<T>`
* `Map<K,V>`
* Unions: `T|U|V|...`
* `Cycle<int>` (allows the creation of cyclic types)
* `Ref<T>` (explicit out-of-line references)

Blobs, sets, lists, and maps can be gigantic - Noms will _chunk_ these types into reasonable sized parts internally for efficient storage, searching, and updating (see [Prolly Trees](#prolly-trees-probabilistic-b-trees) below for more on this).

Strings, numbers, unions, and structs are not chunked, and should be used for "reasonably-sized" values. Use `Ref` if you need to force a particular value to be in a different chunk for some reason.

Types serve several purposes in Noms:

1. Most importantly, types allow Noms data to be self-describing. Every Noms chunk has a header that describes its type. This means that anyone (or any software) can look at the header of a chunk and know the shape of all the data reachable via that chunk with some precision. This makes writing code that works with Noms much less error-prone. In languages that support generics, one can work with Noms data in a completely statically typed way.

2. Users of Noms can define their own structures and publish data that uses them. This allows for ad-hoc standardization of types within communities working on similar data.

3. Types can be used _structurally_. A program can check incoming data against a required type. If the incoming root chunk matches the type, or is a superset of it, then the program can proceed with certainty of the shape of all accessible data. This enables richer interoperability between software, since schemas can be expanded over time as long as a compatible subset remains.

4. Eventually, we plan to add type restrictions to datasets, which would enforce the allowed types that can be committed to a dataset. This would allow something akin to schema validation in traditional databases.

### Refs vs Hashes

A _hash_ in Noms is just like the hashes used elsewhere in computing: a short string of bytes that uniquely identifies a larger value. Every value in Noms has a hash. Noms currently uses the [sha2-512](https://github.com/attic-labs/noms/blob/master/go/hash/hash.go#L7) hash function, but that can change in future versions of the system.

A _ref_ is different in subtle, but important ways. A `Ref` is a part of the type system - a `Ref` is a value. Anywhere you can find a Noms value, you can find a `Ref`. For example, you can commit a `Ref<T>` to a dataset, but you can't commit a bare hash.

The difference is that `Ref` carries the type of its target, along with the hash. This allows us to efficiently validate commits that include `Ref`, among other things.

### Type Accretion

Noms is an immutable database, which leads to the question: How do you change the schema? If I have a dataset containing `Set<Number>`, and I later decide that it should be `Set<String>`, what do I do?

You might say that you just commit the new type, but that would mean that users can't look at a dataset and understand what types previous versions contained, without manually exploring every one of those commits.

We call our solution to this problem _Type Accretion_.

If you construct a `Set` containing only `Number`s, its type will be `Set<Number>`. If you then insert a string into this set, the type of the resulting value is `Set<Number|String>`.

This is usually completely implicit, done based on the data you store (you can set types explicitly though, which is useful in some cases).

We do the same thing for datasets. If you commit a `Set<Number>`, the type of the commit we create for you is:

```
struct Commit {
	Value: Set<Number>
	Parents: Set<Ref<Cycle<Commit>>>
}
```

This tells you that the current and all previous commits have values of type `Set<Number>`.

But if you then commit a `Set<String>` to this same dataset, then the type of that commit will be:

```
struct Commit {
	Value: Set<String>
	Parents: Set<Ref<Cycle<Commit>> |
		Ref<struct Commit {
			Value: Set<Number>
			Parents: Cycle<Commit>
		}>>
	}
}
```

This tells you that the dataset's current commit has a value of type `Set<String>` and that previous commits are either the same, or else have a value of type `Set<Number>`.

Type accretion has a number of benefits related to schema changes:

1. You can widen the type of any container (list, set, map) without rewriting any existing data. `Set<struct { name: String }>` becomes `Set<struct { name: String }> | struct { name: String, age: Number }>>` and all existing data is reused.
2. You can widen containers in ways that other databases wouldn't allow. For example, you can go from `Set<Number>` to `Set<Number|String>`. Existing data is still reused.
3. You can change the type of a dataset in either direction - either widening or narrowing it, and the dataset remains self-documenting as to its current and previous types.

## Prolly Trees: Probabilistic B-Trees

A critical invariant of Noms is that the same value will be represented by the same graph, having the same chunk boundaries, regardless of what past sequence of logical mutations resulted in the value. This is the essence of content-addressing and it is what makes deduplication, efficient sync, indexing, and and other features of Noms possible.

But this invariant also rules out the use of classical B-Trees, because a B-Tree’s internal state depends upon its mutation history. In order to model large mutable collections in Noms, of the type where B-Trees would typically be used, Noms instead introduces _Prolly Trees_.

A Prolly Tree is a [search tree](https://en.wikipedia.org/wiki/Search_tree) where the number of values stored in each node is determined probabilistically, based on the data which is stored in the tree.

A Prolly Tree is similar in many ways to a B-Tree, except that the number of values in each node has a probabilistic average rather than an enforced upper and lower bound, and the set of values in each node is determined by the output of a rolling hash function over the values, rather than via split and join operations when upper and lower bounds are exceeded.

### Indexing and Searching with Prolly Trees

Like B-Trees, Prolly Trees are sorted. Keys of type Boolean, Number, and String sort in their natural order. Other types sort by their hash.

Because of this sorting, Noms collections can be used as efficient indexes, in the same manner as primary and secondary indexes in traditional databases.

For example, say you want to quickly be able to find `Person` structs by their age. You could build a map of type `Map<Number, Set<Person>>`. This would allow you to quickly (~log<sub>k</sub>(n) seeks, where `k` is average prolly tree width, which is currently 64) find all the people of an exact age. But it would _also_ allow you to find all people within a range of ages efficiently (~num_results/log<sub>k</sub>(n) seeks), even if the ages are non-integral.

Also, because Noms collections are ordered search trees, it is possible to implement set operations like union and intersect efficiently on them.

So, for example, if you wanted to find all the people of a particular age AND having a particular hair color, you could construct a second map having type `Map<String, Set<Person>>`, and intersect the two sets.

Over time, we plan to develop this basic capability into support for some kind of generalized query system.
