# How It Works - Noms Design Overview

Most conventional database systems share two central properties:

1. Data is modeled as a single point-in-time. Once a transaction commits, the previous state of the database is either lost, or available only as a fallback by reconstructing from transaction logs.
2. Data is modeled as a single source of truth. Even large-scale distributed databases which are internally a fault-tolerant network of nodes, present the abstraction to clients of being a single logical master, with which clients must coordinate in order to change state.

Noms blends the properties of decentralized systems, such as Git, with properties of traditional databases in order to create a general-purpose decentralized database, in which:

1. Any peer’s state is as valid as any other
2. All commit-states of the database are retained and available at any time. 
3. Any peer is free to move forward independently of communication from any other -- while retaining the ability to reconcile changes at some point in the future.

## Basics

The central idea of Noms is similar that of Git: model data as a directed graph of nodes in which every node has a hash value which is derived from the values encoded in the node and (transitively) from the values encoded in all nodes which are reachable from that node (IOW, as  a “[Merkle DAG](https://github.com/jbenet/random-ideas/issues/20)”). When two nodes have the same hash, they represent identical logical values and the respective subgraph of nodes reachable from each are topologically equivalent. This allows for efficient operations such as computing and reconciling differences and synchronizing state.

## Databases and Datasets

A _database_ is the largest unit of granularity in Noms. A database has two responsibilities: it provides storage of content-addressed chunks of data, and it keeps track of zero or more _datasets_. We have implementations of Noms databases on top of [LevelDB](https://github.com/google/leveldb) (usually used locally), our own HTTP protocol (used for working with a remote database), and Amazon Dynamo.

A dataset is the unit of atomicy in Noms - it's the thing that is versioned. You commit transactions against a dataset to move the world forward. Datasets are very lightweight - they are essentially named pointers into the graph.

See [Spelling in Noms](spelling.md) for more on accessing databases and datasets.

## Modeling data as a graph

Noms models any logical value as a [directed acyclic graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) which has exactly one root node, zero or more descendent nodes and exactly one corresponding hash value, which is deterministically derived from the data of the value itself.

[TODO - diagram]

A Commit represents the state of a Noms dataset at a point in time. Changes to state are represented by progressions of commits. All values, including commits, are immutable.

### Chunks

When a value is stored in Noms, it is stored as one or more chunks of data.  Chunk boundaries are typically created implicitly, as a way to store large collections efficiently (see [Prolly Trees](#prolly-trees-probabilistic-b-trees)). Programmers can also create explicit chunk boundaries using the `Ref` type (see [Types](#types )).

[TODO - Diagram]

Every chunk encodes a single logical value (which may be a component of another value and/or be composed of sub-values). Chunks are [addressed](https://en.wikipedia.org/wiki/Content-addressable_storage) in the Noms persistence layer by the hash of the value they encode.

## Types

All values in Noms are _typed_. Noms supports the following types:

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

Blobs, sets, lists, and maps can be gigantic - Noms will _chunk_ these types into reasonable sized parts internally for efficient searching and updating (see [Prolly Trees](#prolly-trees-probabilistic-b-trees) below for more on this).

Strings, numbers, unions, and structs are not chunked, and should be used for "reasonably-sized" values. Use `Ref` if you need to force a particular value to be in a different chunk for some reason.

Noms data is self-describing: each value caries its type. For example, you can see the type of any Noms value with the `noms show` command. This allows one to sync a dataset, get the type of that dataset, then write code against that type with certainty that all data within the dataset will work with the code you write. You can even generate statically-typed bindings for Noms types.

### Refs vs Hashes

A _hash_ in Noms is just like the hashes used elsewhere in computing: a short string of bytes that uniquely identifies a larger value. Every value in Noms has a hash. We currently use sha1, but that will [probably change](https://github.com/attic-labs/noms/issues/279).

A _ref_ is subtley, but importantly different. A `Ref` is a part of the type system - a `Ref` is a value. Anywhere you can find a Noms value, you can find a `Ref`. For example, you can commit a `Ref<T>` to a dataset, but you can't commit a bare hash.

The difference is that `Ref` carries the type of its target, along with the hash. This allows us to efficiently validate commits that include `Ref`, among other things.

### Type Accretion

Noms is an immutable database, which leads to the question: how do you change the schema? If I have a dataset containing `Set<Number>`, and I later decide that it should be `Set<String>`, what do I do?

You might say that you just commit the new type, but that would mean that users can't look at a dataset and understand what types previous versions contained, without manually exploring every one of those commits.

Our solution to this problem is _type accretion_. For example, if you construct a `Set`, and you put some `Number`s in it, its type is `Set<Number>`. If you then put some strings in the set, its type becomes `Set<Number|String>`.

We do the same thing for datasets. If you commit a `Set<Number>`, the type of the commit we create for you is:

```
struct Commit {
	Value: Set<Number>
	Parents: Set<Ref<Cycle<0>>>
}
```

This tells you that the current and all previous commits have values of type `Set<Number>`.

But if you then commit a `Set<String>` to this same dataset, then the type of that commit will be:

```
struct Commit {
	Value: Set<String>
	Parents: Set<Ref<Cycle<0>> |
		Ref<struct Commit {
			Value: Set<Number>
			Parents: Cycle<0>
		}>>
	}
}
```

This tells you that the dataset's current commit has a value of type `Set<String>` and that previous commits are either the same, or else have a value of type `Set<Number>`.

Type accretion has a number of benefits around schema changes:

1. You can widen the type of any container (list, set, map) without rewriting any existing data. `Set<Struct<Name:String>>` becomes `Set<Struct<Name:String>|Struct<Name:String,Age:Number>>` and all existing data is reused.
2. You can widen containers in ways that other typed datastores wouldn't allow. For example, you can go from `Set<Number>` to `Set<Number|String>`. Existing data is still reused.
3. You can change the type of a dataset in either direction - either widening or narrowing it, and the dataset remains self-documenting as to its current and previous types.

### Type Validation

We haven't implemented this yet, but it's easy to see how when every value carries its type, one can implement type validation. We will have some way to annotate a dataset with the allowed types that can be committed there, then we will simply check this against incoming values on commit.

## Prolly Trees: Probabilistic B-Trees

A critical invariant of Noms is that the same value will be represented by the same graph, having the same chunk boundaries, regardless of what sequence of logical mutations resulted in the present value. This is the essence of content-addressing and it is what makes deduplication, efficient sync, indexing, and and other features of Noms possible.

But this invariant also rules out the use of classical B-Trees, because a B-Tree’s internal state depends upon its mutation history. In order to model large mutable collections, where B-Trees would typically be used, Noms instead introduces _Prolly Trees_.

A Prolly Tree is a [search tree](https://en.wikipedia.org/wiki/Search_tree) where the number of values stored in each node is determined probabilistically, based on the data which is stored in the tree.

A Prolly Tree is similar in many ways to a B-Tree, except that the number of values in each node has a probabilistic average rather than an enforced upper and lower bound, and the set of values in each node is determined by the output of a rolling hash function over the values, rather than via split and join operations when upper and lower bounds are exceeded.

### Indexing and Searching with Prolly Trees

Like B-Trees, Prolly Trees are sorted. Keys of type Boolean, Number, and String sort in their natural order. Other types sort by their ref.

Because of this sorting, Noms collections can be used as efficient indexes. For example, say you want to quickly be able to find people by their age. You could build a map with type `Map<Number, Set<Person>>`. This would allow you to quickly (~log<sub>k</sub>(n) seeks, where `k` is average prolly tree width - currently 64) find all the people of an exact age. But it would _also_ allow you to find all people within a range of ages efficiently (~num_results/log<sub>k</sub>(n) seeks), even if the ages are non-integral.

Not only that, but say you also wanted to be able to find people by eye color. You could setup a different map with type `Map<String, Set<Person>>`. Because `Set` is also sorted (in this case, by `Person.Ref()`), we can do efficient compound boolean queries.

