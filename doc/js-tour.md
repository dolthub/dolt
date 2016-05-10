# A Short Tour of Noms for JavaScript

This is a short introduction to using Noms from JavaScript. It should only take a few minutes if you have some familiarity with JavaScript and Node.

## Requirements

You'll need Node (v5.11+). Go [install that](https://nodejs.org/en/), if you haven't already.

## Install Noms

On a command-line:

```sh
mkdir noms-tour
cd noms-tour
echo '{"name":"noms-tour","version":"0.0.1"}' > package.json
npm install @attic/noms
```

Then launch Node so that we can have a play:

```sh
node
```

## [Database](https://github.com/attic-labs/noms/blob/master/js/src/database.js)

To get started with Noms, first create a Database:

```js
const noms = require('@attic/noms');

const database = new noms.Database(new noms.MemoryStore());
```

A database is backed by a "ChunkStore", which is where physical chunks of data are kept. Noms/JS comes with several ChunkStore implementations, including MemoryStore, which is useful for testing.



## [Dataset](https://github.com/attic-labs/noms/blob/master/js/src/dataset.js)

Datasets are the main interface you'll use to work with Noms. A dataset is just a named value in the database that you can update:

```js
const dataset = new noms.Dataset(database, "counter");

// prints: null
dataset.head().then(console.log);

dataset.commit(1);

// prints:
// struct Commit<{
//   parents: Set
//   value: Value
// }>(
//   parents: [],
//   value: 1,
// )
dataset.head().then(console.log);

dataset.commit(2);
dataset.commit("three");
dataset.commit({
	count: 4,
});

// prints:
// struct<{
//   count: Number
// }>(
//   value: 4,
// )
dataset.head().then(h => console.log(h.value));
```

Datasets are versioned. When you *commit* a new value, you aren't overwriting the old value, but adding to a historical log of values.

```js
function printHead(head) {
	console.log(head.value);
	head.parents().first()
		.then(headRef => headRef.targetValue())
		.then(printHead);
}

// Prints:
// {count:4}
// "three"
// 2
// 1
dataset.head().then(printHead);
```

Datasets can be very large. Noms will automatically break large lists, maps, sets, and blobs into chunks (using [Prolly Trees](TODO)), so that they can be transferred, searched, and updated efficiently.

If you have structs with really large fields though, it's sometimes useful to break them up manually:

```js
// Write a value to the database manually, outside of a commit
// Note that this doesn't get flushed until the next commit()
const log = 'logloglog'.repeat(100000);
const logRef = database.writeValue({log});
console.log(r);

database.readValue(r).then(v => console.log(v == log));

dataset.commit({
	count: 5,
	logOutput: r,
});

dataset.head().then(h => {
	// prints: {ref: ...}
	console.log(h.value);

	// Prints the first bit of the log
	h.value.targetValue().then(v => console.log.substr(0, 100));
}
```

## Values

Noms supports a [variety of datatypes](TODO-link-to-overview-of-Noms-and-Noms-datatypes). The following table summarizes the JavaScript datatype(s) used to represent each Noms datatype.

Noms Type | JavaScript Type
--------------- | ---------
Boolean | boolean
Number | number
String | string
Blob | [NomsBlob](#NomsBlob)
Set | [NomsSet](#NomsSet)
List | [NomsList](#NomsList)
Map | [NomsMap](#NomsMap)
Ref | [Ref](#Ref)
Struct | Object

In most cases, the Noms JavaScript library will automatically convert between JavaScript types and Noms types. For example:

```js
// Writes a noms value of type:
// Struct<foo: String, num: Number, list: List<String|Number>>
store.writeValue({
  foo: "bar",
  num: 42,
  list: new NomsList("a", "b", 4, 8),
});
```

Sometimes it's nice to expliclty control the type. You can do that with [NomsValue](TODO).