# A Short Tour of Noms for JavaScript

This is a short tour of using Noms from JavaScript. It should only take a few minutes if you have some familiarty with JavaScript.

## Requirements

You'll need Node (v5.3+). Go [install that](https://nodejs.org/en/), if you haven't already.

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

## [Database](TODO-link-to-Database-API)

In Noms, data is represented as trees of immutable *values*. For example, the number `42` is a value. The string `'hello, world'` is a value. The set of all photos from the Hubble space telescope is a value, and each of those photos is also a value.

A Database is a place where you can store Noms values. To do anything with Noms, you will first need to create an instance of a Database:

```js
const noms = require('@attic/noms');

// A database is backed by a "ChunkStore", which is where the physical chunks of data will be kept
// Noms/JS comes with several ChunkStore implementations, including MemoryStore, which is useful
// for testing.
const db = new noms.Database(new noms.MemoryStore());
```

Noms is a [content-addressed](https://en.wikipedia.org/wiki/Content-addressable_storage) database. Every noms value has a hash. When you store a value in noms, you get a *Ref* (short for *reference*) to the data back. The Ref encapsulates the value's hash and some other details.

```js
const ref1 = db.writeValue("Hello, world");
ref1.targetRef;  // prints: Ref { _refStr: 'sha1-b237e82a5ed084438714743d30dd4900b1327609' }

// prints: Hello, world
db.readValue(ref1.targetRef).then(console.log);
```


## [Dataset](TODO-link-to-DataSet-API)

A Database on its own can only be used to store and retrieve immutable objects by their hash. This has limited utility.

If you need to keep track of something that changes over time, you need a [Dataset](TODO). A Dataset is a named pointer to a value that can change:

```
let dataSet = new noms.Dataset(db, "salutation");

// prints: null
dataSet.head().then(console.log);

// prints: sha1-b237e82a5ed084438714743d30dd4900b1327609 (same ref as we committed initially)
dataSet = dataSet.commit(ref1);
dataSet
	.then(ds => ds.head())
	.then(commit => console.log(commit.value));

// prints: Hello, world
dataSet
	.then(ds => ds.head())
	.then(commit => commit.value.targetValue(db))
	.then(console.log);

// prints: Buenos dias
const ref2 = db.writeValue("Buenos dias");
dataSet = dataSet.then(ds => ds.commit(ref2));
dataSet
	.then(ds => ds.head())
	.then(commit => commit.value.targetValue(db))
	.then(console.log);
```

A DataSet is versioned. When you *commit* a new value, you aren't overwriting the old value, but adding to a historical log of values.

```
// prints: Hello, world
dataSet
	.then(ds => ds.head())
	.then(h => h.parents.first())
	.then(rv => rv.targetValue(db))
	.then(commit => commit.value.targetValue(db))
	.then(console.log);
```

## Values

The Noms format supports a [variety of datatypes](TODO-link-to-overview-of-Noms-and-Noms-datatypes). The following table summarizes the JavaScript datatype(s) used to represent each Noms datatype.

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
Struct | object or [NomsStruct](#NomsStruct)

For example:

```
// Writes a noms value of type:
// Struct<foo: String, num: Number, list: List<String|Number>>
db.writeValue({
  foo: "bar",
  num: 42,
  list: new NomsList("a", "b", 4, 8),
});
```