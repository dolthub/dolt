# A Short Tour of Noms for JavaScript

This is a short tour of how to use Noms from JavaScript. It should only take a few minutes if you have some familiarity with JavaScript and Node.

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

In Noms, data is represented as trees of immutable *values*. For example, the number `42` is a value. The string `'Hello, world'` is a value. The set of all photos from the Hubble space telescope is a value, and each of those photos is also a value.

A Database is a place where you can store Noms values. To do anything with Noms, you'll need a Database:

```js
const noms = require('@attic/noms');

// A database is backed by a "ChunkStore", which is where the physical chunks of data will be kept
// Noms/JS comes with several ChunkStore implementations, including MemoryStore, which is useful
// for testing.
const database = new noms.Database(new noms.MemoryStore());
```

Noms is [content-addressed](https://en.wikipedia.org/wiki/Content-addressable_storage), meaning that every Noms value is identified by a unique hash. When you store a value, you receive a *Ref* which contains the value's hash, and which can be used to retrieve the value later.

```js
const ref = database.writeValue("Hello, world");
ref.targetHash;  // prints: Ref { _refStr: 'sha1-b237e82a5ed084438714743d30dd4900b1327609' }

// prints: Hello, world
database.readValue(ref).then(console.log);
```


## [Dataset](TODO-link-to-Dataset-API)

A Database on its own can only be used to store and retrieve immutable objects.

If you need to keep track of something that changes over time, you need a [Dataset](TODO). A Dataset is a named pointer to a value that can change:

```js
const dataset = new noms.Dataset(database, "salutation");

// prints: null
dataset.head().then(console.log);

// prints: Hello, world
dataset.commit({
	lang: "English",
	msg: "Hello, world",
});
dataset.head().then(h => console.log(h.value.msg));

// prints: Buenos dias
dataset.commit({
	lang: "Spanish",
	msg: "Buenos dias",
});
dataset.head().then(h => console.log(h.value.msg));
```

A Dataset is versioned. When you *commit* a new value, you aren't overwriting the old value, but adding to a historical log of values.

```
// prints: Hello, world
dataset.head()
	.then(h => h.parents().first())
	.then(parent => console.log(parent.value.msg));
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

However, you can control the Noms type explicitly with [NomsValue](#NomsValue).