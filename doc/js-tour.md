# A Short Tour of Noms for JavaScript

This is a short introduction to using Noms from JavaScript. It should only take a few minutes if you have some familiarity with JavaScript and Node.

During the tour, you can refer to the complete [JavaScript SDK Reference](docs.noms.io/js/) for more information on anything you see.


## Requirements

* [Noms command-line tools](https://github.com/attic-labs/noms#setup)
* [Node v5.11+](https://nodejs.org/en/)

## Start a Local Database

Let's create a local database to play with:

```sh
noms serve /tmp/noms-js-tour
```

## Install Noms NPM Package

Leave the server running, and in a separate terminal:

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

## [Database](https://github.com/attic-labs/noms/blob/master/js/noms/src/database.js)

To get started with Noms, first create a Database:

```js
const noms = require('@attic/noms');

const db = noms.DatabaseSpec.parse('http://localhost:8000').database();
```

See [Spelling in Noms](spelling.md) for more information on database spec strings.



## [Dataset](https://github.com/attic-labs/noms/blob/master/js/noms/src/dataset.js)

Datasets are the main interface you'll use to work with Noms. A dataset is just a named value in the database that you can update:

```js
let ds = db.getDataset('people');

// prints: null
ds.head().then(console.log);

let data = new noms.List([
  noms.newStruct('', {
  	given: 'Rickon',
  	male: true,
  }),
  noms.newStruct('', {
  	given: 'Bran',
  	male: true,
  }),
  noms.newStruct('', {
  	given: 'Arya',
  	male: false,
  }),
  noms.newStruct('', {
  	given: 'Sansa',
  	male: false,
  }),
]);

// prints:
// List<struct  {
//   given: String
//   male: Bool
// }>
console.log(data.type.describe());

db.commit(ds, data).
  then(r => ds = r);
```

Now we can explore the data:

```
// prints: Rickon
ds.head().
  then(commit => commit.value.get(0)).
  then(v => console.log(v.given));
```

You can also see this on the command-line. In a new (third) terminal:

```sh
> noms ds http://localhost:8000
people

> noms show http://localhost:8000::people
struct Commit {
  parents: Set<Ref<Cycle<0>>>
  value: Value
}({
  parents: {},
  value: [
     {
      given: "Rickon",
      male: true,
    },
...
```

Let's add some more data. Back in Node:

```js
data.append(noms.newStruct('', {
  given: 'Jon',
  family: 'Snow',
  male: true,
})).then(d => data = d);

// prints:
// List<struct  {
//   family: String
//   given: String
//   male: Bool
// } | struct  {
//   given: String
//   male: Bool
// }>
console.log(data.type.describe());

db.commit(ds, data).
  then(r => ds = r);
```

Datasets are versioned. When you *commit* a new value, you aren't overwriting the old value, but adding to a historical log of values.

```js
function printCommit(commit) {
  console.log('list', commit.value.hash.toString(),
      'length:', commit.value.length);
  if (commit.parents.isEmpty()) {
    return;
  }
  commit.parents.first().
    then(r => r.targetValue(db)).
    then(printCommit);
}

// Prints:
// list sha1-eba46d10a2d1d10eb9f115c7b8df8c45653b430e length: 5
// list sha1-9cf762c697b10a2868957b6c4ea30de36608ac08 length: 4
ds.head().then(printCommit);
```

## Values

Noms supports a [variety of datatypes](intro.md#types). The following table summarizes the JavaScript datatype(s) used to represent each Noms datatype.

Noms Type | JavaScript Type
--------------- | ---------
Boolean | boolean
Number | number
String | string
Blob | noms.Blob
Set | noms.Set
List | noms.List
Map | noms.Map
Ref | noms.Ref
Struct | noms.Struct

In most cases, the Noms JavaScript library will automatically convert between JavaScript types and Noms types. For example:

```js
// Writes a noms value of type:
// Struct<foo: String, num: Number, list: List<String|Number>>
store.writeValue(newStruct('', {
  foo: "bar",
  num: 42,
  list: new List("a", "b", 4, 8),
}));
```

Sometimes it's nice to explicitly control the type. You can do that by calling `new Struct` directy and passing a `Type` for the first parameter.
