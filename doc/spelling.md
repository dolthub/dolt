# Spelling in Noms

Many commands and APIs in Noms accept database, dataset, or value specifications as arguments. This document describes how to construct these specifications.

## Spelling Databases

database specifications take the form:

```
<protocol>[:<path>]
```

The `path` part of the name is interpreted differently depending on the protocol:

- **http(s)** specs describe a remote database to be accessed over HTTP. In this case, the entire database spec is a normal http(s) URL. For example: `https://dev.noms.io/aa`.
- **ldb** specs describe a local [LevelDB](https://github.com/google/leveldb)-backed database. In this case, the path component should be a relative or absolute path on disk to a directory in which to store the LevelDB data. For example: `ldb:/tmp/noms-data`.
- **mem** specs describe an ephemeral memory-backed database. In this case, the path component is not used and must be empty.

## Spelling Datasets

Dataset specifications take the form:

```
<database>::<dataset>
```

See [spelling databases](#spelling-databases) for how to build the `database` part of the name. The `dataset` part is just any string matching the regex `^[a-zA-Z0-9\-_/]+$`.

Example datasets:

```
ldb:/tmp/test-db::my-dataset
http://localhost:8000::registered-businesses
https://demo.noms.io/aa::music
```

## Spelling Values

Value specifications take the form:

```
<database>::<value-name>::<path>
```

See [spelling databases](#spelling-databases) for how to build the database part of the name.

The `value-name` part can be either a hash or a dataset name. If  `value-name` matches the pattern `^#[0-9a-v]{32}$`, it will be interpreted as a hash. Otherwise it will be interpreted as a dataset name.

The `path` part is relative to the value at `value-name`. See [#1399](https://github.com/attic-labs/noms/issues/1399) for spelling.

### Examples

```sh
# “sf-crime” dataset at http://demo.noms.io/cli-tour
http://demo.noms.io/cli-tour::sf-crume

# value o38hugtf3l1e8rqtj89mijj1dq57eh4m at http://localhost:8000
http://localhost:8000/monkey::#o38hugtf3l1e8rqtj89mijj1dq57eh4m

# “bonk” dataset at ldb:/foo/bar
ldb:/foo/bar::bonk
```
