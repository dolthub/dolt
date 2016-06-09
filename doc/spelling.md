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

## Spelling Values

Value specifications take the form:

```
<database>::<value-name>
```

See [spelling databases](#spelling-databases) for how to build the database part of the name.

The `value-name` part can be either a ref or a dataset name. If  `value-name` matches the pattern `^sha1-[0-9a-fA-F]{40}$`, it will be interpreted as a ref. Otherwise it will be interpreted as a dataset name.

### Examples

```sh
# “foo” dataset at http://api.noms.io/-/aa
http://api.noms.io/-/aa::foo

# value sha1-e7219f3603e1a20a9fabaa43b3f3a7c443ae1041 at http://localhost:8000
http://localhost:8000/monkey::sha1-e7219f3603e1a20a9fabaa43b3f3a7c443ae1041

# “bonk” dataset at ldb:/foo/bar
ldb:/foo/bar::bonk
```
