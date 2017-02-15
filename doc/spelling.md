# Spelling in Noms

Many commands and APIs in Noms accept database, dataset, or value specifications as arguments. This document describes how to construct these specifications.

## Spelling Databases

database specifications take the form:

```
<protocol>[:<path>]
```

The `path` part of the name is interpreted differently depending on the protocol:

- **http(s)** specs describe a remote database to be accessed over HTTP. In this case, the entire database spec is a normal http(s) URL. For example: `https://dev.noms.io/aa`.
- **mem** specs describe an ephemeral memory-backed database. In this case, the path component is not used and must be empty.
- **nbs** specs describe a local [Noms Block Store (NBS)](https://github.com/attic-labs/noms/tree/master/go/nbs)-backed database. In this case, the path component should be a relative or absolute path on disk to a directory in which to store the data, e.g. `nbs:/tmp/noms-data`.
  - In Go, `nbs:` can be ommitted (just `/tmp/noms-data` will work).
- **aws** specs describe a remote Noms Block Store backed directly by Amazon Web Services, specifically DynamoDB and S3. The format is a URI containing the names of the DynamoDB table to use, the S3 bucket to use, and the database to serve. For example: `aws://dynamo-table:s3-bucket/database`.

## Spelling Datasets

Dataset specifications take the form:

```
<database>::<dataset>
```

See [spelling databases](#spelling-databases) for how to build the `database` part of the name. The `dataset` part is just any string matching the regex `^[a-zA-Z0-9\-_/]+$`.

Example datasets:

```
/tmp/test-db::my-dataset
nbs:/tmp/test-db::my-dataset
http://localhost:8000::registered-businesses
https://demo.noms.io/aa::music
```

## Spelling Values

Value specifications take the form:

```
<database>::<root><path>
```

See [spelling databases](#spelling-databases) for how to build the database part of the name.

The `root` part can be either a hash or a dataset name. If `root` begins with `#` it will be interpreted as a hash otherwise it is used as a dataset name. See [spelling datasets](#spelling-datasets) for how to build the dataset part of the name.

The `path` part is relative to the `root` provided.

### Specifying Struct Fields
Elements of a Noms struct can be referenced using a period `.`.

For example, if the `root` is a dataset, then one can use `.value` to get the root of the data in the dataset. In this case `.value` selects the `value` field from the `Commit` struct at the top of the dataset. One could instead use `.meta` to select the `meta` struct from the `Commit` struct. The `root` does not need to be a dataset though, so if it is a hash that references a struct, the same notation still works: `#o38hugtf3l1e8rqtj89mijj1dq57eh4m.field`.

### Specifying Collection Values
Elements of a Noms list, map, or set can be retrieved using brackets `[...]`.

For example, if the dataset is a Noms map of number to struct then one could use `.value[42]` to get the Noms struct associated with the key 42. Similarly selecting the first element from a Noms list would be `.value[0]`. If the Noms map was keyed by string, then using `.value["0000024-02-999"]` would reference the Noms struct associated with key "0000024-02-999".

Noms lists also support indexing from the back, using `.value[-1]` to mean the last element of a last, `.value[-2]` for the 2nd last, and so on.

If the key of a Noms map or set is a Noms struct or a more complex value, then indexing into the collection can be done using the hash of that more complex value. For example, if the `root` of our dataset is a Noms set of Noms structs, then if you provide the hash of the struct element then you can index into the map using the brackets as described above. e.g. http://localhost:8000::dataset.value[#o38hugtf3l1e8rqtj89mijj1dq57eh4m].field

Similarly, the key is addressable using `@key` syntax. One use for this is when you have the hash of a complex value, but want need to retrieve the key (rather than or in addition to the value) in a Noms map. The syntax is to append `@key` after the closing bracket of the index specifier. e.g. http://localhost:8000::dataset.value[#o38hugtf3l1e8rqtj89mijj1dq57eh4m]@key would retrieve the key element specified by the hash key `#o38hugtf3l1e8rqtj89mijj1dq57eh4m` from the `dataset.value` collection.

### Specifying Collection Positions
Elements of a Noms list, map, or set can be retrived _by their position_ using the `@at(index)` annotation.

For lists, this is exactly equivalent to `[index]`. For sets and maps, note that Noms has a stable ordering, so `@at(0)` will always return the smallest element, `@at(1)` the 2nd smallest, and so on. `@at(-1)` will return the largest. For maps, adding the `@key` annotation will retrieve the key of the map entry instead of the value.

### Examples

```sh
# “sf-registered-business” dataset at https://demo.noms.io/cli-tour
https://demo.noms.io/cli-tour::sf-registered-business

# value o38hugtf3l1e8rqtj89mijj1dq57eh4m at https://localhost:8000
https://localhost:8000/monkey::#o38hugtf3l1e8rqtj89mijj1dq57eh4m

# “bonk” dataset at /foo/bar
/foo/bar::bonk

# from https://demo.noms.io/cli-tour, select the "sf-registered-business" dataset,
# the root value is a Noms map, select the value of the Noms map identified by string
# key "0000024-02-999", then from that resulting struct select the Ownership_Name field
https://demo.noms.io/cli-tour::sf-registered-business.value["0000024-02-999"].Ownership_Name
```

Be careful with shell escaping. Your shell might require escaping of the double quotes and other characters or use single quotes around the entire command line argument. e.g.:

```sh
> noms show https://demo.noms.io/cli-tour::sf-registered-business.value["0000024-02-999"].Ownership_Name
error: Invalid index: 0000024-02-999

> noms show https://demo.noms.io/cli-tour::sf-registered-business.value[\"0000024-02-999\"].Ownership_Name
"EASTMAN KODAK CO"

> noms show 'https://demo.noms.io/cli-tour::sf-registered-business.value["0000024-02-999"].Ownership_Name'
"EASTMAN KODAK CO"
```
