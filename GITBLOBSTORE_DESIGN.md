# GitBlobstore design (Git object DB-backed `Blobstore`)

## Summary

This document proposes a new `Blobstore` implementation, **`GitBlobstore`**, whose backing store is a **git repository’s object database** (a bare repo or `.git` directory). `GitBlobstore` is intended to eventually enable using **Git remotes as Dolt remotes** by storing Dolt’s NBS artifacts as git objects, **without creating a working tree checkout**.

Key constraints:

- Must implement the existing `go/store/blobstore/blobstore.go` `Blobstore` interface (no interface changes).
- Must be compatible with NBS usage patterns (`go/store/nbs/*`) and the existing blobstore test suite (`go/store/blobstore/blobstore_test.go`).
- Must operate using only the `.git` directory / bare repo and **manipulate git objects directly** (blobs/trees/commits/refs). No checkout.
- Initial ref to use: **`refs/dolt/data`**.

## Background: current `Blobstore` expectations beyond the interface

Although `Blobstore`’s interface is small, the rest of the codebase imposes several **implicit behavioral requirements**.

### Where `Blobstore` is used

`Blobstore` is used by NBS when you construct a store via:

- `nbs.NewBSStore(ctx, ..., bs, ...)` (full conjoin path, requires `Concatenate`)
- `nbs.NewNoConjoinBSStore(ctx, ..., bs, ...)` (OCI-like path, no conjoin/compose)

and for the NBS manifest:

- `go/store/nbs/bs_manifest.go` stores the manifest as a blob named `"manifest"` using `Get` + `CheckAndPut`.

### Required semantics (tests + NBS)

#### 1) Missing keys must return `blobstore.NotFound`

`blobstore_test.go` checks missing-key behavior with `IsNotFoundError(err)` which relies on `err.(NotFound)` type assertion (`go/store/blobstore/errors.go`).

`GitBlobstore.Get()` must return `NotFound{Key: key}` (or with a more descriptive Key string) when the blob does not exist.

#### 2) `Get()` must implement `BlobRange` correctly, including negative offsets

`blobstore_test.go` validates:

- `AllRange` returns the entire blob
- `NewBlobRange(offset, length)` returns exactly the requested bytes
- `NewBlobRange(-n, 0)` returns the last `n` bytes
- `NewBlobRange(-n, m)` returns a slice from the tail

NBS relies on these forms heavily to read table indices/footers:

- `NewBlobRange(-N, 0)` to read the last `N` bytes of table files (index/footer)
- range reads to implement `ReadAtWithStats` for table readers

#### 3) `Get()` must return the **total blob size** as `size`

NBS code uses the `size` returned by `Get()` (even for a range request) as the full object size, e.g. archive readers use it as the total file size.

For range requests, `size` must be the full object size, not the response byte count.

#### 4) `Put()` and `Get()` versions must be consistent

`blobstore_test.go` verifies that:

- `Put()` returns a version string
- `Get()` returns the same version string immediately after the `Put()`

#### 5) `CheckAndPut()` must provide real CAS behavior under concurrency

`blobstore_test.go` has a concurrent read/modify/write loop that:

1. calls `Get()` to read bytes and a version
2. calls `CheckAndPut(expectedVersion=thatVersion, ...)`

Under contention, `CheckAndPut()` must:

- fail with a `blobstore.CheckAndPutError` (recognized via `IsCheckAndPutError`)
- succeed only when the expected version matches the current version

`CheckAndPut(expectedVersion="")` must also support create-if-absent semantics (the tests call it on a missing key).

#### 6) `Concatenate()` is required for the full NBS blobstore persister

NBS’s `blobstorePersister` (`go/store/nbs/bs_persister.go`) persists tables by writing:

- `<name>.records`
- `<name>.tail`

then calling:

- `bs.Concatenate(name, []string{name+".records", name+".tail"})`

Conjoin operations also call `Concatenate` to compose record-only sub-objects.

If a blobstore cannot implement `Concatenate`, it must be used with `NewNoConjoinBSStore`, and conjoin is disabled (as is done for OCI).

## Concept: representing a Blobstore inside git

The `GitBlobstore` will map the blobstore keyspace onto the **tree of a commit** referenced by `refs/dolt/data`.

- The ref `refs/dolt/data` points to a commit.
- The commit’s tree is the “directory” holding all blobstore keys.
- Each blobstore `key` is a **path** in that tree; its contents are a git **blob** object.

This gives us:

- immutable content-addressed blob objects for data
- an append-only history of updates (commits)
- a single ref head that can serve as a “version” for CAS updates

No working tree is required; we only read/write objects and update refs.

## Key design decisions

### Ref name

Use:

- `refs/dolt/data`

as the authoritative reference for the blobstore.

### Version string

Define the blobstore “version” as:

- the **commit hash** (hex object id) currently pointed to by `refs/dolt/data`

Rationale:

- it is globally consistent across keys (a snapshot version)
- it naturally supports CAS: update ref from old commit → new commit
- it mirrors object-store generation/etag semantics as a stable identifier

Implications:

- `Get()` returns the commit hash of `refs/dolt/data` at the time of lookup.
- `Put()` returns the new commit hash it wrote.
- `CheckAndPut()` compares expected commit hash to current ref commit hash.

### Atomic CAS

`CheckAndPut()` must be atomic: it must only update if the ref still points to the expected commit id.

Implementation requirement:

- update `refs/dolt/data` with a compare-and-swap on the old object id

This can be achieved by:

- using git plumbing (`update-ref <ref> <new> <old>`) which is designed to be atomic, or
- implementing direct ref updates with proper locking and packed-refs handling (more complex).

### Key/path validation

Blobstore keys are treated as paths in the git tree. We must prevent traversal or invalid path components.

Rules:

- reject keys that are absolute (`/…`) or contain `..` path segments
- reject NUL bytes
- normalize path separators to `/` (git tree paths are `/`)
- optionally, restrict keys to a conservative character set if desired (not required initially)

### `.bs` extension

Unlike `LocalBlobstore` which appends `.bs` on disk, `GitBlobstore` should store keys **exactly as provided** (e.g. `manifest`, `<hash>`, `<hash>.records`), matching other remote blobstores (GCS/OCI/OSS) which also store by key directly.

## Interface mapping

Below is the planned behavior for each `Blobstore` method.

### `Path() string`

Return a stable identifier for logging/debugging, e.g.:

- `<gitDir>@refs/dolt/data`

### `Exists(ctx, key) (bool, error)`

- Resolve `refs/dolt/data` to a commit (if the ref does not exist, return `(false, nil)`).
- Read the commit tree and look up the path for `key`.
- Return true if present and is a blob, else false.

### `Get(ctx, key, br) (rc, size, version, err)`

Steps:

1. Resolve `refs/dolt/data` → commit id (`version`).
2. Locate the blob object id at path `key` in that commit’s tree.
3. Determine `size` (full blob size), even if the caller requests a range.
4. Return an `io.ReadCloser` over the requested range:
   - If `br` is `AllRange`, stream full blob.
   - If `br.offset < 0`, convert to a positive range using total `size` (same behavior as `BlobRange.positiveRange`).
   - If `br.length == 0`, stream from offset to end.
   - If `br.length > 0`, stream exactly `length` bytes starting at `offset`.

Errors:

- if key is missing: return `NotFound{Key: key}`

### `Put(ctx, key, totalSize, reader) (version, error)`

Behavior:

- Write a new git blob object with the content from `reader`.
- Create a new tree that is the previous tree with path `key` updated to point to that blob.
- Create a new commit and update `refs/dolt/data` to point to it (last-writer-wins).

Notes:

- `Put()` does not provide CAS; `CheckAndPut()` is used where CAS is required (manifest updates and concurrent RMW patterns).
- `Put()` should return the new commit hash as `version`.

### `CheckAndPut(ctx, expectedVersion, key, totalSize, reader) (version, error)`

CAS semantics:

- If `expectedVersion != ""`:
  - require that `refs/dolt/data` currently points to `expectedVersion`
  - if not, return `CheckAndPutError{Key, ExpectedVersion, ActualVersion}`
- If `expectedVersion == ""`:
  - implement create-if-absent behavior:
    - if `key` exists in the current ref snapshot, return `CheckAndPutError`
    - otherwise, proceed

If check passes:

- perform the same write as `Put()` (write blob, update tree, commit)
- atomically update the ref from old commit id to new commit id (CAS)

Return:

- new commit hash

### `Concatenate(ctx, key, sources) (version, error)`

Correctness-first approach:

- Stream all sources in order and write a new git blob object whose content is their concatenation.
- Update path `key` to point to that new blob.
- Commit and update `refs/dolt/data` (either last-writer-wins, or CAS on current head; CAS is preferred to avoid lost updates if multiple writers are composing concurrently).

Notes:

- NBS calls `Concatenate` for `Persist()` and for conjoin; correctness matters more than git-level “compose” optimization.
- This is expected to be efficient enough initially because objects are already local to the `.git` store; it avoids network.

## NBS compatibility modes

`GitBlobstore` is expected to implement `Concatenate`, so it can be used with:

- `nbs.NewBSStore(...)` (full conjoin enabled)

If at some point we introduce a mode that cannot implement efficient `Concatenate`, then it must be paired with:

- `nbs.NewNoConjoinBSStore(...)` (conjoin disabled)

## Performance notes and future work

### Range reads

Git does not provide a native “range read” API for blobs. Implementations will likely:

- read the full decompressed blob stream and slice/limit, or
- optimize tail reads (`offset < 0`) with a ring buffer to avoid buffering whole blobs in memory

NBS does many tail reads for index/footer, so tail-range optimization is a likely follow-up.

### Tree updates at scale

Updating a file path in a git tree requires rebuilding trees along the path. If the blobstore keyspace gets large and flat, tree operations may become hot.

Potential future optimizations:

- key sharding (e.g. store keys under `aa/bb/<key>` derived from hash prefixes)
- batching multiple key updates into one commit when possible
- using lower-level object formats or packfile streaming for high write throughput

### Repository maintenance

Because each write creates new objects and commits:

- git GC / repack policy will matter
- prune of unreachable objects may be required in long-running deployments

These are explicitly out of scope for the first iteration.

## Dependency / implementation strategy

This design is compatible with multiple backends:

- **Git CLI plumbing** (e.g., `git --git-dir <dir> cat-file`, `hash-object`, `mktree`, `commit-tree`, `update-ref`)
  - Pros: low code, uses battle-tested git object manipulation, no extra Go dependencies
  - Cons: runtime dependency on `git`, process overhead

- **Pure-Go implementation**
  - Pros: no external process dependency
  - Cons: larger implementation effort; likely requires introducing a library dependency (e.g., `go-git`) or writing object plumbing in-house

Given the current module does not include a Go git implementation dependency, the initial implementation can reasonably use **git plumbing commands** while still meeting the “no checkout” requirement.

## Git internals and plumbing commands (implementation preview)

This section previews the **git internals** and a concrete set of **plumbing commands** we expect `GitBlobstore` to use. The approach explicitly avoids a working tree checkout.

### Git internals we rely on

- **Objects** live in the object database (ODB) as either:
  - **loose** objects under `.git/objects/aa/bb…`, or
  - **packed** objects under `.git/objects/pack/*.pack`
  `GitBlobstore` must be agnostic to storage format; it should interact through git’s plumbing which abstracts over loose/packed storage.
- **Object types**:
  - **blob**: raw file contents (this is the value for a blobstore key)
  - **tree**: directory mapping `name -> (mode, type, oid)` (this is the blobstore keyspace index)
  - **commit**: points to a root tree + parent(s) + metadata (this is the snapshot “version”)
- **Ref**:
  - `refs/dolt/data` points at the current commit snapshot
  - updating this ref publishes a new blobstore state

### Minimal plumbing command set

Below are the commands we expect to use (exact invocation may vary).

#### Resolve ref / versions

- Check ref existence:
  - `git --git-dir <gitDir> show-ref --verify --quiet refs/dolt/data`
- Resolve current commit (version):
  - `git --git-dir <gitDir> rev-parse --verify refs/dolt/data`

#### Key existence and path lookup

- Check if a path exists in a commit tree:
  - `git --git-dir <gitDir> cat-file -e <commit>:<path>`
- Resolve the blob oid for a path:
  - `git --git-dir <gitDir> rev-parse <commit>:<path>`

#### Blob size and blob content streaming

- Object size (needed so `Get()` can return total blob size even for range requests):
  - `git --git-dir <gitDir> cat-file -s <blobOid>`
- Stream blob contents:
  - `git --git-dir <gitDir> cat-file blob <blobOid>`

Note: git does not have a native “range read” for blobs; for `BlobRange` we will stream and slice/limit in Go. Tail-range reads (`offset < 0`) can be optimized later with a ring buffer.

#### Write a blob object

- Write blob from stdin and get its oid:
  - `git --git-dir <gitDir> hash-object -w --stdin`

This is used by `Put`, `CheckAndPut`, and `Concatenate` (after concatenating sources).

### Tree updates without a checkout (Approach A: temporary index)

We prefer **Approach A**: use a *temporary git index file* to stage a tree update without a working directory.

Core idea:

- set `GIT_DIR=<gitDir>`
- set `GIT_INDEX_FILE=<tempIndexPath>`
- use `read-tree` + `update-index --cacheinfo` + `write-tree`

Commands:

- Initialize the temporary index from the current tree:
  - if `refs/dolt/data` exists:
    - `git read-tree <commit>^{tree}`
  - if the ref is missing (bootstrap):
    - `git read-tree --empty`
- Add or replace a path with a specific blob oid:
  - `git update-index --add --cacheinfo 100644 <blobOid> <path>`
  - (mode may be `100644` for regular file; no executable bits)
- Write the resulting tree:
  - `git write-tree` → outputs `<treeOid>`

Advantages:

- avoids manual tree reconstruction for nested paths
- avoids checkout / working tree entirely
- supports deep key paths naturally

### Commit creation and atomic ref update (CAS)

- Create a commit object from the new tree:
  - `git commit-tree <treeOid> -p <parentCommitOid> -m <message>`
  - (author/committer identity can be supplied via env; we can use a fixed identity initially)
- Atomic compare-and-swap ref update:
  - `git update-ref -m "<msg>" refs/dolt/data <newCommitOid> <oldCommitOid>`

This `update-ref` form is the primary CAS primitive; it will fail if the ref no longer points to `<oldCommitOid>`.

Bootstrap note:

- for “create ref only if absent”, git accepts an “old” value of all-zeros (`000…000`) which enforces that the ref does not exist.

### Command-to-method mapping (quick reference)

- `Exists(key)`:
  - `rev-parse refs/dolt/data` (if missing → false)
  - `cat-file -e <commit>:<key>`
- `Get(key, br)`:
  - `rev-parse refs/dolt/data` → version
  - `rev-parse <commit>:<key>` → blob oid
  - `cat-file -s <blobOid>` → total size
  - `cat-file blob <blobOid>` → stream; slice/limit in Go per `BlobRange`
- `Put(key, reader)`:
  - `hash-object -w --stdin` → blob oid
  - temp index: `read-tree` → `update-index --cacheinfo` → `write-tree` → tree oid
  - `commit-tree` → new commit oid
  - `update-ref refs/dolt/data <new>` (non-CAS)
- `CheckAndPut(expectedVersion, key, reader)`:
  - same as `Put`, but final step is:
  - `update-ref refs/dolt/data <new> <expectedVersion>` (CAS)
- `Concatenate(key, sources)`:
  - for each source: resolve blob oid and stream `cat-file blob`
  - pipe concatenation into `hash-object -w --stdin`
  - then same tree/commit/ref update flow as `Put` (CAS preferred)

## Testing plan (expected to pass existing suite)

`GitBlobstore` should be able to join the existing blobstore tests in `go/store/blobstore/blobstore_test.go`:

- Put/Get version equality
- NotFound behavior
- CAS correctness (concurrent CheckAndPut test)
- Range read correctness (including negative offsets)
- Concatenate correctness

Additional recommended tests:

- behavior when `refs/dolt/data` does not exist yet (bootstrap)
- packed-refs handling (if the ref is packed)
- concurrent writers updating different keys (should not corrupt ref)

