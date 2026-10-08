Dolt builder is a tool for more easily installing dolt binaries.

It takes Dolt commit shas or tags as arguments
and builds corresponding binaries to a path specified
by `$DOLT_BIN`

If `$DOLT_BIN` is not set `./doltBin` will be used

(Optional) set `$DEBUG=true` to run in debug mode

Supply additional Go build tags with `-tags` (before the revision arguments):

```bash
dolt-builder -tags tag_one,tag_two v1.33.0
dolt-builder -tags tag_one -profile /absolute/path/cpu.pprof v1.33.0
```

Tags apply to every requested revision. A PGO profile can only be used when
building a single revision. Output paths remain `$DOLT_BIN/<revision>/dolt`, so
building the same revision with different tags replaces the previous binary.

Go consumers can use `RunWithBuildTags`:

```go
err := builder.RunWithBuildTags(ctx, []string{version}, profilePath, []string{"tag_one", "tag_two"})
```

The existing `Run(ctx, revisions, profilePath)` API builds without additional tags.

Example usage:

```bash
$ dolt-builder dccba46 4bad226
$ dolt version 0.1
$ dolt version 0.2
```

```bash
$ dolt-builder v0.19.0 v0.22.6
$ dolt version 0.19.0
$ dolt version v0.22.6
```
