Dolt builder is a tool for more easily installing dolt binaries.

It takes Dolt commit shas or tags as arguments
and builds corresponding binaries to a path specified
by `$DOLT_BIN`

If `$DOLT_BIN` is not set `./doltBin` will be used

(Optional) set `$DEBUG=true` to run in debug mode

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
