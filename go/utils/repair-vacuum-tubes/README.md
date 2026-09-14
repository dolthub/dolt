# vacuum-tubes foreign-key repair

This one-off tool repairs the empty table names in foreign-key object
`4jnflfmh77fvhemaduakn7ndv8jgi8oq`, diagnosed in `captainstabs/vacuum-tubes`.
It restores `vacuum_tubes(type) REFERENCES tubes_type(id)` for constraint
`i9u233qd`, preserving its other settings. It refuses other metadata hashes,
unexpected column tags, dirty working/staged roots, and active merges/rebases.

Build from this repository's `go` directory:

```sh
go build -o /tmp/repair-vacuum-tubes ./utils/repair-vacuum-tubes
go test ./utils/repair-vacuum-tubes
```

Stop any SQL server and other processes writing this database first. Check it:

```sh
/tmp/repair-vacuum-tubes -db "$HOME/dbs/vacuum-tubes"
```

Apply the repair:

```sh
/tmp/repair-vacuum-tubes -db "$HOME/dbs/vacuum-tubes" -apply
```

The default branch is `master`; `-branch` selects another local branch. The tool
copies the database to a new sibling directory named
`vacuum-tubes.before-fk-repair-*` before writing repair data. A backup failure
aborts the repair. Keep the printed backup path until verification is complete.

It adds one new commit and updates working/staged roots atomically using Dolt's
working-set optimistic lock. It verifies that all table object hashes remain
unchanged. It does not push to a remote or rewrite history. Historical queries,
diffs, or checkouts involving damaged older roots may still fail. Running the
tool again refuses the already repaired metadata without adding another commit.

Check the result from the database directory:

```sh
dolt status
dolt schema show
dolt fsck
```

Integration-tested on a separate copy: both table hashes unchanged, 122 tube
rows and 12 type rows, clean working state, restored schema output, invalid
foreign-key insert rejected, repaired database and backup both pass `dolt fsck`.
The original database was not modified during testing.
