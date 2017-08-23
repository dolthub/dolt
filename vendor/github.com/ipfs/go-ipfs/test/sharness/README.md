# ipfs whole tests using the [sharness framework](https://github.com/mlafeldt/sharness/)

## Running all the tests

Just use `make` in this directory to run all the tests.
Run with `TEST_VERBOSE=1` to get helpful verbose output.

```
TEST_VERBOSE=1 make
```

The usual ipfs env flags also apply:

```sh
# the output will make your eyes bleed
IPFS_LOGGING=debug TEST_VERBOSE=1 make
```

## Running just one test

You can run only one test script by launching it like a regular shell
script:

```
$ ./t0010-basic-commands.sh
```

## Debugging one test

You can use the `-v` option to make it verbose and the `-i` option to
make it stop as soon as one test fails.
For example:

```
$ ./t0010-basic-commands.sh -v -i
```

## Sharness

When running sharness tests from main Makefile or when `test_sharness_deps`
target is run dependencies for sharness
will be downloaded from its github repo and installed in a "lib/sharness"
directory.

Please do not change anything in the "lib/sharness" directory.

If you really need some changes in sharness, please fork it from
[its cannonical repo](https://github.com/mlafeldt/sharness/) and
send pull requests there.

## Writing Tests

Please have a look at existing tests and try to follow their example.

When possible and not too inefficient, that means most of the time,
an ipfs command should not be on the left side of a pipe, because if
the ipfs command fails (exit non zero), the pipe will mask this failure.
For example after `false | true`, `echo $?` prints 0 (despite `false`
failing).

It should be possible to put most of the code inside `test_expect_success`,
or sometimes `test_expect_failure`, blocks, and to chain all the commands
inside those blocks with `&&`, or `||` for diagnostic commands.

### Diagnostics

Make your test case output helpful for when running sharness verbosely.
This means cating certain files, or running diagnostic commands.
For example:

```
test_expect_success ".ipfs/ has been created" '
  test -d ".ipfs" &&
  test -f ".ipfs/config" &&
  test -d ".ipfs/datastore" &&
  test -d ".ipfs/blocks" ||
  test_fsh ls -al .ipfs
'
```

The `|| ...` is a diagnostic run when the preceding command fails.
test_fsh is a shell function that echoes the args, runs the cmd,
and then also fails, making sure the test case fails. (wouldnt want
the diagnostic accidentally returning true and making it _seem_ like
the test case succeeded!).


### Testing commands on daemon or mounted

Use the provided functions in `lib/test-lib.sh` to run the daemon or mount:

To init, run daemon, and mount in one go:

```sh
test_launch_ipfs_daemon_and_mount

test_expect_success "'ipfs add --help' succeeds" '
  ipfs add --help >actual
'

# other tests here...

# dont forget to kill the daemon!!
test_kill_ipfs_daemon
```

To init, run daemon, and then mount separately:

```sh
test_init_ipfs

# tests inited but not running here

test_launch_ipfs_daemon

# tests running but not mounted here

test_mount_ipfs

# tests mounted here

# dont forget to kill the daemon!!
test_kill_ipfs_daemon
```
