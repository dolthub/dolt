BATS Executor Exemplar is used to run all BATS tests in parallel to get faster results than running serially.

## Installation

To install, go to the go/utils/batsee directory and run:

    `go install .`

Results will be in `$HOME/go/bin/`, so add that to your path.

## Usage

Before you do anything related to batsee, you should make sure you can [run bats](../../../integration-tests/bats/README.md).

```
$ batsee --help
NAME
        batsee - Run the Bats Tests concurrently

SYNOPSIS
        batsee [-t <threads>] [--skip-slow] [--max-time <time>] [--retries <retries>] [--only test1,test2,...]

DESCRIPTION
        From within the integration-test/bats directory, run the bats tests concurrently.
        Output for each test is written to a file in the batsee_output directory.
        Example:  batsee -t 42 --max-time 1h15m -r 2 --only types.bats,foreign-keys.bats

OPTIONS
        -t <threads>, --threads=<threads>
          Number of tests to execute in parallel. Defaults to 12

        -s, --skip-slow
          Skip slow tests. This is a static list of test we know are slow, may grow stale.

        --max-time=<duration>
          Maximum time to run tests. Defaults to 30m

        --only
          Only run the specified test, or tests (comma separated)

        -r <retries>, --retries=<retries>
          Number of times to retry a failed test. Defaults to 1
```
