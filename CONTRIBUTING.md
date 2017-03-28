Contributing to Noms
====================

## Install Go

First setup Go on your machine per https://golang.org/doc/install.

Don't forget to [setup your `$GOPATH` and `$BIN` environment variables](https://golang.org/doc/install) correctly. Everybody forgets that.

You can test your setup like so:

```
# This should print something
echo $GOPATH

# We need at least version 1.7
go version
```

## Setup Noms Environment

Add `NOMS_VERSION_NEXT=1` to your environment. The current trunk codebase is a development version of the format and this environment variable is a safety check to ensure people aren't accidentally using this development format against production servers.

## Get and build Noms

```
go get github.com/attic-labs/noms/cmd/noms
cd $GOPATH/src/github.com/attic-labs/noms/cmd/noms
go build
go test
```

## License

Noms is open source software, licensed under the [Apache License, Version 2.0](LICENSE).

## Contributing code

Due to legal reasons, all contributors must sign a contributor agreement, either for an [individual](http://noms.io/ca_individual.html) or [corporation](http://noms.io/ca_corporation.html), before a pull request can be accepted.

## Languages

* Use Go, JS, or Python.
* Shell script is not allowed.

## Coding style

* Go uses `gofmt`, advisable to hook into your editor
* JS follows the [Airbnb Style Guide](https://github.com/airbnb/javascript)
* Tag PRs with either `toward: #<bug>` or `fixes: #<bug>` to help establish context for why a change is happening
* Commit messages follow [Chris Beam's awesome commit message style guide](http://chris.beams.io/posts/git-commit/)

### Go error reporting

In general, for Public API in Noms, we use the Go-style of returning errors by default.

For non-exposed code, we do provide, and use, some wrappers to do Exception-style error handling. There *must* be an overriding rationale for using this style, however. One reason to use the Exception-style is that the current code doesn't know how to proceed and needs to panic, but you want to signal that a calling function somewhere up the stack might be able to recover from the failure and continue.

For these cases, please use the following family of functions to 'raise' a 'catchable' error (see [go/d/try.go](https://godoc.org/github.com/attic-labs/noms/go/d)):

	* d.PanicIfError()
	* d.PanicIfTrue()
	* d.PanicIfFalse()

You might see some old code that uses functions that seem similar starting with `d.Chk`, however we are going to remove those and don't want to use them for new code. See #3258 for details.

## Submitting PRs

We follow a code review protocol dervied from the one that the [Chromium team](https://www.chromium.org/) uses:

1. Create a GitHub fork of the repo you want to modify (e.g., fork `https://github.com/attic-labs/noms` to `https://github.com/<username>/noms`).
2. Add your own fork as a remote to your github repo: `git remote add <username> https://github.com/<username>/noms`.
3. Push your changes to a branch at your fork: `git push <username> <branch>`
4. Create a PR using the branch you just created. Usually you can do this by just navigating to https://github.com/attic-labs/noms in a browser - GitHub recognizes the new branch and offers to create a PR for you.
5. When you're ready for review, make a comment in the issue asking for a review. Sometimes people won't review until you do this because we're not sure if you think the PR is ready for review.
6. Iterate with your reviewer using the normal Github review flow.
7. Once the reviewer is happy with the changes, they will submit them.

## Running the tests

You can use `go test` command, e.g:

* `go test $(go list ./... | grep -v /vendor/)` should run every test except from vendor packages.

If you have commit rights, Jenkins automatically runs the Go tests on every PR, then every subsequent patch. To ask Jenkins to immediately run, any committer can reply (no quotes) "Jenkins: test this" to your PR.

### Perf tests

By default, neither `go test` nor Jenkins run the perf tests, because they take a while.

To run the tests yourself, use the `-perf` and `-v` flag to `go test`, e.g.:

* `go test -v ./samples/go/csv/... -perf mem`

See https://godoc.org/github.com/attic-labs/noms/go/perf/suite for full documentation and flags.

To ask Jenkins to run the perf tests for you, reply (no quotes) "Jenkins: perf this" to your PR. Your results will be viewable at http://perf.noms.io/?ds=http://demo.noms.io/perf::pr_$your-pull-request-number/csv-import. Again, only a committer can do this.
