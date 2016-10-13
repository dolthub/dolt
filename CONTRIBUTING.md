Contributing to Noms
====================

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

Test code rarely, if ever, needs to or should use the following Exception-style functions.

* For problems that are conceptually `asserts`, meaning that the program cannot meaningfully continue, use the following calls:
	* d.Chk.Error()
	* d.Chk.NoError()
	* d.Chk.True()
	* d.Chk.False()
* For problems that might occur during correct processing, you can choose the Go-style of returned errors, or use the following to 'raise' a 'catchable' error (see [go/d/try.go](https://godoc.org/github.com/attic-labs/noms/go/d)):
	* d.PanicIfError()
	* d.PanicIfTrue()
	* d.PanicIfFalse()

## Submitting PRs

We follow a code review protocol dervied from the one that the [Chromium team](https://www.chromium.org/) uses:

1. Create a GitHub fork of the repo you want to modify (e.g., fork `https://github.com/attic-labs/noms` to `https://github.com/<username>/noms`).
2. Add your own fork as a remote to your github repo: `git remote add <username> https://github.com/<username>/noms`.
3. Push your changes to a branch at your fork: `git push <username> <branch>`
4. Create a PR using the branch you just created. Usually you can do this by just navigating to https://github.com/attic-labs/noms in a browser - GitHub recognizes the new branch and offers to create a PR for you.
5. When you're ready for review, use GitHub's _assign_ UI to assign someone to review. Typically nobody will review until you assign someone (because we assume you're still getting it ready for review).
6. Reviewer will make comments, then say either 'LGTM' (looks good to me) or 'BTY' (back to you).
7. If the reviewer said LGTM, it means it is ready to merge. If you have commit rights to the respository, go ahead and land the PR. Otherwise the reviewer will land it.
  * *Important*: Only squash merges are allowed, because we like each commit in master to be a single logical piece of work. _GitHub may generate an odd commit message_, so double check before clicking Confirm!
8. If the reviewer said BTY, make the requested changes.
  * *Important*: Please make each round of review comments its own commit. This makes it easy for reviewers to see how your PR has evolved in response to feedback.
  * *Important*: Please do not rebase on top of master until the end of the review for the same reason - you're trying to make it easy for the reviewer to see your changes in isolation.
9. Comment on the review with 'PTAL' (please take another look) when you are ready for the next round of review comments.

For very trivial fixes that are time-sensitive (e.g., to unbreak the build), we do review-after-land. In that case, assign someone to review the PR, and add the phrase 'TBR' (to be reviewed) to the PR description.

## Running the tests

You can use `go test` command, e.g:

* `go test $(go list ./... | grep -v /vendor/)` should run every test except from vendor packages.

For JS code, We have a python script to run all js tests.

* `python tools/run-all-js-tests.py`

If you have commit rights, Jenkins automatically runs the Go and JS tests on every PR, then every subsequent patch. To ask Jenkins to immediately run, any committer can reply (no quotes) "Jenkins: test this" to your PR.

### Perf tests

By default, neither `go test` nor Jenkins run the perf tests, because they take a while.

To run the tests yourself, use the `-perf` and `-v` flag to `go test`, e.g.:

* `go test -v ./samples/go/csv/... -perf mem`

See https://godoc.org/github.com/attic-labs/noms/go/perf/suite for full documentation and flags.

To ask Jenkins to run the perf tests for you, reply (no quotes) "Jenkins: perf this" to your PR. Your results will be viewable at http://perf.noms.io/?ds=http://demo.noms.io/perf::pr_$your-pull-request-number/csv-import. Again, only a committer can do this.
