Contributing to Noms
====================

## License

Noms is open source software, licensed under the [Apache License, Version 2.0](LICENSE).

## Contributing code

Due to legal reasons, all contributors must sign a contributor license agreement, either for an [individual](http://noms.io/ca_individual.html) or [corporation](http://noms.io/ca_corporation.html), before a pull request can be accepted.

## Languages

* Use Go, JS, or Python.
* Shell script is not allowed.

## Coding style

* Go uses `gofmt`, advisable to hook into your editor
* JS follows the [Airbnb Style Guide](https://github.com/airbnb/javascript)
* Tag PRs with either `toward: #<bug>` or `fixes: #<bug>` to help establish context for why a change is happening
* Commit messages follow [Chris Beam's awesome commit message style guide](http://chris.beams.io/posts/git-commit/)

## Submitting PRs

We follow a code review protocol dervied from the one that the [Chromium team](https://www.chromium.org/) uses:

1. Create a fork of the repo you want to modify (e.g., fork `https://github.com/attic-labs/noms` to `https://github.com/<you>/noms`.
2. Push your changes to a branch at your fork
3. Create a PR using that branch against, e.g., `noms/master`.
4. When you're ready for review, use Github's _assign_ UI to assign someone to review. Typically nobody will review until you assign someone (because we assume you're still getting it ready for review).
5. Reviewer will make comments, then say either 'LGTM' (looks good to me) or 'BTY' (back to you).
6. If the reviewer said LGTM, it means it is ready to merge. If you have commit writes to the respository, go ahead and land the PR. Otherwise the reviewer will land it.
  * *Important*: Please squash the changes at this point into one commit. We like each commit in master to be a single logical piece of work that leaves the tree in a valid state.
7. If the reviewer said BTY, make the requested changes.
  * *Important*: Please make each round of review comments its own commit. This makes it easy for reviewers to see how your PR has evolved in response to feedback.
  * *Important*: Please do not rebase on top of master until the end of the review for the same reason - you're trying to make it easy for the reviewer to see your changes in isolation.
8. Comment on the review with 'PTAL' (please take another look) when you are ready for the next round of review comments.

For very trivial fixes that are time-sensitive (e.g., to unbreak the build), we do review-after-land. In that case, assign someone to review the PR, and add the phrase 'TBR' (to be reviewed) to the PR description.

## Running the tests

You can use `go test` command, e.g:

* `go test $(go list ./... | grep -v /vendor/)` should run every test except from vendor packages.

For JS code, We have a python script to run all js tests.

* `python tools/run-all-js-tests.py`

## Recommended Chrome extensions
* Hides generated (Go) files from GitHub pull requests: [GitHub PR gen hider](https://chrome.google.com/webstore/detail/mhemmopgidccpkibohejfhlbkggdcmhf).
