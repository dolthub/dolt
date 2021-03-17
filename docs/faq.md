# Dolt FAQ

## Why is it called Dolt? Are you calling me dumb?

It's named `dolt` to pay homage to [how Linus Torvalds named
git](https://en.wikipedia.org/wiki/Git#Naming):

> Torvalds sarcastically quipped about the name git (which means
> "unpleasant person" in British English slang): "I'm an egotistical
> bastard, and I name all my projects after myself. First 'Linux',
> now 'git'."

We wanted a word that meant "idiot" and started with D for Data. So,
dolt.

## Why does my connection to the server hang / time out?

With no config file, the server starts up in single user mode. It
won't allow a second connection until the first hangs up. This is to
prevent any unpleasant surprises with multiple writers, since Dolt's
transaction / concurrency model is a work in progress.

To allow multiple simultaneous connections, set the `max_connections`
field in the config.yaml file you pass to the `dolt sql-server`
command, [as described in the
docs](https://docs.dolthub.com/interfaces/cli#dolt-sql-server).

## What does `@@autocommit` do?

This is a SQL variable that you can turn on for your SQL session like so:

`SET @@autocommit = 1`

If it's set to a true value, then Dolt will flush your changes to disk
after every SQL statement, so that you can see your changes when you
run other commands (like `dolt diff`) from the command
line. Specifically, it updates the working set in your database state,
the same way that running `dolt sql -q ...` on the command line does.

Otherwise, you won't see changes outside your session until you issue
a `COMMIT` statement. See the next question.

## What's the difference between `COMMIT`, `COMMIT()`, and `DOLT_COMMIT()`?

`COMMIT` is a standard SQL statement that commits a transaction. In
dolt, it just flushes any pending changes in the current SQL session
to disk, updating the working set. HEAD stays the same, but your
working set changes. This means your edits will persist after this
session ends.

`COMMIT()` creates a new dolt commit, but doesn't references it
anywhere. If you want to reference it, you have to take the value
returned by the function and create a branch with it (by inserting
into `dolt_branches`)

`DOLT_COMMIT()` is the same as if you run `dolt commit` from the
command line. It updates HEAD.

See the [docs on
concurrency](https://docs.dolthub.com/interfaces/sql/concurrency) for
more detail.

## I want each of my connected SQL users to get their own branch to make changes on, then merge them back into `master` when they're done making edits. How do I do that?

We are glad you asked! This is a common use case, and we wrote a
couple blog articles about how to do this effectively.

[dolt sql-server
concurrency](https://www.dolthub.com/blog/2021-03-12-dolt-sql-server-concurrency/)

[Merging and resolving conflicts programmatically with
SQL](https://www.dolthub.com/blog/2021-03-15-programmatic-merge-and-resolve/)

## Does Dolt support transactions?

Not really, in the way that normal databases do. That's a work in
progress. Until then, you can't `ROLLBACK` or create `SAVEPOINTS`, and
things like row-level locking with `SELECT FOR UPDATE` don't work. You
manage concurrency explicitly by creating branches / merging them back
in to `master` when you're done with a unit of work (which can span
any amount of time and any number of SQL sessions). `COMMIT` does
something, but it's not a real transaction (see questions above).

Named locks work, via `GET_LOCK()` and `RELEASE_LOCK()` functions.

Support for traditional database transactions, concurrency, and
row-level locking is on our [roadmap](roadmap.md).

## What SQL features / syntax are supported?

Most of them! Check out [the docs for the full list of supported
features](https://docs.dolthub.com/interfaces/sql/sql-support).

You can check out what we're working on next on our
[roadmap](roadmap.md). Paying customers get their feature requests
bumped to the front of the line.

## Does Dolt support my favorite SQL workbench / tool?

Probably! Have you tried it? If you try it and it doesn't work, [let
us know with an issue](https://github.com/dolthub/dolt/issues) or in
[our Discord](https://discord.com/invite/RFwfYpu) and we'll see what
we can do. A lot of times we can fix small compatibility issues really
quick, like the same week. And even if we can't, we want to know about
it! Our goal is to be a 100% drop-in replacement for MySQL.
