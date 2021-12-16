# Dolt FAQ

## Why is it called Dolt? Are you calling me dumb?

It's named `dolt` to pay homage to [how Linus Torvalds named
git](https://en.wikipedia.org/wiki/Git#Naming):

> Torvalds sarcastically quipped about the name git (which means
> "unpleasant person" in British English slang): "I'm an egotistical
> bastard, and I name all my projects after myself. First 'Linux',
> now 'git'."

We wanted a word meaning "idiot", starting with D for Data,
short enough to type on the command line, and
not taken in the standard command line lexicon. So,
`dolt`.

## The MySQL shell gives me an error: `Can't connect to local MySQL server through socket '/tmp/mysql.sock'`

The MySQL shell will try to connect through a socket file on many OSes.
To force it to use TCP instead, give it the loopback address like this:

```bash
% mysql --host 127.0.0.1 ...
```

## What does `@@autocommit` do?

This is a SQL variable that you can turn on for your SQL session like so:

`SET @@autocommit = 1`

It's on by default in the MySQL shell, as well as in most clients. But
some clients (notably the Python MySQL connector) turn it off by
default.

You must commit your changes for them to persist after your session
ends, either by setting `@@autocommit` to on, or by issuing `COMMIT`
statements manually.

## What's the difference between `COMMIT` and `DOLT_COMMIT()`?

`COMMIT` is a standard SQL statement that commits a transaction. In
dolt, it just flushes any pending changes in the current SQL session
to disk, updating the working set. HEAD stays the same, but your
working set changes. This means your edits will persist after this
session ends.

`DOLT_COMMIT()` commits the current SQL transaction, then creates a
new dolt commit on the current branch. It's the same as if you run
`dolt commit` from the command line.

## I want each of my connected SQL users to get their own branch to make changes on, then merge them back into `main` when they're done making edits. How do I do that?

We are glad you asked! This is a common use case, and giving each user
their own branch is something we've spent a lot of time getting
right. For more details on how to use this pattern effectively, see
[using branches](https://docs.dolthub.com/reference/sql/branches).

## Does Dolt support transactions?

Yes, it should exactly work the same as MySQL, but with fewer locks
for competing writes.

It's also possible for different sessions to connect to different
branches on the same server. See [using
branches](https://docs.dolthub.com/reference/sql/branches) for details.

## What SQL features / syntax are supported?

Most of them! Check out [the docs for the full list of supported
features](https://docs.dolthub.com/reference/sql/support).

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
