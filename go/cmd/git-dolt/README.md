# Git-Dolt

Git-Dolt: It's Git for data...in Git!

## Idea

It should be easy to embed a Dolt repository inside a Git repository, and have
changes tracked and synchronized without leaving Git.

To this end, Git-Dolt creates persistent references to Dolt remotes in the form
of pointer files. This approach is very similar to how [git-lfs](https://git-lfs.github.com/) embeds
large binary files inside of Git repositories.

A Git-Dolt pointer file specifies everything necessary to
reconstruct (clone) the Dolt repository at a given revision:

```
version 0
remote http://dolthub.com/some-org/some-repo
revision eidemcn2rsa5r1kpr5ds7mi0g5h8jt37
```

In the future, we imagine `git-dolt` doing things like automatically proposing
modifications to the pointer file when the cloned Dolt repo is modified. For
now, if you want the pointer file to point to a different commit, you must update
it manually; see the [`update`](#update) command below.

## Example

### Setup

Make sure that `git-dolt` and `git-dolt-smudge` are in your PATH. Then do the steps below in a new directory somewhere:

```
$ git init
Initialized empty Git repository in /Users/mjesuele/dolt/git-dolt-smudge-test/.git/

$ git dolt install
Installed git-dolt smudge filter.
When git-dolt pointer files are checked out in this git repository, the corresponding Dolt repositories will be automatically cloned.

$ git dolt link Liquidata/lunch-places

Dolt repository successfully linked!

* Dolt repository cloned to lunch-places at revision qi331vjgoavqpi5am334cji1gmhlkdv5
* Pointer file created at lunch-places.git-dolt
* lunch-places added to .gitignore

You should git commit these results.

$ git add .

$ git commit -m set up git-dolt integration
[master (root-commit) 82c762e] set up git-dolt integration
 3 files changed, 5 insertions(+)
 create mode 100644 .gitattributes
 create mode 100644 .gitignore
 create mode 100644 lunch-places.git-dolt
```

### Testing the smudge filter

```
$ rm -rf lunch-places lunch-places.git-dolt

$ git checkout -- lunch-places.git-dolt
Found git-dolt pointer file. Cloning remote Liquidata/lunch-places to revision qi331vjgoavqpi5am334cji1gmhlkdv5 in directory lunch-places...done.

$ cd lunch-places

$ dolt log -n 3
commit qi331vjgoavqpi5am334cji1gmhlkdv5
Author: bheni <brian@liquidata.co>
Date:   Thu Jun 06 17:22:24 -0700 2019

	update tocaya rating

commit eidemcn2rsa5r1kpr5ds7mi0g5h8jt37
Merge: 137qgvrsve1u458briekqar5f7iiqq2j ngfah8jf5r9apr7bds87nua9uavnlc1d
Author: Aaron Son <aaron@liquidata.co>
Date:   Thu Apr 18 13:04:18 -0700 2019

	Merge...

commit 137qgvrsve1u458briekqar5f7iiqq2j
Author: bheni <brian@liquidata.co>
Date:   Thu Apr 04 15:43:00 -0700 2019

	change rating
```

## Commands

### install

```
git dolt install
```

Initializes a git-dolt integration in the current git repository by:

1. Adding a line in `.gitattributes` declaring the git-dolt smudge filter on `.git-dolt` files.
2. Adding a line in `.git/config` telling git which executable(s) to use for the git-dolt filter.

Once this is done, any time a `.git-dolt` file is checked out in the git repository, the Dolt
repository that it points to will automatically be cloned to the specified revision.

Note that this functionality requires that the `git-dolt-smudge` executable be present in your `PATH`.

See [the chapter in Pro Git on Git Attributes](https://git-scm.com/book/en/v2/Customizing-Git-Git-Attributes)
for more information about filters.

_Example:_

```
$ git dolt install
Installed git-dolt smudge filter. When git-dolt pointer files are checked out in this git repository, the corresponding Dolt repositories will be automatically cloned.

You should git commit the changes to .gitattributes.
```

### link

```
git dolt link <remote-url>
```

Links a dolt remote to the current git repository by:

1. Cloning the dolt repository in the current directory
2. Adding the dolt repository directory to `.gitignore`
3. Outputting a persistent `.git-dolt` pointer file intended to be committed with git

_Example:_

```
$ git dolt link http://localhost:50051/test-org/test-repo eidemcn2rsa5r1kpr5ds7mi0g5h8jt37

Dolt repository successfully linked!

* Dolt repository cloned to test-repo at revision jdnj4siav9lk8obprgfmsvlae4rvc5jc
* Pointer file created at test-repo.git-dolt
* test-repo added to .gitignore

You should git commit these results.
```

### fetch

```
git dolt fetch <pointer-file>
```

Fetches a dolt repository from the remote and at the revision specified by the given git-dolt pointer file (you may omit the `.git-dolt` suffix when specifying this file).

_Example:_

```
$ git dolt fetch test-repo
Dolt repository cloned from remote http://localhost:50051/test-org/test-repo to directory test-repo at revision 2diug5q1okrdi5rq4f8ct3vea00gblbj
```

#### Note

Currently, dolt lacks detached head support, meaning that you can't check out individual commits. It is also currently not possible to fetch only a specific revision from a remote repository.

Accordingly, the current behavior of `git dolt fetch` is to clone the remote repository and create/check out a new branch called `git-dolt-pinned` which points at the specified commit.

### update

```
git dolt update <pointer-file> <revision>
```

Updates the given git-dolt pointer file to point to the specified revision.

_Example:_

```
$ git dolt update im-interested ppbq8n1difju3u02jf8iqmctd1ovbj76
Updated pointer file im-interested.git-dolt to revision ppbq8n1difju3u02jf8iqmctd1ovbj76. You should git commit this change.
```

## Tests

There are unit tests in Go and CLI tests using [BATS](https://github.com/sstephenson/bats).
