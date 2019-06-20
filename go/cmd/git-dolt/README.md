# Git-Dolt

Git-Dolt: It's Git for data...in Git!

## Idea

It should be easy to embed a Dolt repository inside a Git repository, and have
changes tracked and synchronized without leaving Git.

To this end, Git-Dolt creates persistent references to Dolt remotes in the form
of pointer files. A Git-Dolt pointer file specifies everything necessary to
reconstruct (clone) the Dolt repository at a given revision:

```
version 0
remote http://dolthub.com/some-org/some-repo
revision eidemcn2rsa5r1kpr5ds7mi0g5h8jt37
```

## Commands

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

### Tests

There are unit tests in Go and CLI tests using [BATS](https://github.com/sstephenson/bats).

### Next steps:

- Better error messaging (when underlying exec'd processes fail, we need to do more than just report their nonzero exit status)
- Support updating the revision in the persistent pointer file
- Use git's smudge filter to automatically fetch dolt repositories when checking out a git repository with git-dolt pointer files
- Even more test coverage
- ...and much more
