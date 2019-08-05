# Dolt &mdash; It's Git for Data

Dolt lets users collaborate on databases in the same way they collaborate on
source code. Dolt is a relational database combined with the version control
concepts of Git.

# We also built DoltHub

DoltHub is GitHub for Dolt, a place on the internet to share Dolt repositories. https://wwww.dolthub.com

# Getting Started

## Installation

Dolt receives regular binary releases for Windows, macOS and Linux which are
available at https://github.com/liquidata-inc/dolt/releases. The `./bin`
directory of your platforms tarball should be placed on your path so you can
run `dolt`.

Installing from source requires Go 1.13+. Using go install:

```sh
$ go install github.com/liquidata-inc/dolt/go/cmd/dolt
$ go install github.com/liquidata-inc/dolt/go/cmd/git-dolt
$ go install github.com/liquidata-inc/dolt/go/cmd/git-dolt-smudge
```

Or from a checkout of the repository:

```sh
$ cd go
$ go install ./cmd/dolt
$ go install ./cmd/git-dolt
$ go install ./cmd/git-dolt-smudge
```

## Global Config

Setup your name and email by running:

```sh
$ dolt config --global --add user.email YOU@DOMAIN.COM
$ dolt config --global --add user.name "YOUR NAME"
```
 
## First Repository

Make a directory with nothing in it, initialize a dolt repository, and create a
schema:

```sh
$ mkdir first_dolt_repo
$ cd first_dolt_repo
$ dolt init
Successfully initialized dolt data repository.
$ dolt sql -q "create table state_populations ( state varchar, population int, primary key (state) )"
$ dolt sql -q "show tables"
+-------------------+
| tables            |
+-------------------+
| state_populations |
+-------------------+
$ dolt sql -q 'insert into state_populations (state, population) values
("Delaware", 59096),
("Maryland", 319728),
("Tennessee", 35691),
("Virginia", 691937),
("Connecticut", 237946),
("Massachusetts", 378787),
("South Carolina", 249073),
("New Hampshire", 141885),
("Vermont", 85425),
("Georgia", 82548),
("Pennsylvania", 434373),
("Kentucky", 73677),
("New York", 340120),
("New Jersey", 184139),
("North Carolina", 393751),
("Maine", 96540),
("Rhode Island", 68825)
'
$ dolt add .
$ dolt commit -m 'adding state populations from 1790.'
```

Now you can interact with the repository, as in `dolt log` or `dolt sql -q
"select * from state_populations"`.

# Credits and License

The implementation of Dolt makes use of code and ideas from
[noms](https://github.com/attic-labs/noms).

Dolt is licensed under the Apache License, Version 2.0. See
[LICENSE](https://github.com/liquidata-inc/dolt/blob/master/LICENSE) for
details.
