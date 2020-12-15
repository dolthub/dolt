# Dolt

<img align="left" height="300" src="./dolt_hi.svg"/>

### Dolt is Git for Data!

Dolt is a SQL database that you can fork, clone, branch, merge, push
and pull just like a git repository. Connect to Dolt just like any
MySQL database to run queries or update the data using SQL
commands. Use the command line interface to import CSV files, commit
your changes, push them to a remote, or merge your teammate's changes.

All the commands you know for Git work exactly the same for Dolt. Git
versions files, Dolt versions tables. It's like Git and MySQL had a
baby!

We also built [DoltHub](https://www.dolthub.com), a place to share
Dolt databases. We host public data for free!

[Join us on Discord](https://discord.com/invite/RFwfYpu) to say hi and
ask questions!

# Dolt CLI

```
$ dolt
Valid commands for dolt are
                init - Create an empty Dolt data repository.
              status - Show the working tree status.
                 add - Add table changes to the list of staged table changes.
               reset - Remove table changes from the list of staged table changes.
              commit - Record changes to the repository.
                 sql - Run a SQL query against tables in repository.
          sql-server - Start a MySQL-compatible server.
                 log - Show commit logs.
                diff - Diff a table.
               blame - Show what revision and author last modified each row of a table.
               merge - Merge a branch.
              branch - Create, list, edit, delete branches.
                 tag - Create, list, delete tags.
            checkout - Checkout a branch or overwrite a table from HEAD.
              remote - Manage set of tracked repositories.
                push - Push to a dolt remote.
                pull - Fetch from a dolt remote data repository and merge.
               fetch - Update the database from a remote data repository.
               clone - Clone from a remote data repository.
               creds - Commands for managing credentials.
               login - Login to a dolt remote host.
             version - Displays the current Dolt cli version.
              config - Dolt configuration.
                  ls - List tables in the working set.
              schema - Commands for showing and importing table schemas.
               table - Commands for copying, renaming, deleting, and exporting tables.
           conflicts - Commands for viewing and resolving merge conflicts.
             migrate - Executes a repository migration to update to the latest format.
         read-tables - Fetch table(s) at a specific commit into a new dolt repo
                  gc - Cleans up unreferenced data from the repository.
```

# Installation

## From Latest Release

To install on Linux or Mac based systems run this command in your
terminal:

```
sudo bash -c 'curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash'
```

This will download the latest `dolt` release and put it in
`/usr/local/bin/`, which is probably on your `$PATH`.

### Homebrew

Dolt is on Homebrew, updated every release.

```
brew install dolt
```

### Windows

Download the latest Microsoft Installer (`.msi` file) in
[releases](https://github.com/dolthub/dolt/releases) and run
it. Package manager releases coming soon!

For information on running on Windows, see [here](windows.md).

## From Source

Make sure you have Go installed, and that `go` is in your path.

Clone this repository and cd into the `go` directory. Then run:

```
go install ./cmd/dolt
```

# Configuration

Verify that your installation has succeeded by running `dolt` in your
terminal.

```
$ dolt
Valid commands for dolt are
[...]
```

Configure `dolt` with your user name and email, which you'll need to
create commits. The commands work exactly the same as git.

```
$ dolt config --global --add user.email YOU@DOMAIN.COM
$ dolt config --global --add user.name "YOUR NAME"
```

# Getting started

Let's create our first repo, storing state population data.

```
$ mkdir state-pops
$ cd state-pops
```

Run `dolt init` to set up a new `dolt` repo, just like you do with
git. Then run some SQL queries to insert data.

```
$ dolt init
Successfully initialized dolt data repository.
$ dolt sql -q "create table state_populations ( state varchar(14), population int, primary key (state) )"
$ dolt sql -q "show tables"
+-------------------+
| tables            |
+-------------------+
| state_populations |
+-------------------+
$ dolt sql -q "insert into state_populations (state, population) values
('Delaware', 59096),
('Maryland', 319728),
('Tennessee', 35691),
('Virginia', 691937),
('Connecticut', 237946),
('Massachusetts', 378787),
('South Carolina', 249073),
('New Hampshire', 141885),
('Vermont', 85425),
('Georgia', 82548),
('Pennsylvania', 434373),
('Kentucky', 73677),
('New York', 340120),
('New Jersey', 184139),
('North Carolina', 393751),
('Maine', 96540),
('Rhode Island', 68825)"
Query OK, 17 rows affected
```

Use `dolt sql` to jump into a SQL shell, or run single queries with
the `-q` option.

```
$ dolt sql -q "select * from state_populations where state = 'New York'"
+----------+------------+
| state    | population |
+----------+------------+
| New York | 340120     |
+----------+------------+
```

`add` the new tables and `commit` them. Every command matches `git`
exactly, but with tables instead of files.

```
$ dolt add .
$ dolt commit -m "initial data"
$ dolt status
On branch master
nothing to commit, working tree clean
```

Update the tables with more SQL commands, this time using the shell:

```
$ dolt sql
# Welcome to the DoltSQL shell.
# Statements must be terminated with ';'.
# "exit" or "quit" (or Ctrl-D) to exit.
state_pops> update state_populations set population = 0 where state like 'New%';
Query OK, 3 rows affected
Rows matched: 3  Changed: 3  Warnings: 0
state_pops> exit
Bye
```

See what you changed with `dolt diff`:

```
$ dolt diff
diff --dolt a/state_populations b/state_populations
--- a/state_populations @ qqr3vd0ea6264oddfk4nmte66cajlhfl
+++ b/state_populations @ 17cinjh5jpimilefd57b4ifeetjcbvn2
+-----+---------------+------------+
|     | state         | population |
+-----+---------------+------------+
|  <  | New Hampshire | 141885     |
|  >  | New Hampshire | 0          |
|  <  | New Jersey    | 184139     |
|  >  | New Jersey    | 0          |
|  <  | New York      | 340120     |
|  >  | New York      | 0          |
+-----+---------------+------------+
```

Then commit your changes once more with `dolt add` and `dolt commit`.

```
$ dolt add state_populations
$ dolt commit -m "More like Old Jersey"
```

See the history of your repository with `dolt log`.

```
% dolt log
commit babgn65p1r5n36ao4gfdj99811qauo8j
Author: Zach Musgrave <zach@dolthub.com>
Date:   Wed Nov 11 13:42:27 -0800 2020

    More like Old Jersey

commit 9hgk7jb7hlkvvkbornpldcopqh2gn6jo
Author: Zach Musgrave <zach@dolthub.com>
Date:   Wed Nov 11 13:40:53 -0800 2020
        
    initial data
                
commit 8o8ldh58pjovn8uvqvdq2olf7dm63dj9
Author: Zach Musgrave <zach@dolthub.com>
Date:   Wed Nov 11 13:36:24 -0800 2020

    Initialize data repository
```

# Importing data

If you have data in flat files like CSV or JSON, you can import them
using the `dolt table import` command. Use `dolt table import -u` to
add data to an existing table, or `dolt table import -c` to create a
new one.

```
$ head -n3 data.csv
state,population
Delaware,59096
Maryland,319728
$ dolt table import -c -pk=state state_populations data.csv
```

# Branch and merge

Just like with git, it's a good idea to make changes on your own
branch, then merge them back to `master`. The `dolt checkout` command
works exactly the same as `git checkout`.

```
$ dolt checkout -b <branch>
```

The `merge` command works the same too.

```
$ dolt merge <branch>
```

# Working with remotes

Dolt supports remotes just like git. Remotes are set up automatically
when you clone data from one.

```
$ dolt clone dolthub/corona-virus
...
$ cd corona-virus
$ dolt remote -v
origin https://doltremoteapi.dolthub.com/dolthub/corona-virus
```

To push to a remote, you'll need credentials. Run `dolt login` to open
a browser to sign in and cache your local credentials. You can sign
into DoltHub with your Google account, your Github account, or with a
user name and password.

```
$ dolt login
```

If you have a repo that you created locally that you now want to push
to a remote, add a remote exactly like you would with git.

```
$ dolt remote add origin myname/myRepo
$ dolt remote -v
origin https://doltremoteapi.dolthub.com/myname/myRepo
```

And then push to it.

```
$ dolt push origin master
```

## Other remotes 

`dolt` also supports directory, aws, and gcs based remotes:

- file - Use a directory on your machine

```
dolt remote add <remote> file:///Users/xyz/abs/path/
```

- aws - Use an S3 bucket

```
dolt remote add <remote> aws://dynamo-table:s3-bucket/database
```

- gs - Use a GCS bucket

```
dolt remote add <remote> gs://gcs-bucket/database
```

# Interesting datasets to clone

[DoltHub](https://dolthub.com) has lots of interesting datasets to
explore and clone. Here are some of our favorites.

- Coronavirus: https://www.dolthub.com/repositories/dolthub/corona-virus
- WordNet: https://www.dolthub.com/repositories/dolthub/word-net
- ImageNet: https://www.dolthub.com/repositories/dolthub/image-net
- Google Open Images: https://www.dolthub.com/repositories/dolthub/open-images
- Iris Classification: https://www.dolthub.com/repositories/dolthub/classified-iris-measurements
- Public Holidays: https://www.dolthub.com/repositories/oscarbatori/holidays
- IP Address to Country: https://www.dolthub.com/repositories/dolthub/ip-to-country

# More documentation

There's a lot more to Dolt than can fit in a README file! For full
documentation, check out the [docs on
DoltHub](https://www.dolthub.com/docs/). Some of the topics we didn't
cover here:

* [Querying past revisions of your
  tables](https://www.dolthub.com/docs/reference/sql/#querying-non-head-revisions-of-a-database)
* [Starting a SQL
  server](https://www.dolthub.com/docs/tutorials/using-sql/#getting-started_mysql-server)
* [Selecting the diff between two
  commits](https://www.dolthub.com/docs/reference/sql/#dolt-system-tables)
* [Documentation for all CLI
  commands](https://www.dolthub.com/docs/reference/cli/)

# Credits and License

Dolt relies heavily on open source code and ideas from the
[Noms](https://github.com/attic-labs/noms) project. We are very
thankful to the Noms team for making this code freely available,
without which we would not have been able to build Dolt so rapidly.

Dolt is licensed under the Apache License, Version 2.0. See
[LICENSE](https://github.com/dolthub/dolt/blob/master/LICENSE) for
details.
