# Dolt is Git for Data!

[Dolt](https://doltdb.com) is a SQL database that you can fork, clone, branch, merge, push
and pull just like a Git repository. Connect to Dolt just like any
MySQL database to run queries or update the data using SQL
commands. Use the command line interface to import CSV files, commit
your changes, push them to a remote, or merge your teammate's changes.

All the commands you know for Git work exactly the same for Dolt. Git
versions files, Dolt versions tables. It's like Git and MySQL had a
baby.

We also built [DoltHub](https://www.dolthub.com), a place to share
Dolt databases. We host public data for free. If you want to host
your own version of DoltHub, we have [DoltLab](https://www.doltlab.com). If you want us to run a Dolt server for you, we have [Hosted Dolt](https://hosted.doltdb.com).

[Join us on Discord](https://discord.com/invite/RFwfYpu) to say hi and
ask questions, or [check out our roadmap](https://docs.dolthub.com/other/roadmap)
to see what we're building next.

## What's it for?

Lots of things! Dolt is a generally useful tool with countless
applications. But if you want some ideas, [here's how people are using
it so far](https://www.dolthub.com/blog/2022-07-11-dolt-case-studies/).

Learn more about Dolt use cases, configuration and guides to use dolt on our [documentation page](https://docs.dolthub.com/introduction/what-is-dolt).

# How to use this image

This image is for Dolt CLI, which has the same commands as `git`, with some extras. Running this image without any 
arguments is equivalent to running `dolt` command locally.

```shell
$ docker run dolthub/dolt:latest
Valid commands for dolt are
                init - Create an empty Dolt data repository.
              status - Show the working tree status.
                 add - Add table changes to the list of staged table changes.
                diff - Diff a table.
               reset - Remove table changes from the list of staged table changes.
               clean - Remove untracked tables from working set.
              commit - Record changes to the repository.
                 sql - Run a SQL query against tables in repository.
          sql-server - Start a MySQL-compatible server.
          sql-client - Starts a built-in MySQL client.
                 log - Show commit logs.
              branch - Create, list, edit, delete branches.
            checkout - Checkout a branch or overwrite a table from HEAD.
               merge - Merge a branch.
           conflicts - Commands for viewing and resolving merge conflicts.
         cherry-pick - Apply the changes introduced by an existing commit.
              revert - Undo the changes introduced in a commit.
               clone - Clone from a remote data repository.
               fetch - Update the database from a remote data repository.
                pull - Fetch from a dolt remote data repository and merge.
                push - Push to a dolt remote.
              config - Dolt configuration.
              remote - Manage set of tracked repositories.
              backup - Manage a set of server backups.
               login - Login to a dolt remote host.
               creds - Commands for managing credentials.
                  ls - List tables in the working set.
              schema - Commands for showing and importing table schemas.
               table - Commands for copying, renaming, deleting, and exporting tables.
                 tag - Create, list, delete tags.
               blame - Show what revision and author last modified each row of a table.
         constraints - Commands for handling constraints.
             migrate - Executes a database migration to use the latest Dolt data format.
         read-tables - Fetch table(s) at a specific commit into a new dolt repo
                  gc - Cleans up unreferenced data from the repository.
       filter-branch - Edits the commit history using the provided query.
          merge-base - Find the common ancestor of two commits.
             version - Displays the current Dolt cli version.
                dump - Export all tables in the working set into a file.
```

This image is useful for creating custom Docker Image using this image as base image.
