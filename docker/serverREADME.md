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

# Dolt CLI

The `dolt` CLI has the same commands as `git`, with some extras.

```
$ dolt
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

Learn more about Dolt use cases, configuration and guides to use dolt on our [documentation page](https://docs.dolthub.com/introduction/what-is-dolt).

# How to use this image

This image is for Dolt SQL Server, which is similar to MySQL Docker Image. Running this image without any arguments 
is equivalent to running `dolt sql-server --host 0.0.0.0 --port 3306` command locally. The reason for persisted host
and port is that it allows user to connect to the server inside the container from the local host system through
port-mapping.

To check out supported options for `dolt sql-server`, you can run the image with `--help` flag.

```shell
$ docker run dolthub/dolt-sql-server:latest --help
```

### Connect to the server in the container from the host system

To be able to connect to the server running in the container, we need to set up a port to connect to locally that
maps to the port in the container. The host is set to `0.0.0.0` for accepting connections to any available network 
interface.

```shell
$ docker run -p 3307:3306 dolthub/dolt-sql-server:latest
```

Now, you have a running server in the container, and we can connect to it by specifying our host, 3307 for the port, and root for the user, 
since that's the default user and we didn't provide any configuration when running the server.

For example, you can run mysql client to connect to the server like this:
```shell
$ mysql --host 0.0.0.0 -P 3307 -u root
```

### Define configuration for the server

You can either define server configuration as commandline arguments, or you can use yaml configuration file.
For the commandline argument definition you can simply define arguments after whole docker command. 

```shell
$ docker run -p 3307:3306 dolthub/dolt-sql-server:latest -l debug --no-auto-commit
```

Or, we can mount a local directory to specific directories in the container.
The special directory for server configuration is `/etc/dolt/servercfg.d/`. You can only have one `.yaml` configuration
file in this directory. If there are multiple, the default configuration will be used. If the location of
configuration file was `/Users/jennifer/docker/server/config.yaml`, this is how to use `-v` flag which mounts
`/Users/jennifer/docker/server/` local directory to `/etc/dolt/servercfg.d/` directory in the container.

```shell
$ docker run -p 3307:3306 -v /Users/jennifer/docker/server/:/etc/dolt/servercfg.d/ dolthub/dolt-sql-server:latest
```

The Dolt configuration and data directories can be configured similarly: 

- The dolt configuration directory is `/etc/dolt/doltcfg.d/`
There should be one `.json` dolt configuration file. It will replace the global dolt configuration file in the 
container.

- We set the location of where data to be stored to default location at `/var/lib/dolt/` in the container. 
The data directory does not need to be defined in server configuration for container, but to store the data 
on the host system, it can also be mounted to this default location.

```shell
$ docker run -p 3307:3306 -v /Users/jennifer/docker/databases/:/var/lib/dolt/ dolthub/dolt-sql-server:latest
```
