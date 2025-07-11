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

This image is for Dolt SQL Server, which is similar to the MySQL Docker image. Running this image without any arguments 
is equivalent to running `dolt sql-server --host 0.0.0.0 --port 3306` command inside a Docker container. 

To see all supported options for `dolt sql-server`, you can run the image with `--help` flag.

```shell
$ docker run dolthub/dolt-sql-server:latest --help
```

### Connect to the server in the container from the host system

From the host system, to connect to a server running in a container, we need to map a port on the host system to the port our sql-server is running on in the container.

We also need a user account that has permission to connect to the server
from the host system's address. By default, as of Dolt version 1.46.0, the `root` superuser is configured to only allow connections from localhost. This is a security feature to prevent unauthorized access to the server. If you don't want to log in to the container and then connect to your sql-server, you can use the `DOLT_ROOT_HOST` and `DOLT_ROOT_PASSWORD` environment variables to control how the `root` superuser is initialized. When the Docker container is started, it will ensure the `root` superuser is configured according to those environment variables.

In our example below, we're using `DOLT_ROOT_HOST` to override the host of the `root` superuser account to `%` in order to allow any host to connect to our server and log in as `root`. We're also using `DOLT_ROOT_PASSWORD` to override the default, empty password and specify a password for the `root` account. Setting a password is strongly advised for security when allowing the `root` account to connect from any host.

```bash
> docker run -e DOLT_ROOT_PASSWORD=secret2 -e DOLT_ROOT_HOST=% -p 3307:3306 dolthub/dolt-sql-server:latest
```

If we run the command above with -d or switch to a separate window we can connect with MySQL:

```bash
> mysql --host 0.0.0.0 -P 3307 -u root -p secret2
```

### Define configuration for the server

You can specify server configuration with commandline arguments, or you can use a YAML configuration file.
For commandline arguments, you can simply add arguments at the end of the docker command, as shown below. 

```shell
$ docker run -p 3307:3306 dolthub/dolt-sql-server:latest -l debug --no-auto-commit
```

To use a configuration file, you can map a local directory to location in the container.
The special directory for server configuration is `/etc/dolt/servercfg.d/`. You can only have one `.yaml` configuration
file in this directory. If there are multiple, the default configuration will be used. If the location of
configuration file was `/Users/jennifer/docker/server/config.yaml`, this is how to use `-v` flag which mounts
`/Users/jennifer/docker/server/` local directory to `/etc/dolt/servercfg.d/` directory in the container.

```shell
$ docker run -p 3307:3306 -v /Users/jennifer/docker/server/:/etc/dolt/servercfg.d/ dolthub/dolt-sql-server:latest
```

The Dolt configuration and data directories can be configured similarly: 

- The dolt configuration directory is `/etc/dolt/doltcfg.d/`
There should be one `.json` Dolt configuration file. It will replace the global Dolt configuration file in the 
container.

- We set the location of where data to be stored to default location at `/var/lib/dolt/` in the container. 
The data directory does not need to be defined in server configuration for container, but to store the data 
on the host system, it can also be mounted to this default location.

```shell
$ docker run -p 3307:3306 -v /Users/jennifer/docker/databases/:/var/lib/dolt/ dolthub/dolt-sql-server:latest
```

## Environment Variables

The Dolt SQL Server image supports the following environment variables:

- `DOLT_ROOT_PASSWORD`: Sets the password for the root user
- `DOLT_ROOT_HOST`: Specifies a host for the root user (default: localhost)
- `DOLT_DATABASE` / `MYSQL_DATABASE`: Creates a database with this name if it doesn't exist
- `DOLT_USER` / `MYSQL_USER`: Creates a user with this name if it doesn't exist
- `DOLT_PASSWORD` / `MYSQL_PASSWORD`: Sets the password for the user specified in `DOLT_USER`/`MYSQL_USER`

The user will be granted all privileges on the database specified by `DOLT_DATABASE`/`MYSQL_DATABASE` if provided.
