# nomsconfig

The noms cli now provides experimental support for configuring a convenient default database and database aliases.

You can enable this support by placing a *.nomsconfig* config file (like the [one](.nomsconfig) in this sample) in the directory where you'd like to use the configuration. Like git, any noms command issued from that directory or below will use it.

# Features

- *Database Aliases* - Define simple names to be used in place of database URLs
- *Default Database* - Define one database to be used by default when no database in mentioned
- *Dot (`.`) Shorthand* - Use `.` instead of repeating dataset/object name in destination

# Example

This example defines a simple [.nomsconfig](.nomsconfig) to try:

```
# Default database URL to be used whenever a database is not explictly provided
[db.default]
url = "ldb:.noms/tour"

# DB alias named `origin` that refers to the remote cli-tour db 
[db.origin]
url = "http://demo.noms.io/cli-tour"

# DB alias named `temp` that refers to a noms db stored under /tmp
[db.temp]
url = "ldb:/tmp/noms/shared

```

The *[db.default]* section:

 - Defines a default database
 - It will be used implicitly whenever a database url is omitted in a command

The *[db.origin]* and *[db.shared]* sections:

 - Define aliases that can be used wherever a db url is required
 - You can define additional aliases by adding *[db.**alias**]* sections using any **alias** you prefer

Dot (`.`) shorthand:

 - When issuing a command that requires a source and destination (like `noms sync`), 
   you can use `.` in place of the dataset/object in the destination. This is shorthand 
   that repeats whatever was used in the source (see below).


You can kick the tires by running noms commands from this directory. Here are some examples and what to expect:

```
noms ds          # -> noms ds ldb:.noms/tour
noms ds default  # -> noms ds ldb:.noms/tour
noms ds origin   # -> noms ds http://demo.noms.io/cli-tour

noms sync origin::sf-film-locations sf-films   # sync ds from origin to default

noms log sf-films                    # -> noms log ldb:.noms/tour::sf-films
noms log origin::sf-film-locations   # -> noms log http://demo.noms.io/cli-tour::sf-film-locations

noms show '#1a2aj8svslsu7g8hplsva6oq6iq3ib6c'         # -> noms show ldb:.noms/tour::'#1a2a...'
noms show origin::'#1a2aj8svslsu7g8hplsva6oq6iq3ib6c' # -> noms show http://demo.noms.io/cli-tour::'#1a2a...'

noms diff '#1a2aj8svslsu7g8hplsva6oq6iq3ib6c' origin::. # diff default::object with origin::object

noms sync origin::sf-bike-parking . # sync origin::sf-bike-parking to default::sf-bike-parking

``` 

A few more things to note:

 - Relative paths will be expanded relative to the directory where the *.nomsconfg* is defined
 - Use `noms config` to see the current alias definitions with expanded paths
 - Use `-v` or `--verbose` on any command to see how the command arguments are being resolved
 - Explicit DB urls are still fully supported