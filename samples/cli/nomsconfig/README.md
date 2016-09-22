# nomsconfig

The *.nomsconfig* file is an experimental feature that allows you to define 
a default database and aliases to simplify the noms command line.

When *.nomsconfig* is present in the current directory or ancestor, it's
definitions will be active when running noms cli. 

This directory contains an [example](.gitconfig) config to try out. If you lke what 
you see once you've kicked the tires, you can create your own *.nomsconfig*'s wherever 
you need one.

A *.gitconfig* file can contain a single default database definition and 
any number of database aliases. Here's are the definitons from the example:
 
```
# Default database URL to be used whenever a database is not explictly provided
[default]
url = "ldb:.noms/tour"

# DB alias named `origin` that refers to the remote cli-tour db 
[db.origin]
url = "http://demo.noms.io/cli-tour"

# DB alias named `tour` provides an explicit name for the default database
[db.tour]
url = "ldb:.noms/tour"
```

The *[default]* definition will be used implicitly whenever a database is
required but not specified. 

The *origin* and *tour* aliases can be used in place of a dataspec.  

You can try out this config by running the noms cli in this directory. Here are some 
commands to try and what to expect:

```
noms ds          # noms ds ldb:.noms/tour 
noms ds origin   # noms ds http://demo.noms.io/cli-tour

noms sync origin::sf-film-locations sf-films   # sync from remote to local

noms log sf-films                    # noms log ldb:.noms/tour::sf-films
noms log origin::sf-film-locations   # noms log http://demo.noms.io/cli-tour::sf-film-locations

noms show '#1a2aj8svslsu7g8hplsva6oq6iq3ib6c'         # noms show ldb:.noms/tour::'...'
noms show origin::'#1a2aj8svslsu7g8hplsva6oq6iq3ib6c' # noms show http://demo.noms.io/cli-tour::'...'

noms diff '#1a2aj8...' origin::'#1a2aj8...'  # diff local object with object at origin

``` 

Note that explicit DB urls are still fully supported.