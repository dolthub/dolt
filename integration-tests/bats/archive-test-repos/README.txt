
This directory contains dolt database files which are used as part of the
archive.bats tests.

Each directory is a .dolt directory which can be copied into place
for a given test.

Eg:

$ mkdir -p repo/.dolt
$ cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* repo/.dolt
$ cd repo
$ dolt sql

The Directories are as follows:
--------------------------------------------------------------------------
base: This was the first repo created. It's contents are all archive files.

$ dolt admin storage
Storage Artifact:
  ID: 8p5e2m6skovfdjlh4jg3llr8sfvu384l
  Path: /Users/neil/Documents/data_dir_1/db1/base/.dolt/noms/8p5e2m6skovfdjlh4jg3llr8sfvu384l
  Table File Metadata:
    Snappy Chunk Count: 2 (bytes: 382)

Storage Artifact:
  ID: 29o8a3uevcpr15tilcemb3s438edmoog
  Path: /Users/neil/Documents/data_dir_1/db1/base/.dolt/noms/oldgen/29o8a3uevcpr15tilcemb3s438edmoog.darc
  Archive Metadata:
    Format Version: 1
    Snappy Chunk Count: 0 (bytes: 0)
    ZStd Chunk Count: 125 (bytes: 51318)
    Dictionary Count: 1 (bytes: 2620)

Storage Artifact:
  ID: dnu4lr5j8sstbj5usbld7alsnuj5nf23
  Path: /Users/neil/Documents/data_dir_1/db1/base/.dolt/noms/oldgen/dnu4lr5j8sstbj5usbld7alsnuj5nf23.darc
  Archive Metadata:
    Format Version: 1
    Snappy Chunk Count: 0 (bytes: 0)
    ZStd Chunk Count: 139 (bytes: 105419)
    Dictionary Count: 1 (bytes: 2641)


--------------------------------------------------------------------------
large_clone: a full clone of base, with a bunch of mutations on it. It was
        then garbage collected to ensure all objects were in Snappy format

        This repo has more than 1000 new chunks in it, which is important
        because when we push with archive as the default, all snappy
        objects will be converted into zstd compressed objects.

# clones from base.
$ dolt clone http://localhost:$port/test-org/test-repo clone1
$ cd clone1
$ dolt sql -q "$(mutations_and_gc_statement)" # see archive.bats 
... repeated ...
$ dolt sql -q "$(mutations_and_gc_statement)"
$ dolt gc --full

$ dolt admin storage
Storage Artifact:
  ID: p6d0rsovtipdk6r2b1kh9qrndif41703
  Path: /Users/neil/Documents/data_dir_1/db1/clone1/.dolt/noms/p6d0rsovtipdk6r2b1kh9qrndif41703
  Table File Metadata:
    Snappy Chunk Count: 2 (bytes: 386)

Storage Artifact:
  ID: b5j6kmj2m68sukiu22ouoke7u9281a06
  Path: /Users/neil/Documents/data_dir_1/db1/clone1/.dolt/noms/oldgen/b5j6kmj2m68sukiu22ouoke7u9281a06
  Table File Metadata:
    Snappy Chunk Count: 1873 (bytes: 4119222)

--------------------------------------------------------------------------
small_clone: a full clone of base with a very small number of mutations on it.

        Similar to clone1, it was garbage collected to convert chunks into
        the snappy format. There are only a few new chunks though, so
        pushing with archives enabled will result in archive files which
        have snappy objects in them.

$ dolt admin storage
Storage Artifact:
  ID: sv0o2e33pel8caor7979s4rk227raink
  Path: /Users/neil/Documents/data_dir_1/db1/clone2/.dolt/noms/sv0o2e33pel8caor7979s4rk227raink
  Table File Metadata:
    Snappy Chunk Count: 2 (bytes: 386)

Storage Artifact:
  ID: c5j9u4ced6eg6cnegk0mgdglc3t04air
  Path: /Users/neil/Documents/data_dir_1/db1/clone2/.dolt/noms/oldgen/c5j9u4ced6eg6cnegk0mgdglc3t04air
  Table File Metadata:
    Snappy Chunk Count: 273 (bytes: 377979)

--------------------------------------------------------------------------
v1: A copy of base. This is used to verify we can read from an archive
        file which was created with the version 1 archive format.

Content: see `base` above.

--------------------------------------------------------------------------
v2: This is used to verify we can read from an archive in format 2.

$ dolt admin storage
Storage Artifact:
  ID: i8ivrn485g73hao1foo3p7l2clq3gqqv
  Path: /Users/neil/Documents/data_dir_1/db8/.dolt/noms/i8ivrn485g73hao1foo3p7l2clq3gqqv
  Table File Metadata:
    Snappy Chunk Count: 2 (bytes: 383)

Storage Artifact:
  ID: 27avtn2a3upddh52eu750m4709gfps7s
  Path: /Users/neil/Documents/data_dir_1/db8/.dolt/noms/oldgen/27avtn2a3upddh52eu750m4709gfps7s.darc
  Archive Metadata:
    Format Version: 2
    Snappy Chunk Count: 0 (bytes: 0)
    ZStd Chunk Count: 230 (bytes: 58956)
    Dictionary Count: 1 (bytes: 2968)