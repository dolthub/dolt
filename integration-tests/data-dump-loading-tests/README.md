## Data Dump Loading Test
We created tests for loading data dumps from mysqldump, and we run these tests through Github Actions
on pull requests.

These tests can be run locally using Docker. From the root directory of this repo, run:
```bash
$ docker build -t data-dump-loading-tests -f DataDumpLoadDockerfile .
$ docker run data-dump-loading-tests:latest
```

The `docker build` step will take a few minutes to complete as it needs to install all of the
dependencies in the image.

Running the built container will produce output like:
```bash
$ docker run data-dump-loading-tests:latest
updating dolt config for tests:
Config successfully updated.
Config successfully updated.
Config successfully updated.
Config successfully updated.
Running data-dump-loading-tests:
1..2
ok 1 import mysqldump: empty database dump
ok 2 import mysqldump: a simple table dump
```

We are using MySQL Sakila Database dump file with few modifications (commented out some parts that are not supported yet) to fit Dolt.
These commented out parts can be found with 'UNSUPPORTED SYNTAX' in search bar in 'sakila_dump.sql' file. The stored functions and
procedures from the original database is not present in the dump file. Only one procedure is added to fit and test Dolt procedure 
functionality.

The Sakila database dump file is the only dump file we have tests for. If there are more dump files to be added as needed for 
more testing, we need to use different way to store those dump files.

Sakila Database License:

-- Sakila Sample Database Data
-- Version 1.2

-- Copyright (c) 2022, Oracle and/or its affiliates.

-- Redistribution and use in source and binary forms, with or without
-- modification, are permitted provided that the following conditions are
-- met:

-- * Redistributions of source code must retain the above copyright notice,
--   this list of conditions and the following disclaimer.
-- * Redistributions in binary form must reproduce the above copyright
--   notice, this list of conditions and the following disclaimer in the
--   documentation and/or other materials provided with the distribution.
-- * Neither the name of Oracle nor the names of its contributors may be used
--   to endorse or promote products derived from this software without
--   specific prior written permission.

-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
-- IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
-- THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
-- PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
-- CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
-- EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
-- PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
-- PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
-- LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
-- NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
-- SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
