# General

This code uses git submodules. You need to recursively pull all the submodules
in order for it to build.

# Building on OS X

```sh
$ brew install cmake openssl mysql-client boost
$ export PATH=/usr/local/Cellar/mysql-client/8.0.21/bin/:"$PATH"
$ mkdir _build
$ cd _build
$ cmake .. -DWITH_SSL=/usr/local/Cellar/openssl@1.1/1.1.1g/ -DWITH_JDBC=yes
$ make -j 10
```

TODO: These instructions are coupled to openssl and mysql-client version that
happen to be installed...

# Build on Ubuntu / Debian

```sh
$ apt-get install g++ cmake libmysqlcppconn-dev
$ mkdir _build
$ cd _build
$ cmake ..
$ make -j 10
```
