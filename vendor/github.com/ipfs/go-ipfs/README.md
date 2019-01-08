# go-ipfs

![banner](https://ipfs.io/ipfs/QmVk7srrwahXLNmcDYvyUEJptyoxpndnRa57YJ11L4jV26/ipfs.go.png)

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://ipfs.io/)
[![](https://img.shields.io/badge/freenode-%23ipfs-blue.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23ipfs)
[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![GoDoc](https://godoc.org/github.com/ipfs/go-ipfs?status.svg)](https://godoc.org/github.com/ipfs/go-ipfs)
[![Build Status](https://travis-ci.org/ipfs/go-ipfs.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs)

## Project Status

[![Throughput Graph](https://graphs.waffle.io/ipfs/go-ipfs/throughput.svg)](https://waffle.io/ipfs/go-ipfs/metrics/throughput)

[**`Weekly Core Dev Calls`**](https://github.com/ipfs/pm/issues/674)

## What is IPFS?

IPFS is a global, versioned, peer-to-peer filesystem. It combines good ideas from Git, BitTorrent, Kademlia, SFS, and the Web. It is like a single bittorrent swarm, exchanging git objects. IPFS provides an interface as simple as the HTTP web, but with permanence built in. You can also mount the world at /ipfs.

For more info see: https://github.com/ipfs/ipfs.

Please put all issues regarding:
  - IPFS _design_ in the [ipfs repo issues](https://github.com/ipfs/ipfs/issues).
  - Go IPFS _implementation_ in [this repo](https://github.com/ipfs/go-ipfs/issues).

## Table of Contents

- [Security Issues](#security-issues)
- [Install](#install)
  - [System Requirements](#system-requirements)
  - [Install prebuilt packages](#install-prebuilt-packages)
  - [From Linux package managers](#from-linux-package-managers)
  - [Build from Source](#build-from-source)
    - [Install Go](#install-go)
    - [Download and Compile IPFS](#download-and-compile-ipfs)
    - [Troubleshooting](#troubleshooting)
  - [Development Dependencies](#development-dependencies)
  - [Updating](#updating-go-ipfs)
- [Usage](#usage)
- [Getting Started](#getting-started)
  - [Some things to try](#some-things-to-try)
  - [Docker usage](#docker-usage)
  - [Troubleshooting](#troubleshooting-1)
- [Packages](#packages)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Security Issues

The IPFS protocol and its implementations are still in heavy development. This means that there may be problems in our protocols, or there may be mistakes in our implementations. And -- though IPFS is not production-ready yet -- many people are already running nodes in their machines. So we take security vulnerabilities very seriously. If you discover a security issue, please bring it to our attention right away!

If you find a vulnerability that may affect live deployments -- for example, by exposing a remote execution exploit -- please send your report privately to security@ipfs.io. Please DO NOT file a public issue. The GPG key for security@ipfs.io is [4B9665FB 92636D17 7C7A86D3 50AAE8A9 59B13AF3](https://pgp.mit.edu/pks/lookup?op=get&search=0x50AAE8A959B13AF3).

If the issue is a protocol weakness that cannot be immediately exploited or something not yet deployed, just discuss it openly.

## Install

The canonical download instructions for IPFS are over at: http://ipfs.io/docs/install/. It is **highly suggested** you follow those instructions if you are not interested in working on IPFS development.

### System Requirements

IPFS can run on most Linux, macOS, and Windows systems. We recommend running it on a machine with at least 2 GB of RAM (it’ll do fine with only one CPU core), but it should run fine with as little as 1 GB of RAM. On systems with less memory, it may not be completely stable.

### Install prebuilt packages

We host prebuilt binaries over at our [distributions page](https://ipfs.io/ipns/dist.ipfs.io#go-ipfs).

From there:
- Click the blue "Download go-ipfs" on the right side of the page.
- Open/extract the archive.
- Move `ipfs` to your path (`install.sh` can do it for you).

You can also download go-ipfs from this project's GitHub releases page if you are unable to access ipfs.io.

### From Linux package managers

- [Arch Linux](#arch-linux)
- [Nix](#nix)
- [Snap](#snap)

#### Arch Linux

In Arch Linux go-ipfs is available as
[go-ipfs](https://www.archlinux.org/packages/community/x86_64/go-ipfs/) package.

```
$ sudo pacman -S go-ipfs
```

Development version of go-ipfs is also on AUR under
[go-ipfs-git](https://aur.archlinux.org/packages/go-ipfs-git/).
You can install it using your favourite AUR Helper or manually from AUR.

#### Nix

For Linux and MacOSX you can use the purely functional package manager [Nix](https://nixos.org/nix/):

```
$ nix-env -i ipfs
```
You can also install the Package by using it's attribute name, which is also `ipfs`.

#### Snap

With snap, in any of the [supported Linux distributions](https://snapcraft.io/docs/core/install):

```
$ sudo snap install ipfs
```

### Build from Source

#### Install Go

The build process for ipfs requires Go 1.10 or higher. If you don't have it: [Download Go 1.10+](https://golang.org/dl/).

You'll need to add Go's bin directories to your `$PATH` environment variable e.g., by adding these lines to your `/etc/profile` (for a system-wide installation) or `$HOME/.profile`:

```
export PATH=$PATH:/usr/local/go/bin
export PATH=$PATH:$GOPATH/bin
```

(If you run into trouble, see the [Go install instructions](https://golang.org/doc/install)).

#### Download and Compile IPFS

```
$ go get -u -d github.com/ipfs/go-ipfs

$ cd $GOPATH/src/github.com/ipfs/go-ipfs
$ make install
```

If you are building on FreeBSD instead of `make install` use `gmake install`.

#### Building on less common systems

If your operating system isn't officially supported, but you still want to try
building ipfs anyways (it should work fine in most cases), you can do the
following instead of `make install`:

```
$ make install_unsupported
```

Note: This process may break if [gx](https://github.com/whyrusleeping/gx)
(used for dependency management) or any of its dependencies break as `go get`
will always select the latest code for every dependency, often resulting in
mismatched APIs.

#### Troubleshooting

- Separate [instructions are available for building on Windows](docs/windows.md).
- Also, [instructions for OpenBSD](docs/openbsd.md).
- `git` is required in order for `go get` to fetch all dependencies.
- Package managers often contain out-of-date `golang` packages.
  Ensure that `go version` reports at least 1.10. See above for how to install go.
- If you are interested in development, please install the development
dependencies as well.
- _WARNING_: Older versions of OSX FUSE (for Mac OS X) can cause kernel panics when mounting!-
  We strongly recommend you use the [latest version of OSX FUSE](http://osxfuse.github.io/).
  (See https://github.com/ipfs/go-ipfs/issues/177)
- For more details on setting up FUSE (so that you can mount the filesystem), see the docs folder.
- Shell command completion is available in `misc/completion/ipfs-completion.bash`. Read [docs/command-completion.md](docs/command-completion.md) to learn how to install it.
- See the [init examples](https://github.com/ipfs/website/tree/master/static/docs/examples/init) for how to connect IPFS to systemd or whatever init system your distro uses.

### Updating go-ipfs

#### Using ipfs-update

IPFS has an updating tool that can be accessed through `ipfs update`. The tool is
not installed alongside IPFS in order to keep that logic independent of the main
codebase. To install `ipfs update`, [download it here](https://ipfs.io/ipns/dist.ipfs.io/#ipfs-update).

#### Downloading IPFS builds using IPFS

List the available versions of go-ipfs:

```
$ ipfs cat /ipns/dist.ipfs.io/go-ipfs/versions
```

Then, to view available builds for a version from the previous command ($VERSION):

```
$ ipfs ls /ipns/dist.ipfs.io/go-ipfs/$VERSION
```

To download a given build of a version:

```
$ ipfs get /ipns/dist.ipfs.io/go-ipfs/$VERSION/go-ipfs_$VERSION_darwin-386.tar.gz # darwin 32-bit build
$ ipfs get /ipns/dist.ipfs.io/go-ipfs/$VERSION/go-ipfs_$VERSION_darwin-amd64.tar.gz # darwin 64-bit build
$ ipfs get /ipns/dist.ipfs.io/go-ipfs/$VERSION/go-ipfs_$VERSION_freebsd-amd64.tar.gz # freebsd 64-bit build
$ ipfs get /ipns/dist.ipfs.io/go-ipfs/$VERSION/go-ipfs_$VERSION_linux-386.tar.gz # linux 32-bit build
$ ipfs get /ipns/dist.ipfs.io/go-ipfs/$VERSION/go-ipfs_$VERSION_linux-amd64.tar.gz # linux 64-bit build
$ ipfs get /ipns/dist.ipfs.io/go-ipfs/$VERSION/go-ipfs_$VERSION_linux-arm.tar.gz # linux arm build
$ ipfs get /ipns/dist.ipfs.io/go-ipfs/$VERSION/go-ipfs_$VERSION_windows-amd64.zip # windows 64-bit build
```

## Usage

```
  ipfs - Global p2p merkle-dag filesystem.

  ipfs [<flags>] <command> [<arg>] ...

SUBCOMMANDS
  BASIC COMMANDS
    init          Initialize ipfs local configuration
    add <path>    Add a file to ipfs
    cat <ref>     Show ipfs object data
    get <ref>     Download ipfs objects
    ls <ref>      List links from an object
    refs <ref>    List hashes of links from an object

  DATA STRUCTURE COMMANDS
    block         Interact with raw blocks in the datastore
    object        Interact with raw dag nodes
    files         Interact with objects as if they were a unix filesystem

  ADVANCED COMMANDS
    daemon        Start a long-running daemon process
    mount         Mount an ipfs read-only mountpoint
    resolve       Resolve any type of name
    name          Publish or resolve IPNS names
    dns           Resolve DNS links
    pin           Pin objects to local storage
    repo          Manipulate an IPFS repository

  NETWORK COMMANDS
    id            Show info about ipfs peers
    bootstrap     Add or remove bootstrap peers
    swarm         Manage connections to the p2p network
    dht           Query the DHT for values or peers
    ping          Measure the latency of a connection
    diag          Print diagnostics

  TOOL COMMANDS
    config        Manage configuration
    version       Show ipfs version information
    update        Download and apply go-ipfs updates
    commands      List all available commands

  Use 'ipfs <command> --help' to learn more about each command.

  ipfs uses a repository in the local file system. By default, the repo is located
  at ~/.ipfs. To change the repo location, set the $IPFS_PATH environment variable:

    export IPFS_PATH=/path/to/ipfsrepo
```

## Getting Started

See also: http://ipfs.io/docs/getting-started/

To start using IPFS, you must first initialize IPFS's config files on your
system, this is done with `ipfs init`. See `ipfs init --help` for information on
the optional arguments it takes. After initialization is complete, you can use
`ipfs mount`, `ipfs add` and any of the other commands to explore!

### Some things to try

Basic proof of 'ipfs working' locally:

	echo "hello world" > hello
	ipfs add hello
	# This should output a hash string that looks something like:
	# QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o
	ipfs cat <that hash>


### Docker usage

An IPFS docker image is hosted at [hub.docker.com/r/ipfs/go-ipfs](https://hub.docker.com/r/ipfs/go-ipfs/).
To make files visible inside the container you need to mount a host directory
with the `-v` option to docker. Choose a directory that you want to use to
import/export files from IPFS. You should also choose a directory to store
IPFS files that will persist when you restart the container.

    export ipfs_staging=</absolute/path/to/somewhere/>
    export ipfs_data=</absolute/path/to/somewhere_else/>

Start a container running ipfs and expose ports 4001, 5001 and 8080:

    docker run -d --name ipfs_host -v $ipfs_staging:/export -v $ipfs_data:/data/ipfs -p 4001:4001 -p 127.0.0.1:8080:8080 -p 127.0.0.1:5001:5001 ipfs/go-ipfs:latest

Watch the ipfs log:

    docker logs -f ipfs_host

Wait for ipfs to start. ipfs is running when you see:

    Gateway (readonly) server
    listening on /ip4/0.0.0.0/tcp/8080

You can now stop watching the log.

Run ipfs commands:

    docker exec ipfs_host ipfs <args...>

For example: connect to peers

    docker exec ipfs_host ipfs swarm peers

Add files:

    cp -r <something> $ipfs_staging
    docker exec ipfs_host ipfs add -r /export/<something>

Stop the running container:

    docker stop ipfs_host

### Troubleshooting

If you have previously installed IPFS before and you are running into problems getting a newer version to work, try deleting (or backing up somewhere else) your IPFS config directory (~/.ipfs by default) and rerunning `ipfs init`. This will reinitialize the config file to its defaults and clear out the local datastore of any bad entries.

Please direct general questions and help requests to our [forum](https://discuss.ipfs.io) or our IRC channel (freenode #ipfs).

If you believe you've found a bug, check the [issues list](https://github.com/ipfs/go-ipfs/issues) and, if you don't see your problem there, either come talk to us on IRC (freenode #ipfs) or file an issue of your own!

## Packages

> This table is generated using the module [`package-table`](https://github.com/ipfs-shipyard/package-table) with `package-table --data=package-list.json`.

Listing of the main packages used in the IPFS ecosystem. There are also three specifications worth linking here:

| Name | CI/Travis | CI/Jenkins | Coverage | Description |
| ---------|---------|---------|---------|--------- |
| **Files** |
| [`go-unixfs`](//github.com/ipfs/go-unixfs) | [![Travis CI](https://travis-ci.org/ipfs/go-unixfs.svg?branch=master)](https://travis-ci.org/ipfs/go-unixfs) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-unixfs/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-unixfs) | the core 'filesystem' logic |
| [`go-mfs`](//github.com/ipfs/go-mfs) | [![Travis CI](https://travis-ci.org/ipfs/go-mfs.svg?branch=master)](https://travis-ci.org/ipfs/go-mfs) | [![jenkins](https://ci.ipfs.team/buildStatus/icon?job=ipfs/go-mfs/master)](https://ci.ipfs.team/job/ipfs/job/go-mfs/job/master/) | [![codecov](https://codecov.io/gh/ipfs/go-mfs/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-mfs) | a mutable filesystem editor for unixfs |
| [`go-ipfs-posinfo`](//github.com/ipfs/go-ipfs-posinfo) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-posinfo.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-posinfo) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-posinfo/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-posinfo) | helper datatypes for the filestore |
| [`go-ipfs-chunker`](//github.com/ipfs/go-ipfs-chunker) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-chunker.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-chunker) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-chunker/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-chunker) | file chunkers |
| **Exchange** |
| [`go-ipfs-exchange-interface`](//github.com/ipfs/go-ipfs-exchange-interface) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-exchange-interface.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-exchange-interface) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-exchange-interface/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-exchange-interface) | exchange service interface |
| [`go-ipfs-exchange-offline`](//github.com/ipfs/go-ipfs-exchange-offline) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-exchange-offline.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-exchange-offline) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-exchange-offline/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-exchange-offline) | (dummy) offline implementation of the exchange service |
| [`go-bitswap`](//github.com/ipfs/go-bitswap) | [![Travis CI](https://travis-ci.org/ipfs/go-bitswap.svg?branch=master)](https://travis-ci.org/ipfs/go-bitswap) | [![jenkins](https://ci.ipfs.team/buildStatus/icon?job=ipfs/go-bitswap/master)](https://ci.ipfs.team/job/ipfs/job/go-bitswap/job/master/) | [![codecov](https://codecov.io/gh/ipfs/go-bitswap/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-bitswap) | bitswap protocol implementation |
| [`go-blockservice`](//github.com/ipfs/go-blockservice) | [![Travis CI](https://travis-ci.org/ipfs/go-blockservice.svg?branch=master)](https://travis-ci.org/ipfs/go-blockservice) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-blockservice/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-blockservice) | service that plugs a blockstore and an exchange together |
| **Datastores** |
| [`go-datastore`](//github.com/ipfs/go-datastore) | [![Travis CI](https://travis-ci.org/ipfs/go-datastore.svg?branch=master)](https://travis-ci.org/ipfs/go-datastore) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-datastore/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-datastore) | datastore interfaces, adapters, and basic implementations |
| [`go-ipfs-ds-help`](//github.com/ipfs/go-ipfs-ds-help) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-ds-help.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-ds-help) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-ds-help/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-ds-help) | datastore utility functions |
| [`go-ds-flatfs`](//github.com/ipfs/go-ds-flatfs) | [![Travis CI](https://travis-ci.org/ipfs/go-ds-flatfs.svg?branch=master)](https://travis-ci.org/ipfs/go-ds-flatfs) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ds-flatfs/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ds-flatfs) | a filesystem-based datastore |
| [`go-ds-measure`](//github.com/ipfs/go-ds-measure) | [![Travis CI](https://travis-ci.org/ipfs/go-ds-measure.svg?branch=master)](https://travis-ci.org/ipfs/go-ds-measure) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ds-measure/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ds-measure) | a metric-collecting database adapter |
| [`go-ds-leveldb`](//github.com/ipfs/go-ds-leveldb) | [![Travis CI](https://travis-ci.org/ipfs/go-ds-leveldb.svg?branch=master)](https://travis-ci.org/ipfs/go-ds-leveldb) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ds-leveldb/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ds-leveldb) | a leveldb based datastore |
| [`go-ds-badger`](//github.com/ipfs/go-ds-badger) | [![Travis CI](https://travis-ci.org/ipfs/go-ds-badger.svg?branch=master)](https://travis-ci.org/ipfs/go-ds-badger) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ds-badger/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ds-badger) | a badgerdb based datastore |
| **Namesys** |
| [`go-ipns`](//github.com/ipfs/go-ipns) | [![Travis CI](https://travis-ci.org/ipfs/go-ipns.svg?branch=master)](https://travis-ci.org/ipfs/go-ipns) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipns/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipns) | IPNS datastructures and validation logic |
| **Repo** |
| [`go-ipfs-config`](//github.com/ipfs/go-ipfs-config) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-config.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-config) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-config/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-config) | go-ipfs config file definitions |
| [`go-fs-lock`](//github.com/ipfs/go-fs-lock) | [![Travis CI](https://travis-ci.org/ipfs/go-fs-lock.svg?branch=master)](https://travis-ci.org/ipfs/go-fs-lock) | [![jenkins](https://ci.ipfs.team/buildStatus/icon?job=ipfs/go-fs-lock/master)](https://ci.ipfs.team/job/ipfs/job/go-fs-lock/job/master/) | [![codecov](https://codecov.io/gh/ipfs/go-fs-lock/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-fs-lock) | lockfile management functions |
| [`fs-repo-migrations`](//github.com/ipfs/fs-repo-migrations) | [![Travis CI](https://travis-ci.org/ipfs/fs-repo-migrations.svg?branch=master)](https://travis-ci.org/ipfs/fs-repo-migrations) | N/A | [![codecov](https://codecov.io/gh/ipfs/fs-repo-migrations/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/fs-repo-migrations) | repo migrations |
| **Blocks** |
| [`go-block-format`](//github.com/ipfs/go-block-format) | [![Travis CI](https://travis-ci.org/ipfs/go-block-format.svg?branch=master)](https://travis-ci.org/ipfs/go-block-format) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-block-format/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-block-format) | block interfaces and implementations |
| [`go-ipfs-blockstore`](//github.com/ipfs/go-ipfs-blockstore) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-blockstore.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-blockstore) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-blockstore/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-blockstore) | blockstore interfaces and implementations |
| [`go-ipfs-blockstore`](//github.com/ipfs/go-ipfs-blockstore) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-blockstore.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-blockstore) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-blockstore/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-blockstore) | blockstore interfaces and implementations |
| **Commands** |
| [`go-ipfs-cmds`](//github.com/ipfs/go-ipfs-cmds) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-cmds.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-cmds) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-cmds/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-cmds) | CLI & HTTP commands library |
| [`go-ipfs-cmdkit`](//github.com/ipfs/go-ipfs-cmdkit) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-cmdkit.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-cmdkit) | [![jenkins](https://ci.ipfs.team/buildStatus/icon?job=ipfs/go-ipfs-cmdkit/master)](https://ci.ipfs.team/job/ipfs/job/go-ipfs-cmdkit/job/master/) | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-cmdkit/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-cmdkit) | helper types for the commands library |
| [`go-ipfs-blockstore`](//github.com/ipfs/go-ipfs-blockstore) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-blockstore.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-blockstore) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-blockstore/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-blockstore) | blockstore interfaces and implementations |
| [`go-ipfs-api`](//github.com/ipfs/go-ipfs-api) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-api.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-api) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-api/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-api) | a shell for the IPFS HTTP API |
| **Metrics & Logging** |
| [`go-metrics-interface`](//github.com/ipfs/go-metrics-interface) | [![Travis CI](https://travis-ci.org/ipfs/go-metrics-interface.svg?branch=master)](https://travis-ci.org/ipfs/go-metrics-interface) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-metrics-interface/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-metrics-interface) | metrics collection interfaces |
| [`go-metrics-prometheus`](//github.com/ipfs/go-metrics-prometheus) | [![Travis CI](https://travis-ci.org/ipfs/go-metrics-prometheus.svg?branch=master)](https://travis-ci.org/ipfs/go-metrics-prometheus) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-metrics-prometheus/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-metrics-prometheus) | prometheus-backed metrics collector |
| [`go-log`](//github.com/ipfs/go-log) | [![Travis CI](https://travis-ci.org/ipfs/go-log.svg?branch=master)](https://travis-ci.org/ipfs/go-log) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-log/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-log) | logging framework |
| **Generics/Utils** |
| [`go-ipfs-routing`](//github.com/ipfs/go-ipfs-routing) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-routing.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-routing) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-routing/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-routing) | routing (content, peer, value) helpers |
| [`go-ipfs-util`](//github.com/ipfs/go-ipfs-util) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-util.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-util) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-util/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-util) | the kitchen sink |
| [`go-ipfs-addr`](//github.com/ipfs/go-ipfs-addr) | [![Travis CI](https://travis-ci.org/ipfs/go-ipfs-addr.svg?branch=master)](https://travis-ci.org/ipfs/go-ipfs-addr) | N/A | [![codecov](https://codecov.io/gh/ipfs/go-ipfs-addr/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/go-ipfs-addr) | utility functions for parsing IPFS multiaddrs |

For brevity, we've omitted go-libp2p and go-ipld packages. These package tables can be found in their respective project's READMEs:

* [go-libp2p](https://github.com/libp2p/go-libp2p#packages)
* [go-ipld](https://github.com/ipld/go-ipld#packages)

## Development

Some places to get you started on the codebase:

- Main file: [./cmd/ipfs/main.go](https://github.com/ipfs/go-ipfs/blob/master/cmd/ipfs/main.go)
- CLI Commands: [./core/commands/](https://github.com/ipfs/go-ipfs/tree/master/core/commands)
- Bitswap (the data trading engine): [go-bitswap](https://github.com/ipfs/go-bitswap)
- libp2p
  - libp2p: https://github.com/libp2p/go-libp2p
  - DHT: https://github.com/libp2p/go-libp2p-kad-dht
  - PubSub: https://github.com/libp2p/go-libp2p-pubsub

### CLI, HTTP-API, Architecture Diagram

![](./docs/cli-http-api-core-diagram.png)

> [Origin](https://github.com/ipfs/pm/pull/678#discussion_r210410924)

Description: Dotted means "likely going away". The "Legacy" parts are thin wrappers around some commands to translate between the new system and the old system. The grayed-out parts on the "daemon" diagram are there to show that the code is all the same, it's just that we turn some pieces on and some pieces off depending on whether we're running on the client or the server.

### Testing

```
make test
```

### Development Dependencies

If you make changes to the protocol buffers, you will need to install the [protoc compiler](https://github.com/google/protobuf).

### Developer Notes

Find more documentation for developers on [docs](./docs)

## Contributing

[![](https://cdn.rawgit.com/jbenet/contribute-ipfs-gif/master/img/contribute.gif)](https://github.com/ipfs/community/blob/master/contributing.md)

We ❤️ all [our contributors](docs/AUTHORS); this project wouldn’t be what it is without you! If you want to help out, please see [CONTRIBUTING.md](CONTRIBUTING.md).

This repository falls under the IPFS [Code of Conduct](https://github.com/ipfs/community/blob/master/code-of-conduct.md).

## License

[MIT](./LICENSE)
