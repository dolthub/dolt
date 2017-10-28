# Plugins

Since 0.4.11 go-ipfs has an experimental plugin system that allows augmenting
the daemons functionality without recompiling.

When an IPFS node is created, it will load plugins from the `$IPFS_PATH/plugins`
directory (by default `~/.ipfs/plugins`).

### Plugin types

#### IPLD
IPLD plugins add support for additional formats to `ipfs dag` and other IPLD
related commands.

### Supported plugins

| Name | Type |
|------|------|
|  git | IPLD |

#### Installation

##### Linux

1. Build included plugins:
```bash
go-ipfs$ make build_plugins
go-ipfs$ ls plugin/plugins/*.so
```

3. Copy desired plugins to `$IPFS_PATH/plugins`
```bash
go-ipfs$ mkdir -p ~/.ipfs/plugins/
go-ipfs$ cp plugin/plugins/git.so ~/.ipfs/plugins/
go-ipfs$ chmod +x ~/.ipfs/plugins/git.so # ensure plugin is executable
```

4. Restart daemon if it is running

##### Other

Go currently only supports plugins on Linux, for other platforms you will need
to compile them into IPFS binary.

1. Uncomment plugin entries in `plugin/loader/preload_list`
2. Build ipfs
```bash
go-ipfs$ make build
```
