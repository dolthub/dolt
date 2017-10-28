# hang-fds - stress test your network listeners

This module will open a bunch of connections to a given [multiaddr](https://github.com/multiformats/multiaddr), and hang them. It is meant to stress test your network listeners.

### Install

```sh
go get github.com/jbenet/hang-fds
```

### Usage

```sh
# open 2048 connections to /ip4/127.0.0.1/tcp/8080
hang-fds 2048 /ip4/127.0.0.1/tcp/8080
```

There is a test listener in the [test_server](./test_server) directory.

```sh
# accept any connections and hang them
> test-server /ip4/0.0.0.0/tcp/1234
```

### License

MIT
