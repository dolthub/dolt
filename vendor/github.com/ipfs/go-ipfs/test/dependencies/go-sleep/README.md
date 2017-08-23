# go-sleep sleeps for some duration

This unix tool is a thin wrapper around `time.Sleep()`.
It aims to provide a portable way to sleep for an amount of time that
need not to be a number of seconds.

See https://godoc.org/time#ParseDuration for how the duration can be
specified.

### Install

```sh
go install github.com/chriscool/go-sleep
```

### Usage:

```
> go-sleep
Usage: go-sleep <duration>
Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
See https://godoc.org/time#ParseDuration for more.
> time go-sleep 100ms

real    0m0.104s
user    0m0.000s
sys     0m0.007s
```

### License

MIT
