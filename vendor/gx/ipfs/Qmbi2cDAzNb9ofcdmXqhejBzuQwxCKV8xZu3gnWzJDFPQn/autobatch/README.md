# autobatch

Autobatch is an implementation of
[go-datastore](https://github.com/ipfs/go-datastore) that automatically batches
together writes by holding puts in memory until a certain threshold is met.
This can improve disk performance at the cost of memory in certain situations.

## Usage

Simply wrap your existing datastore in an autobatching layer like so:

```go
bds := NewAutoBatching(basedstore, 128)
```

And make all future calls to the autobatching object.

## License
MIT
