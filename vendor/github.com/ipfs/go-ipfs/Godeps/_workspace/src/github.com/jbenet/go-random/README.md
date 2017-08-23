# go-random outputs randomness

This is a unix util that outputs randomness.
It is a thin wrapper around `crypto/rand`.
It aims to be portable (though it may not yet be).

### Install

```sh
go install github.com/jbenet/go-random/random
```

(The extra /random is there because go get is stupidly too proscriptive about
package/repository names and I don't yet know how to change the default binary
output name)

### Usage:

```
> random
Usage: random <int>
Print <int> random bytes (from Go's crypto/rand)
> random 6
2q���#
```

### License

MIT
