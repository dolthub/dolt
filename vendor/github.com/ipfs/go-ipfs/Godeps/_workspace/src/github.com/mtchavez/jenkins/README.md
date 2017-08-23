Jenkins
=================

Golang Jenkins hash

[![Build Status](https://travis-ci.org/mtchavez/go-jenkins-hashes.png?branch=master)](https://travis-ci.org/mtchavez/go-jenkins-hashes)

## Install

`go get -u github.com/mtchavez/jenkins`

## Usage

Jenkins follows the [Hash32](http://golang.org/pkg/hash/#Hash32) interface from the Go standard library

```go
// Create a new hash
jenkhash := New()

// Write a string of bytes to hash
key := []byte("my-random-key")
length, err := jenkhash(key)

// Get uint32 sum of hash
sum := jenkhash.Sum32()

// Sum hash with byte string
sumbytes := jenkhash.Sum(key)
```

## Testing

Uses [Ginkgo](http://onsi.github.io/ginkgo/) for testing.

Run via `make test` which will run `go test -cover`

## Documentation

Docs on [godoc](http://godoc.org/github.com/mtchavez/jenkins)

## License

Written by Chavez

Released under the MIT License: http://www.opensource.org/licenses/mit-license.php
