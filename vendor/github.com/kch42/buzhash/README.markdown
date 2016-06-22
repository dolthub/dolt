# buzhash

Package buzhash implements a buzhash algorithm using this defintion <http://en.wikipedia.org/wiki/Rolling_hash#Cyclic_polynomial>.

## Rolling hash

Buzhash is a rolling hash function, that means that the current hash sum is the sum of the last n consumed bytes.

Example:

* Message 1: <code>This is a stupid example text to demo<strong>nstrate buzhash.</strong></code>
* Message 2: <code>Another text to demo<strong>nstrate buzhash.</strong></code>

When hashing both messages with a buzhasher with n=16, both messages will have the same hash sum, since the last 16 characters (`nstrate buzhash.`) are equal.

This can be useful, when searching for a data fragment in large files, without knowing the fragment (only its hash). This is used in binary diff tools, such as `rdiff` (although they use a different rolling hash function).

## Installation

`go get github.com/kch42/buzhash`

## Documentation

Either install the package and use a local godoc server or use [godoc.org](http://godoc.org/github.com/kch42/buzhash)
