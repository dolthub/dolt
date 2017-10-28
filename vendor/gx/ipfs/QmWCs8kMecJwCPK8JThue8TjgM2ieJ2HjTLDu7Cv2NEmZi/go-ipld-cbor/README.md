go-ipld-cbor
==================

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](http://libp2p.io/)
[![](https://img.shields.io/badge/freenode-%23ipfs-blue.svg?style=flat-square)](http://webchat.freenode.net/?channels=%23ipfs)
[![Coverage Status](https://coveralls.io/repos/github/libp2p/js-libp2p-floodsub/badge.svg?branch=master)](https://coveralls.io/github/libp2p/js-libp2p-floodsub?branch=master)
[![Travis CI](https://travis-ci.org/libp2p/js-libp2p-floodsub.svg?branch=master)](https://travis-ci.org/libp2p/js-libp2p-floodsub)

> An implementation of a cbor encoded merkledag object.


## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [API](#api)
- [Contribute](#contribute)
- [License](#license)

## Install

```sh
make install
```

## Usage

TODO: Right now this package isn't the easiest to use, it will be getting better rapidly, soon.
```go
// Make an object
obj := map[interface{}]interface{}{
	"foo": "bar",
	"baz": &Link{
		Target: myCid,
	},
}

// Parse it into an ipldcbor node
nd, err := WrapMap(obj)

fmt.Println(nd.Links())

```

## Contribute

PRs are welcome!

Small note: If editing the Readme, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

MIT © Jeromy Johnson
