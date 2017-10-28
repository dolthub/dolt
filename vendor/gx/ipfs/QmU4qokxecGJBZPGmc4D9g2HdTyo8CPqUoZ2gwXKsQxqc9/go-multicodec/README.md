# go-multicodec

[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![](https://img.shields.io/badge/project-multiformats-blue.svg?style=flat-square)](https://github.com/multiformats/multiformats)
[![](https://img.shields.io/badge/freenode-%23ipfs-blue.svg?style=flat-square)](https://webchat.freenode.net/?channels=%23ipfs)
[![](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![Travis CI](https://img.shields.io/travis/multiformats/go-multicodec.svg?style=flat-square&branch=master)](https://travis-ci.org/multiformats/go-multicodec)
[![codecov.io](https://img.shields.io/codecov/c/github/multiformats/go-multicodec.svg?style=flat-square&branch=master)](https://codecov.io/github/multiformats/go-multicodec?branch=master)
[![GoDoc](https://godoc.org/github.com/multiformats/go-multicodec?status.svg)](https://godoc.org/github.com/multiformats/go-multicodec)

> [multicodec](https://github.com/multiformats/multicodec) implementation in Go.

### Supported codecs

- `/protobuf`
- `/cbor`
- `/json`

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Maintainers](#maintainers)
- [Contribute](#contribute)
- [License](#license)

## Install

```sh
go get github.com/multiformats/go-multicodec
```

## Usage

Look at the Godocs:

- https://godoc.org/github.com/multiformats/go-multicodec

```go
import (
  "os"
  "io"

  cbor "github.com/multiformats/go-multicodec/cbor"
  json "github.com/multiformats/go-multicodec/json"
)

func main() {
  dec := cbor.Multicodec().Decoder(os.Stdin)
  enc := json.Multicodec(false).Encoder(os.Stdout)

  for {
    var item interface{}

    if err := dec.Decode(&item); err == io.EOF {
      break
    } else if err != nil {
      panic(err)
    }

    if err := enc.Encode(&item); err != nil {
      panic(err)
    }
  }
}
```

## Maintainers

Captain: [@jbenet](https://github.com/jbenet).

## Contribute

Contributions welcome. Please check out [the issues](https://github.com/multiformats/go-multicodec/issues).

Check out our [contributing document](https://github.com/multiformats/multiformats/blob/master/contributing.md) for more information on how we work, and about contributing in general. Please be aware that all interactions related to multiformats are subject to the IPFS [Code of Conduct](https://github.com/ipfs/community/blob/master/code-of-conduct.md).

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

[MIT](LICENSE) © 2014 Juan Batiz-Benet
