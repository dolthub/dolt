# multihash tool

The `multihash` tool uses `go-multihash` to hash things much like `shasum`.

Warning: this is a **multihash** tool! Its digests follow the [multihash](https://github.com/jbenet/multihash) format.

### Install

- From Source:
    ```
    go get github.com/multiformats/go-multihash/multihash
    ```
- Precompiled Binaries: https://gobuilder.me/github.com/multiformats/go-multihash/multihash

### Usage

```sh
> multihash -h
usage: ./multihash [options] [FILE]
Print or check multihash checksums.
With no FILE, or when FILE is -, read standard input.

Options:
  -a="sha2-256": one of: sha1, sha2-256, sha2-512, sha3 (shorthand)
  -algorithm="sha2-256": one of: sha1, sha2-256, sha2-512, sha3
  -c="": check checksum matches (shorthand)
  -check="": check checksum matches
  -e="base58": one of: raw, hex, base58, base64 (shorthand)
  -encoding="base58": one of: raw, hex, base58, base64
  -l=-1: checksums length in bits (truncate). -1 is default (shorthand)
  -length=-1: checksums length in bits (truncate). -1 is default
```

### Examples

#### Input

```sh
# from stdin
> multihash < main.go
QmRZxt2b1FVZPNqd8hsiykDL3TdBDeTSPX9Kv46HmX4Gx8

# from file
> ./multihash main.go
QmRZxt2b1FVZPNqd8hsiykDL3TdBDeTSPX9Kv46HmX4Gx8

# from stdin "filename"
> multihash - < main.go
QmRZxt2b1FVZPNqd8hsiykDL3TdBDeTSPX9Kv46HmX4Gx8
```

#### Algorithms

```sh
> multihash -a ?
error: algorithm '?' not one of: sha1, sha2-256, sha2-512, sha3

> multihash -a sha1 < main.go
5drkbcqJUo6fZVvcZJeVEVWAgndvLm

> multihash -a sha2-256 < main.go
QmcK3s36goo9v2HYcfTrDKKwxaxmJJ59etodQQFYsL5T5N

> multihash -a sha2-512 < main.go
8VuDcW4CooyPQA8Cc4eYpwjhyDJZqu5m5ZMDFzWULYsVS8d119JaGeNWsZbZ2ZG2kPtbrMx31MidokCigaD65yUPAs

> multihash -a sha3 < main.go
8tWDCTfAX24DYmzNixTj2ARJkqwRG736VHx5aJppmqRjhW9QT1EuTgKUmu9Pmunzq292jzPKxb2VxSsTXmjFY1HD3B
```

#### Encodings

```sh
> multihash -e raw < main.go
 Ϛ�����I�5  S��WG>���_��]g�����u

> multihash -e hex < main.go
1220cf9aa2b8a38b9b49d135095390059a57473e97aceb5fcae25d67a8b6feb58275

> multihash -e base64 < main.go
EiDPmqK4o4ubSdE1CVOQBZpXRz6XrOtfyuJdZ6i2/rWCdQ==

> multihash -e base58 < main.go
Qmf1QjEXDmqBm7RqHKqFGNUyhzUjnX7cmgKMrGzzPceZDQ
```

#### Digest Length

```sh
# we're outputing hex (good byte alignment) to show the codes changing
# notice the multihash code (first 2 chars) differs!
> multihash -e hex -a sha2-256 -l 256 < main.go
1220cf9aa2b8a38b9b49d135095390059a57473e97aceb5fcae25d67a8b6feb58275
> multihash -e hex -a sha2-512 -l 256 < main.go
132047a4b6c629f5545f529b0ff461dc09119969f3593186277a1cc7a8ea3560a6f1
> multihash -e hex -a sha3 -l 256 < main.go
14206b9222a1a47939e665261bd2b5573e55e7988675223adde73c1011066ad66335

# notice the multihash length (next 2 chars) differs!
> multihash -e hex -a sha2-256 -l 256 < main.go
1220cf9aa2b8a38b9b49d135095390059a57473e97aceb5fcae25d67a8b6feb58275
> multihash -e hex -a sha2-256 -l 200 < main.go
1219cf9aa2b8a38b9b49d135095390059a57473e97aceb5fcae25d
```

#### Verify Checksum

```sh
> multihash -c QmRZxt2b1FVZPNqd8hsiykDL3TdBDeTSPX9Kv46HmX4Gx8 < main.go
OK checksums match (-q for no output)

> multihash -c QmcKaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa < main.go
error: computed checksum did not match (-q for no output)

# works with other arguments too
> multihash -e hex -l 128 -c "12102ffc284a1e82bf51e567c75b2ae6edb9" < main.go
OK checksums match (-q for no output)
```
