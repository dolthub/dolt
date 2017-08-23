# IPFS API Implementation Doc

This short document aims to give a quick guide to anyone implementing API bindings for IPFS implementations-- in particular go-ipfs.

Sections:
- IPFS Types
- API Transports
- API Commands
- Implementing bindings for the HTTP API

## IPFS Types

IPFS uses a set of type value that is useful to enumerate up front:

- `<ipfs-path>` is unix-style path, beginning with `/ipfs/<hash>/...` or `/ipns/<hash>/...` or `/ipns/<domain>/...`.
- `<hash>` is a base58 encoded [multihash](https://github.com/jbenet/multihash) (there are [many implementations](https://github.com/jbenet/multihash#implementations)). Usually the hash of an IPFS object (or merkle dag node).

A note on streams: IPFS is a streaming protocol. Everything about it can be streamed. When importing files, API requests should aim to stream the data in, and handle back-pressure correctly, so that the IPFS node can handle it sequentially without too much memory pressure. (If using HTTP, this is typically handled for you by writes to the request body blocking.)

## API Transports

Like with everything else, IPFS aims to be flexible regarding the API transports. Currently, the [go-ipfs](https://github.com/ipfs/go-ipfs) implementation supports both an in-process API and an HTTP api. More can be added easily, by mapping the API functions over a transport. (This is similar to how gRPC is also _mapped on top of transports_, like HTTP).

Mapping to a transport involves leveraging the transport's features to express function calls. For example:

### CLI API Transport

In the commandline, IPFS uses a traditional flag and arg-based mapping, where:
- the first arguments selects the command, as in git - e.g. `ipfs object get`
- the flags specify options - e.g. `--enc=protobuf -q`
- the rest are positional arguments - e.g. `ipfs object patch <hash1> add-link foo <hash2>`
- files are specified by filename, or through stdin

(NOTE: When go-ipfs runs the daemon, the CLI API is actually converted to HTTP calls. otherwise, they execute in the same process)

### HTTP API Transport

In HTTP, our API layering uses a REST-like mapping, where:
- the URL path selects the command - e.g `/object/get`
- the URL query string implements option arguments - e.g. `&enc=protobuf&q=true`
- the URL query also implements positional arguments - e.g. `&arg=<hash1>&arg=add-link&arg=foo&arg=<hash2>`
- the request body streams file data - reads files or stdin
  - multiple streams are muxed with multipart (todo: add tar stream support)


## API Commands

There is a "standard IPFS API" with a set of commands, which we are documenting clearly soon. But this is not yet extracted into its own document. Perhaps -- as part of this API Bindings effort -- we can document it all. It is currently defined as "all the commands exposed by the go-ipfs implementation". You can see [a listing here](https://github.com/ipfs/go-ipfs/blob/916f987de2c35db71815b54bbb9a0a71df829838/core/commands/root.go#L82-L111), or by running `ipfs commands` locally. **The good news is: we should be able to easily write a program that outputs a markdown API specification!**

(NOTE: The go-ipfs [commands library](https://github.com/ipfs/go-ipfs/tree/916f987de2c35db71815b54bbb9a0a71df829838/commands) also makes sure to keep the CLI and the HTTP API exactly in sync.)

## Implementing bindings for the HTTP API

As mentioned above, the API commands map to HTTP with:
- the URL path selects the command - e.g `/object/get`
- the URL query string implements option arguments - e.g. `&enc=protobuf&q=true`
- the URL query also implements positional arguments - e.g. `&arg=<hash1>&arg=add-link&arg=foo&arg=<hash2>`
- the request body streams file data - reads files or stdin
  - multiple streams are muxed with multipart (todo: add tar stream support)

To date, we have two different HTTP API clients:

- [node-ipfs-api](https://github.com/ipfs/node-ipfs-api) - simple javascript wrapper -- best to look at
- [go-ipfs/commands/http](https://github.com/ipfs/go-ipfs/tree/916f987de2c35db71815b54bbb9a0a71df829838/commands/http) - generalized transport based on the [command definitions](https://github.com/ipfs/go-ipfs/tree/916f987de2c35db71815b54bbb9a0a71df829838/core/commands)

The Go implementation is good to answer harder questions, like how is multipart handled, or what headers should be set in edge conditions. But the javascript implementation is very concise, and easy to follow.

### Anatomy of node-ipfs-api

Currently, node-ipfs-api has three main files
- [src/index.js](https://github.com/ipfs/node-ipfs-api/blob/66d1462bd02181d46e8baf4cd9d476b213426ad8/src/index.js) defines the functions clients of the API module will use. uses `RequestAPI`, and translates function call parameters to the API almost directly.
- [src/get-files-stream.js](https://github.com/ipfs/node-ipfs-api/blob/66d1462bd02181d46e8baf4cd9d476b213426ad8/src/get-files-stream.js) implements the hardest part: file streaming. This one uses multipart.
- [src/request-api.js](https://github.com/ipfs/node-ipfs-api/blob/66d1462bd02181d46e8baf4cd9d476b213426ad8/src/request-api.js) generic function call to perform the actual HTTP requests

### Note on multipart + inspecting requests

Despite all the generalization spoken about above, the IPFS API is actually very simple. You can inspect all the requests made with `nc` and the `--api` option (as of [this PR](https://github.com/ipfs/go-ipfs/pull/1598), or `0.3.8`):

```
> nc -l 5002 &
> ipfs --api /ip4/127.0.0.1/tcp/5002 swarm addrs local --enc=json
POST /api/v0/version?enc=json&stream-channels=true HTTP/1.1
Host: 127.0.0.1:5002
User-Agent: /go-ipfs/0.3.8/
Content-Length: 0
Content-Type: application/octet-stream
Accept-Encoding: gzip


```

The only hard part is getting the file streaming right. It is (now) fairly easy to stream files to go-ipfs using multipart. Basically, we end up with HTTP requests like this:

```
> nc -l 5002 &
> ipfs --api /ip4/127.0.0.1/tcp/5002 add -r ~/demo/basic/test
POST /api/v0/add?encoding=json&progress=true&r=true&stream-channels=true HTTP/1.1
Host: 127.0.0.1:5002
User-Agent: /go-ipfs/0.3.8/
Transfer-Encoding: chunked
Content-Disposition: form-data: name="files"
Content-Type: multipart/form-data; boundary=2186ef15d8f2c4f100af72d6d345afe36a4d17ef11264ec5b8ec4436447f
Accept-Encoding: gzip

1
-
e5
-2186ef15d8f2c4f100af72d6d345afe36a4d17ef11264ec5b8ec4436447f
Content-Disposition: form-data; name="file"; filename="test"
Content-Type: multipart/mixed; boundary=acdb172fe12f25e8ffae9981ce6f4580abdefb0cae3ceebe464d802866be


9c
--acdb172fe12f25e8ffae9981ce6f4580abdefb0cae3ceebe464d802866be
Content-Disposition: file; filename="test%2Fbar"
Content-Type: application/octet-stream


4
bar

dc

--acdb172fe12f25e8ffae9981ce6f4580abdefb0cae3ceebe464d802866be
Content-Disposition: file; filename="test%2Fbaz"
Content-Type: multipart/mixed; boundary=2799ac77a72ef7b8a0281945806b9f9a28f7681145aa8e91b052d599b2dd


a0
--2799ac77a72ef7b8a0281945806b9f9a28f7681145aa8e91b052d599b2dd
Content-Type: application/octet-stream
Content-Disposition: file; filename="test%2Fbaz%2Fb"


4
bar

a2

--2799ac77a72ef7b8a0281945806b9f9a28f7681145aa8e91b052d599b2dd
Content-Disposition: file; filename="test%2Fbaz%2Ff"
Content-Type: application/octet-stream


4
foo

44

--2799ac77a72ef7b8a0281945806b9f9a28f7681145aa8e91b052d599b2dd--

9e

--acdb172fe12f25e8ffae9981ce6f4580abdefb0cae3ceebe464d802866be
Content-Disposition: file; filename="test%2Ffoo"
Content-Type: application/octet-stream


4
foo

44

--acdb172fe12f25e8ffae9981ce6f4580abdefb0cae3ceebe464d802866be--

44

--2186ef15d8f2c4f100af72d6d345afe36a4d17ef11264ec5b8ec4436447f--

0

```

Which produces: http://gateway.ipfs.io/ipfs/QmNtpA5TBNqHrKf3cLQ1AiUKXiE4JmUodbG5gXrajg8wdv

