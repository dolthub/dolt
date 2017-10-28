# Protocol Multiplexing using multicodecs with libp2p

This examples shows how to use multicodecs (i.e. json) to encode and transmit information between LibP2P hosts using LibP2P Streams.

Multicodecs present a common interface, making it very easy to swap the codec implementation if needed.

This example expects that you area already familiar with the [echo example](https://github.com/libp2p/go-libp2p/tree/master/examples/echo).

## Build

From `go-libp2p` base folder:

```
> make deps-protocol-muxing
> go build -o multicodecs ./examples/protocol-multiplexing-with-multicodecs
```

## Usage

```
> ./multicodecs

```

## Details

The example creates two LibP2P Hosts. Host1 opens a stream to Host2. Host2 has an `StreamHandler` to deal with the incoming stream. This is covered in the `echo` example.

Both hosts simulate a conversation. But rather than sending raw messages on the stream, each message in the conversation is encoded under a `json` object (using the `json` multicodec). For example:

```
{
  "Msg": "This is the message",
  "Index": 3,
  "HangUp": false
}
```

The stream lasts until one of the sides closes it when the HangUp field is `true`.
