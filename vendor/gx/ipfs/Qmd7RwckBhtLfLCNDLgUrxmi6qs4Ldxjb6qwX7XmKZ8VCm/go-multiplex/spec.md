# multiplex spec

go-multiplex is an implementation of the multiplexing protocol used by
[multiplex](https://github.com/maxogden/multiplex). This document will attempt
to define a specification for the wire protocol and algorithm used in both
implementations. 

Multiplex is a very simple protocol that does not provide many features offered
by other stream multiplexers. Notably, multiplex does not provide backpressure
at the protocol level, or support half closed streams.

## Message format
Every communication in multiplex consists of a header, and a length prefixed data segment.
The header is an unsigned base128 varint, as defined in the [protocol buffers spec](https://developers.google.com/protocol-buffers/docs/encoding#varints). The lower three bits are the message flags, and the rest of the bits (shifted down by three bits) are the stream ID this message pertains to:

```
header = readUvarint()
flag = head & 0x07
id = flag >> 3
```

### Flag Values
```
----------------------
| NewStream      | 0 |
| Receiver       | 1 |
| Initiator      | 2 |
| CloseReceiver  | 3 |
| CloseInitiator | 4 |
| ResetReceiver  | 5 |
| ResetInitiator | 6 |
----------------------
```

The data segment is length prefixed by another unsigned varint. This results in one message looking like:

```
-------------------------------------------------
| header  | length  | 	data       |
| uvarint | uvarint | 'length' bytes |
-------------------------------------------------
```



## Protocol
Multiplex operates over a reliable ordered pipe between two peers, such as a
TCP socket, or a unix pipe. One peer is designated the 'initiator' (or the
dialer) and the other is the 'receiver'. The initiator does not necessarily
send the first packet, this distinction is just made to make the allocation of
stream ID's unambiguous.

### Opening a new stream
To open a new stream, first allocate the next stream ID (see stream ID
allocation below), then send a message with the flag set to `NewStream`, the id
set to the newly allocated stream ID, and the data of the message set to the
name of the stream. Stream names are purely for interfaces and are not
otherwise considered by the protocol. An empty string may also be used for the
stream name, and they may also be repeated (using the same stream name for
every stream is valid).
Reusing a stream ID after closing a stream may result in undefined behaviour.

### Writing to a stream
To write data to a stream, the header must contain the ID of a previously
opened stream, and the flag will be set to either Receiver (`1`) or Initiator
(`2`) depending on which peer opened the stream (defined in the 'Protocol'
section). The data field should contain simply the data you wish to write to
the stream, limited to a maximum size agreed upon out of band (For reference,
the go-multiplex implementation sets this to 1MB).

### Closing a stream
To close a stream, send a message with a zero length body with the id of the
stream you want to close in the header, as well as the flag being set to Close
(`4`). Writing to a stream after it has been closed should result in an error.
A stream should be considered closed as soon as you send the close message or
receive the close message for a given stream. Reading from a closed stream
should return any data received for the stream before it was closed (Allowing
you to open a stream, write data, and close it immediately without worrying
whether or not the remote peer actually got the data).

## Implementation notes
Since backpressure is not provided at a protocol level, care must be taken when
implementing multiplex to avoid head of line blocking, where reading from one
stream too slowly can cause other stream reads to block. 
