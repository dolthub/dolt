# How to Use Noms in a Decentralized App

If you’d like to use noms in your project we’d love to hear from you:
drop us an email [noms@attic.io](mailto:noms@attic.io) or send us a
message in slack ([slack.noms.io](http://slack.noms.io)).

The steps you’ll need to take are:

1. Vendor the code into your project. 
1. Configure the underlying blockstore (S3, memory, disk, IPFS) and add
  the required boilerplate to your app:
```
<...>
```
1. Decide how you’ll model your problem using noms’ datatypes: boolean,
  number, string, blob, map, list, set, structs, ref, and
  union. (Note: if you are interested in using CRDTs as an alternative
  to classic datatypes please let us know.)
1. Consider...
  * how peers will discover each other
  * how and when they will pull changes, and 
  * what potential there is for conflicting changes. Consider modeling
    your problem so that changes commute[*] in order to make merging
    easier. For example peers could publish new `(root hash, database
    URL)` tuples in
    [IPNS](https://github.com/ipfs/examples/tree/master/examples/ipns)
    and pull immediately. You can check out the [pull
    API](../../go/datas/pull.go) and [merge API](../../go/merge/) can
    see an example of merging in the [IPFS-based-chat sample
    app](../../samples/go/ifs-chat/pubsub.go).  Implement using the
    [Go API](../go-tour.md).


[*] As an example of changes that commute consider modeling a stream
    of chat messages. Appending messages from both parties to a list
    is not commutative; the result depends on the order in which
    messages are added to the list. An example of a commutative
    strategy is adding the messages to a `Map` keyed by
    `Struct{sender, ordinal}`: the resulting `Map` is the same no
    matter what order messages are added.
