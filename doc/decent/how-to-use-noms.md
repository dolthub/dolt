# How to Use Noms in a Decentralized App WORK IN PROGRESS

If you’d like to use noms in your project we’d love to hear from you:
drop us an email ([noms@attic.io](mailto:noms@attic.io)) or send us a
message in slack ([slack.noms.io](http://slack.noms.io)).

The steps you’ll need to take are:

1. Decide how you’ll model your problem using noms’ datatypes: boolean,
  number, string, blob, map, list, set, structs, ref, and
  union. (Note: if you are interested in using CRDTs as an alternative
  to classic datatypes please let us know.)
1. Consider...
    * how peers will discover each other
    * how and when they will pull changes, and 
    * what potential there is for conflicting changes. Consider modeling
    your problem so that changes commute in order to make merging
    easier.[*]
    
    For example peers could publish new `(root hash, database
    URL)` tuples in
    [IPNS](https://github.com/ipfs/examples/tree/master/examples/ipns)
    and pull immediately. 
1. Vendor the code into your project. 
1. TODO add env and nomsconfig stuff here?
1. Decide which type of storage you'd like to use: memory (convenient for playing around), disk, IPFS, or S3. (If you'd to implement a store on top of another type of storage that's possible too; email us or reach out on slack and we can help.)
1. Set up and instantiate a database for your storage. Generally, you give a [dataset spec](https://github.com/attic-labs/noms/blob/master/doc/spelling.md) like `mem::mydataset` to a [`config.Resolver`](https://github.com/attic-labs/noms/blob/master/go/config/resolver.go) which gives you a handle to the [`Database`](https://github.com/attic-labs/noms/blob/master/go/datas/database.go) and [`Dataset`](https://github.com/attic-labs/noms/blob/master/go/datas/dataset.go).
   * **Memory**: no setup required, just instantiate it:
   ```
    cfg := config.NewResolver()
    database, dataset, err := cfg.GetDataset("mem::test")  // Dataset name is "test"
   ```
   * **Disk**: identify a directory for storage, say `/path/to/chunks`, and then instantiate:
   ```
    cfg := config.NewResolver()
    database, dataset, err := cfg.GetDataset("/path/to/chunks::test")  // Dataset name is "test"
   ```
   * **IPFS**: ??? setup
   ```
    ????
   ```
   * **S3**: Follow the [S3 setup instructions](https://github.com/attic-labs/noms/blob/master/go/nbs/NBS-on-AWS.md) then instantiate a database and dataset:
    ```
    sess  := session.Must(session.NewSession(aws.NewConfig().WithRegion("us-west-2")))
    store := nbs.NewAWSStore("dynamo-table", "store-name", "s3-bucket", s3.New(sess), dynamodb.New(sess), 1<<28))
    database := datas.NewDatabase(store)
    dataset := database.GetDataset("aws://dynamo-table:s3-bucket/store-name::test")  // Dataset name is "test"
    ```
...TODO
1. Implement using the [Go API](../go-tour.md).
1. Implement pull and merge. The [pull API](../../go/datas/pull.go) is used pull changes from a peer and the [merge API](../../go/merge/) is used to merge changes before commit. There's an [example of merging in the IPFS-based-chat sample
    app](https://github.com/attic-labs/noms/blob/master/samples/go/ipfs-chat/pubsub.go). 


[*] *As an example of changes that commute consider modeling a stream
    of chat messages. Appending messages from both parties to a list
    is not commutative; the result depends on the order in which
    messages are added to the list. An example of a commutative
    strategy is adding the messages to a `Map` keyed by
    `Struct{sender, ordinal}`: the resulting `Map` is the same no
    matter what order messages are added.*
