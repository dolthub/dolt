**Decentralized Use Case:** [About](about.md)&nbsp; | &nbsp;[Quickstart](quickstart.md)&nbsp; | &nbsp;[Architectures](architectures.md)&nbsp; | &nbsp;[P2P Chat Demo](demo-p2p-chat.md)&nbsp; | &nbsp;[IPFS Chat Demo](demo-ipfs-chat.md)&nbsp; | &nbsp;[Status](status.md)
<br><br>
[![Build Status](http://jenkins3.noms.io/buildStatus/icon?job=NomsMasterBuilder)](http://jenkins3.noms.io/job/NomsMasterBuilder/)
[![codecov](https://codecov.io/gh/attic-labs/noms/branch/master/graph/badge.svg)](https://codecov.io/gh/attic-labs/noms)
[![GoDoc](https://godoc.org/github.com/attic-labs/noms?status.svg)](https://godoc.org/github.com/attic-labs/noms)
[![Slack](http://slack.noms.io/badge.svg)](http://slack.noms.io)

# How to Use Noms in a Decentralized App

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
    easier.  
    
   An example of answers to the above questions is that peers will publish new `(root hash, database URL)` tuples in
    [IPNS](https://github.com/ipfs/examples/tree/master/examples/ipns), pull immediately, and avoid conflicts by ensuring changes are commutative.
    
   As an example of changes that commute consider modeling a stream
    of chat messages. Appending messages from both parties to a list
    is not commutative; the result depends on the order in which
    messages are added to the list. An example of a commutative
    strategy is adding the messages to a `Map` keyed by
    `Struct{sender, ordinal}`: the resulting `Map` is the same no
    matter what order messages are added.

1. Vendor the code into your project. 
1. Set `NOMS_VERSION_NEXT=1` in your environment.
1. Decide which type of storage you'd like to use: memory (convenient for playing around), disk, IPFS, or S3. (If you want to implement a store on top of another type of storage that's possible too; email us or reach out on slack and we can help.)
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
   * **IPFS**: identify an IPFS node by directory. If an IPFS node doesn't exist at that directory, one will be created:
   ```
    cfg := config.NewResolver()
    database, dataset, err := cfg.GetDataset("ipfs:/path/to/chunks::test")  // Dataset name is "test"
   ```
   * **S3**: Follow the [S3 setup instructions](https://github.com/attic-labs/noms/blob/master/go/nbs/NBS-on-AWS.md) then instantiate a database and dataset:
    ```
    sess  := session.Must(session.NewSession(aws.NewConfig().WithRegion("us-west-2")))
    store := nbs.NewAWSStore("dynamo-table", "store-name", "s3-bucket", s3.New(sess), dynamodb.New(sess), 1<<28))
    database := datas.NewDatabase(store)
    dataset := database.GetDataset("aws://dynamo-table:s3-bucket/store-name::test")  // Dataset name is "test"
    ```
1. Implement using the [Go API](../go-tour.md). If you're just playing around you could try something like this (source code can be found in [quickstart.go](https://github.com/attic-labs/noms/blob/master/samples/go/quickstart/quickstart.go)).
    ```
    // Copyright 2017 Attic Labs, Inc. All rights reserved.
    // Licensed under the Apache License, version 2.0:
    // http://www.apache.org/licenses/LICENSE-2.0
    
    package main
    
    import (
        "fmt"
        "os"
    
        "github.com/attic-labs/noms/go/spec"
        "github.com/attic-labs/noms/go/types"
    )
    
    // Usage: quickstart /path/to/store::ds
    func main() {
        sp, err := spec.ForDataset(os.Args[1])
        if err != nil {
            fmt.Fprintf(os.Stderr, "Unable to parse spec: %s, error: %s\n", sp, err)
            os.Exit(1)
        }
        defer sp.Close()
    
        db := sp.GetDatabase()
        if headValue, ok := sp.GetDataset().MaybeHeadValue(); !ok {
            data := types.NewList(sp.GetDatabase(),
                newPerson("Rickon", true),
                newPerson("Bran", true),
                newPerson("Arya", false),
                newPerson("Sansa", false),
            )
    
            fmt.Fprintf(os.Stdout, "data type: %v\n", types.TypeOf(data).Describe())
            _, err = db.CommitValue(sp.GetDataset(), data)
            if err != nil {
                fmt.Fprint(os.Stderr, "Error commiting: %s\n", err)
                os.Exit(1)
            }
        } else {
            // type assertion to convert Head to List
            personList := headValue.(types.List)
            // type assertion to convert List Value to Struct
            personStruct := personList.Get(0).(types.Struct)
            // prints: Rickon
            fmt.Fprintf(os.Stdout, "given: %v\n", personStruct.Get("given"))
        }
    }
    
    func newPerson(givenName string, male bool) types.Struct {
        return types.NewStruct("Person", types.StructData{
            "given": types.String(givenName),
            "male":  types.Bool(male),
        })
    }
    ```
1. You can inspect data that you've committed via the [noms command-line interface](https://github.com/attic-labs/noms/blob/master/doc/cli-tour.md). For example:
  ```
  noms log /path/to/store::ds
  noms show /path/to/store::ds
  ```
  Try Consider creating a [.nomsconfig](https://github.com/attic-labs/noms/blob/master/samples/cli/nomsconfig/README.md) file to save the trouble of writing database specs on the command line.
   * Note that Memory tables won't be inspectable because they exist only in the memory of the process that created them. 
1. Implement pull and merge. The [pull API](../../go/datas/pull.go) is used pull changes from a peer and the [merge API](../../go/merge/) is used to merge changes before commit. There's an [example of merging in the IPFS-based-chat sample
    app](https://github.com/attic-labs/noms/blob/master/samples/go/ipfs-chat/pubsub.go). 
