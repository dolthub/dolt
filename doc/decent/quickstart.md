[Home](../../README.md) » [Use Cases](../../README.md#use-cases) » **Decentralized** »

[About](about.md)&nbsp; | &nbsp;**Quickstart**&nbsp; | &nbsp;[Architectures](architectures.md)&nbsp; | &nbsp;[P2P Chat Demo](demo-p2p-chat.md)&nbsp; | &nbsp;[IPFS Chat Demo](demo-ipfs-chat.md)
<br><br>
# How to Use Noms in a Decentralized App

If you’d like to use noms in your project we’d love to hear from you:
drop us an email ([noms@attic.io](mailto:noms@attic.io)) or send us a
message in slack ([slack.noms.io](http://slack.noms.io)).

The steps you’ll need to take are:

1. Decide how you’ll model your problem using noms’ datatypes: boolean,
  number, string, blob, map, list, set, structs, ref, and
  union. (Note: if you are interested in using CRDTs as an alternative
  to classic datatypes please let us know.)
2. Consider...
    * How peers will discover each other
    * How peers will notify each other of changes
    * How and when they will pull changes, and 
    * What potential there is for conflicting changes. Consider modeling
    your problem so that changes commute in order to make merging
    easier.  

   In our [p2p sample](https://github.com/attic-labs/noms/blob/master/doc/decent/demo-p2p-chat.md) application, all peers periodically broadcast their HEAD on a known channel using [IPFS pubsub](https://ipfs.io/blog/25-pubsub/), pull each others' changes immediately, and avoid conflicts by using operations that can be resolved with Noms' built in merge policies.
   
   This is basically the simplest possible approach, but lots of options are possible. For example, an alternate approach for discoverability could be to keep a registry of all participating nodes in a blockchain (e.g., by storing them in an Ethereum smart contract). One could store either the current HEAD of each node (updated whenever the node changes state), or just an IPNS name that the node is writing to.
    
   As an example of changes that commute consider modeling a stream
    of chat messages. Appending messages from both parties to a list
    is not commutative; the result depends on the order in which
    messages are added to the list. An example of a commutative
    strategy is adding the messages to a `Map` keyed by
    `Struct{sender, ordinal}`: the resulting `Map` is the same no
    matter what order messages are added.

3. Vendor the code into your project. 
4. Set `NOMS_VERSION_NEXT=1` in your environment.
5. Decide which type of storage you'd like to use: memory (convenient for playing around), disk, IPFS, or S3. (If you want to implement a store on top of another type of storage that's possible too; email us or reach out on slack and we can help.)
6. Set up and instantiate a database for your storage. Generally, you use the spec package to parse a [dataset spec](https://github.com/attic-labs/noms/blob/master/doc/spelling.md) like `mem::mydataset` which you can then ask for  [`Database`](https://github.com/attic-labs/noms/blob/master/go/datas/database.go) and [`Dataset`](https://github.com/attic-labs/noms/blob/master/go/datas/dataset.go).
   * **Memory**: no setup required, just instantiate it:

```go
sp := spec.ForDataset("mem::test") // Dataset name is "test"
```
   
   * **Disk**: identify a directory for storage, say `/path/to/chunks`, and then instantiate:
   
```go
sp := spec.ForDataset("/path/to/chunks::test")  // Dataset name is "test"
```
   
   * **IPFS**: identify an IPFS node by directory. If an IPFS node doesn't exist at that directory, one will be created:

```go
sp := spec.ForDataset("ipfs:/path/to/ipfs_repo::test")  // Dataset name is "test"
```

   * **S3**: Follow the [S3 setup instructions](https://github.com/attic-labs/noms/blob/master/go/nbs/NBS-on-AWS.md) then instantiate a database and dataset:

```go
sess  := session.Must(session.NewSession(aws.NewConfig().WithRegion("us-west-2")))
store := nbs.NewAWSStore("dynamo-table", "store-name", "s3-bucket", s3.New(sess), dynamodb.New(sess), 1<<28))
database := datas.NewDatabase(store)
dataset := database.GetDataset("aws://dynamo-table:s3-bucket/store-name::test")  // Dataset name is "test"
```

7. Implement using the [Go API](https://github.com/attic-labs/noms/blob/master/doc/go-tour.md). If you're just playing around you could try something like this:

```go
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

8. You can inspect data that you've committed via the [noms command-line interface](https://github.com/attic-labs/noms/blob/master/doc/cli-tour.md). For example:

```shell
noms log /path/to/store::ds
noms show /path/to/store::ds
```

> Note that Memory tables won't be inspectable because they exist only in the memory of the process that created them. 

9. Implement pull and merge. The [pull API](../../go/datas/pull.go) is used pull changes from a peer and the [merge API](../../go/merge/) is used to merge changes before commit. There's an [example of merging in the IPFS-based-chat sample
    app](https://github.com/attic-labs/noms/blob/master/samples/go/ipfs-chat/pubsub.go). 
