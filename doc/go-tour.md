# A Short Tour of Noms for Go

This is a short introduction to using Noms from Go. It should only take a few minutes if you have some familiarity with Go.

During the tour, you can refer to the complete [Go SDK Reference](https://godoc.org/github.com/attic-labs/noms) for more information on anything you see.



## Requirements

* [Noms command-line tools](https://github.com/attic-labs/noms#setup)
* [Go v1.6+](https://golang.org/dl/)
* Ensure your [$GOPATH](https://github.com/golang/go/wiki/GOPATH) is configured

## Start a Local Database

Let's create a local database to play with:

```sh
> noms serve /tmp/noms-go-tour
```

## [Database](https://github.com/attic-labs/noms/blob/master/go/datas/database.go)
Leave the server running, and in a separate terminal:

```sh
> mkdir noms-tour
> cd noms-tour
```

Then use your favorite editor so that we can start to play with code. To get started with Noms, first create a Database:

```go
package main

import (
  "fmt"
  "os"

  "github.com/attic-labs/noms/go/spec"
)

func main() {
  sp, err := spec.ForDatabase("http://localhost:8000")
  if err != nil {
    fmt.Fprintf(os.Stderr, "Could not access database: %s\n", err)
    return
  }
  defer sp.Close()
}
```

Now let's run it:

```sh
> go run noms-tour.go
```

If you did not leave the server running you would see output of ```Could not access database``` here, otherwise your program should exit cleanly.

See [Spelling in Noms](spelling.md) for more information on database spec strings.


## [Dataset](https://github.com/attic-labs/noms/blob/master/go/dataset/dataset.go)

Datasets are the main interface you'll use to work with Noms. Let's update our example to use a Dataset spec string:

```go
package main

import (
  "fmt"
  "os"

  "github.com/attic-labs/noms/go/spec"
)

func main() {
  sp, err := spec.ForDataset("http://localhost:8000::people")
  if err != nil {
    fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
    return
  }
  defer sp.Close()

  if _, ok := sp.GetDataset().MaybeHeadValue(); !ok {
    fmt.Fprintf(os.Stdout, "head is empty\n")
  }
}
```

Now let's run it:

```sh
> go run noms-tour.go
head is empty
```

Since the dataset does not yet have any values you see ```head is empty```. Let's add some data to make it more interesting:

```go
package main

import (
  "fmt"
  "os"

  "github.com/attic-labs/noms/go/spec"
  "github.com/attic-labs/noms/go/types"
)

func newPerson(givenName string, male bool) types.Struct {
  return types.NewStruct("Person", types.StructData{
    "given": types.String(givenName),
    "male":  types.Bool(male),
  })
}

func main() {
  sp, err := spec.ForDataset("http://localhost:8000::people")
  if err != nil {
    fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
    return
  }
  defer sp.Close()

  data := types.NewList(
    newPerson("Rickon", true),
    newPerson("Bran", true),
    newPerson("Arya", false),
    newPerson("Sansa", false),
  )

  fmt.Fprintf(os.Stdout, "data type: %v\n", data.Type().Describe())

  _, err = sp.GetDatabase().CommitValue(sp.GetDataset(), data)
  if err != nil {
    fmt.Fprint(os.Stderr, "Error commiting: %s\n", err)
  }
}
```

Now you will get output of the data type of our Dataset value:

```
> go run noms-tour.go
data type: List<struct  {
  given: String
  male: Bool
}>
```

Now you can access the data via your program:

```go
package main

import (
  "fmt"
  "os"

  "github.com/attic-labs/noms/go/spec"
  "github.com/attic-labs/noms/go/types"
)

func main() {
  sp, err := spec.ForDataset("http://localhost:8000::people")
  if err != nil {
    fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
    return
  }
  defer sp.Close()

  if headValue, ok := sp.GetDataset().MaybeHeadValue(); !ok {
    fmt.Fprintf(os.Stdout, "head is empty\n")
  } else {
    // type assertion to convert Head to List
    personList := headValue.(types.List)
    // type assertion to convert List Value to Struct
    personStruct := personList.Get(0).(types.Struct)
    // prints: Rickon
    fmt.Fprintf(os.Stdout, "given: %v\n", personStruct.Get("given"))
  }
}
```

Running it now:

```sh
> go run noms-tour.go
given: Rickon
```

You can see this data using the command-line too:

```sh
> noms ds http://localhost:8000
people

> noms show http://localhost:8000::people
struct Commit {
  meta: struct {},
  parents: Set<Ref<Cycle<Commit>>>,
  value: List<struct Person {
    given: String,
    male: Bool,
  }>,
}({
  meta:  {},
  parents: {
    hshltip9kss28uu910qadq04mhk9kuko,
  },
  value: [  // 4 items
    Person {
      given: "Rickon",
      male: true,
    },
...
```

Let's add some more data.

```go
package main

import (
  "fmt"
  "os"

  "github.com/attic-labs/noms/go/spec"
  "github.com/attic-labs/noms/go/types"
)

func main() {
  sp, err := spec.ForDataset("http://localhost:8000::people")
  if err != nil {
    fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
    return
  }
  defer sp.Close()

  if headValue, ok := sp.GetDataset().MaybeHeadValue(); !ok {
    fmt.Fprintf(os.Stdout, "head is empty\n")
  } else {
    // type assertion to convert Head to List
    personList := headValue.(types.List)
    data := personList.Append(
      types.NewStruct("Person", types.StructData{
        "given":  types.String("Jon"),
        "family": types.String("Snow"),
        "male":   types.Bool(true),
      }),
    )

    fmt.Fprintf(os.Stdout, "data type: %v\n", data.Type().Describe())

    _, err = sp.GetDatabase().CommitValue(sp.GetDataset(), data)
    if err != nil {
      fmt.Fprint(os.Stderr, "Error commiting: %s\n", err)
    }
  }
}
```

Running this:

```sh
> go run noms-tour.go
data type: List<struct Person {
  family: String,
  given: String,
  male: Bool,
} | struct Person {
  given: String,
  male: Bool,
}>
```

Datasets are versioned. When you *commit* a new value, you aren't overwriting the old value, but adding to a historical log of values:

```sh
> noms log http://localhost:8000::people
commit ba3lvopbgcqqnofm3qk7sk4j2doroj1l
Parent: f0b1befu9jp82r1vcd4gmuhdno27uobi
(root) {
+   Person {
+     family: "Snow",
+     given: "Jon",
+     male: true,
+   }
  }

commit f0b1befu9jp82r1vcd4gmuhdno27uobi
Parent: hshltip9kss28uu910qadq04mhk9kuko

commit hshltip9kss28uu910qadq04mhk9kuko
Parent: None
```

## Values

Noms supports a [variety of datatypes](intro.md#types) beyond List, Struct, String, and Bool we used above.

## Samples

You can continue learning more about the Noms Go SDK by looking at the documentation and by reviewing the [samples](https://github.com/attic-labs/noms/blob/master/samples/go). The [hr sample](https://github.com/attic-labs/noms/blob/master/samples/go/hr) is a more complete implementation of our example above and will help you to see further usage of the other datatypes.
