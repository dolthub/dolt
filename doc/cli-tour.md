# A Short Tour of the Noms CLI

This is a quick introduction to the Noms command-line interface. It should only take a few minutes to read, but there's also a screencast if you prefer:

[<img src="cli-screencast.png" width="500">](https://www.youtube.com/watch?v=NeBsaNdAn68)

## Install Noms

... if you haven't already. Follow the instructions [here](https://github.com/attic-labs/noms#setup).

## The `noms` command

Now you should be able to run `noms`:

```
> noms

usage: noms [-no-pager] [command] [command-args]

Available commands:

  ds
  log
  show
  sync
  ui

See noms <command> -h for information on each available command.
```

Without any arguments, `noms` lists out all available commands. To get information on a specific command, we can suffix it with `-h`:

```
> noms sync -h
Moves datasets between or within databases.

noms sync [options] <source-object> <dest-dataset>

...
```

## noms ds

There's a sample database running at http://demo.noms.io/cli-tour. Let's take a look inside...

The `noms ds` command lists the _datasets_ within a particular database:

```
> noms ds http://demo.noms.io/cli-tour

registered-businesses
film-locations
fire-incidents
fire-permits
```

## noms log

Noms datasets are versioned. You can see the history with `log`:

```
> noms log http://demo.noms.io/cli-tour::film-locations

sha1-20e6020b3f0b2728935e23f0e4c2d942e26b7ae1
Parent: sha1-b50c323c568bfff07a13fe276236cbdf40b5d846
List<struct Row {
  ActorQ201: String
  ActorQ202: String
  ActorQ203: String
  Director: String
  Distributor: String
  FunQ20Facts: String
  Locations: String
...

sha1-b50c323c568bfff07a13fe276236cbdf40b5d846
Parent: sha1-4883ff49f930718f7aafede265b51f83c99cf7bc
List<struct Row {
  ActorQ201: String
  ActorQ202: String
  ActorQ203: String
  Director: String
  Distributor: String
  FunQ20Facts: String
  Locations: String
...
```

Note that Noms is a typed system. What is being shown here is not text, but the first bit of a serialization of the structured data that Noms actually stores.

## noms show

You can see the entire serialization of any object in the database with `noms show`:

```
> noms show http://demo.noms.io/cli-tour::sha1-20e6020b3f0b2728935e23f0e4c2d942e26b7ae1

struct Commit {
  parents: Set<Ref<Cycle<0>>>
  value: Value
}({
  parents: {
    sha1-b50c323c568bfff07a13fe276236cbdf40b5d846,
  },
  value: [
    Row {
      ActorQ201: "Siddarth",
      ActorQ202: "Nithya Menon",
      ActorQ203: "Priya Anand",
      Director: "Jayendra",
      Distributor: "",
      FunQ20Facts: "",
      Locations: "Epic Roasthouse (399 Embarcadero)",
      ProductionQ20Company: "SPI Cinemas",
      ReleaseQ20Year: "2011",
      SmileQ20AgainQ2CQ20JennyQ20Lee: "",
      Title: "180",
      Writer: "Umarji Anuradha, Jayendra, Aarthi Sriram, & Suba ",
    },
```

## noms sync

You can work with Noms databases that are remote exactly the same as you work with local databases. But it's frequently useful to move data to a local machine, for example, to make a private fork or to work with the data disconnected from the source database.

Moving data in Noms is done with the `sync` command. Note that unlike Git, we do not make a distinction between _push_ and _pull_. It's the same operation in both directions:

```
> noms sync http://demo.noms.io/cli-tour::film-locations ldb:/tmp/noms::films
> noms ds ldb:/tmp/noms
films
```

We can now make an edit locally:

```
> cd $GOPATH/src/github.com/attic-labs/noms/samples/go/csv
> go install ./...
> csv-export ldb:/tmp/noms::films > /tmp/film-locations.csv
```

open /tmp/film-location.csv and edit it, then:

```
> csv-import ldb:/tmp/noms::films /tmp/film-locations.csv
```

#noms diff

The `noms diff` command can show you the differences between any two values. Let's see our change:

```
> noms diff http://demo.noms.io/cli-tour::film-locations ldb:/tmp/noms::films

./.parents {
-   Ref<struct Commit {
-     parents: Set<Ref<Cycle<0>>>
-     value: Value
-   }>(sha1-4883ff49f930718f7aafede265b51f83c99cf7bc)
+   Ref<struct Commit {
+     parents: Set<Ref<Cycle<0>>>
+     value: Value
+   }>(sha1-b50c323c568bfff07a13fe276236cbdf40b5d846)
  }
./.value[213] {
-   "Locations": "Mission Delores Park (Mission District) via J-Church MUNI Train"
+   "Locations": "Mission Dolores Park (Mission District) via J-Church MUNI Train"
...
```
