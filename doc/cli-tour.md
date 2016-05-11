# A Short Tour of the Noms CLI

This is a quick introduction to the Noms command-line interface.

It should only take a few minutes to read, but there's also a screencast for the impatient:

[![screencast](http://img.youtube.com/vi/eRZ4pO0gVWw/0.jpg)](https://www.youtube.com/watch?v=eRZ4pO0gVWw)

## Requirements

* [Go (v1.6+)](https://golang.org/)
* **Important:** Ensure that your [$GOPATH](https://golang.org/doc/code.html#GOPATH) variable is set correctly. Everyone forgets that.

## Install Noms

On a command-line:

```sh
cd $GOPATH
go get https://github.com/attic-labs/noms
cd src/github.com/attic-labs/noms/cmd
go install ./...
```

Now you should be able to run noms:

```
> noms

usage: noms [-no-pager] [command] [command-args]

Available commands:

  clone
  diff
  ds
  log
  show
  sync
  view

See noms <command> -h for information on each available command.
```

## The `noms` command

Without any arguments, `noms` lists out all available commands. To get information on a specific command, we can suffix it with `-h`:

```
> noms clone -h

Makes a complete copy of a database and all its datasets.

usage: noms clone <source-database> <destination-database>

For information on spelling databases, datasets, and values, see noms.io/doc/spelling
```

## Explore

Let's get a sample database to play with. There are a bunch at http://dev.noms.io/aa.

```
> noms clone http://dev.noms.io/aa/sf ldb:~/noms/sf

Cloned 200MB (2035 chunks) in 1.2 minutes (2.78MB/s)
```

We can now explore the datasets inside that db:

```
> noms ds ldb:~/noms/sf

registered-businesses
fire-incidents
fire-permits
movie-locations
```

## History

Noms datasets are versioned. Let's see the history of one:

```
> noms log ldb:~/noms/sf:registered-businesses

sha1-b2ad9ca3f4936c6f4d602c02f92bf8bdf4f9cfbe
@[43678]
Row {
	Location_ID: 9990000024,
	Business_Account_Number: "000071",
-	Ownership_Name: "Ideal Novak Corp",
-	DBA_Name: "Ideal Novak Corp",
+	Ownership_Name: "Practical Novak Corp",
+	DBA_Name: "Practical Novak Corp",
	Street_Address: "8 Mendosa Ave",
	City: "San Francisco",
	State: "CA",
	Zip_Code: "94123",
	Business_Start_Date: "10/01/1968",
}

@[334567]
+ Row {
+	Location_ID: 9990000024,
+	Business_Account_Number: "000071",
+	Ownership_Name: "Practical Novak Corp",

...
```

You can also stream the detail of any individual object:

```
> noms show ldb:~/noms/sf:sha1-b2ad9ca3f4936c6f4d602c02f92bf8bdf4f9cfbe

struct Commit {
  parents: Set<Ref<Parent<0>>>
  value: Value
}({
  parents: {
    sha1-a039f6e56efa4b84671723cf8c396332d0deee54,
  },
  value: [
    Row {
      category: "major",
      city: "Hong Kong",
      continent: "AS",
      coordinates_wkt: "POINT (113.93501638737635 22.315332828086753)",
      country: "HK",
      iata: "HKG",

...
```

## Sync

Let's add some new data:

```
> cd $GOPATH/src/github.com/attic-labs/noms/clients/go/csv
> go install ./...
> csv-export ldb:~/noms/sf:film-locations /tmp/film-locations.csv
```

open /tmp/film-location.csv and edit it, then:

```
> csv-import /tmp/film-location.csv ldb:/tmp/noms:film-locations
> noms diff ldb:~/noms/sf:film-locations ldb:/tmp/noms:film-locations

@[11234]
+ Row {
+	Location_ID: 9990000024,
+	Business_Account_Number: "000071",
+	Ownership_Name: "Practical Novak Corp",
+ }

> noms sync ldb:/tmp/film-locations ldb:~/noms/sf:film-locations
Synced 1.2k (1 chunk) in 50ms (2MB/s)