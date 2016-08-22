// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	flag "github.com/juju/gnuflag"
)

func main() {
	var dsStr = flag.String("ds", "", "noms dataset to read/write from")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] [command] [command-args]\n\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nCommands:")
		fmt.Fprintln(os.Stderr, "\tadd-person <id> <name> <title>")
		fmt.Fprintln(os.Stderr, "\tlist-persons")
	}

	flag.Parse(true)

	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "Not enough arguments")
		return
	}

	if *dsStr == "" {
		fmt.Fprintln(os.Stderr, "Required flag '--ds' not set")
		return
	}

	ds, err := spec.GetDataset(*dsStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
		return
	}
	defer ds.Database().Close()

	switch flag.Arg(0) {
	case "add-person":
		addPerson(ds)
	case "list-persons":
		listPersons(ds)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", flag.Arg(0))
	}
}

type Person struct {
	Name, Title string
	Id          uint64
}

func addPerson(ds dataset.Dataset) {
	if flag.NArg() != 4 {
		fmt.Fprintln(os.Stderr, "Not enough arguments for command add-person")
		return
	}

	id, err := strconv.ParseUint(flag.Arg(1), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid person-id: %s", flag.Arg(1))
		return
	}

	np, err := marshal.Marshal(Person{flag.Arg(2), flag.Arg(3), id})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	_, err = ds.CommitValue(getPersons(ds).Set(types.Number(id), np))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error committing: %s\n", err)
		return
	}
}

func listPersons(ds dataset.Dataset) {
	d := getPersons(ds)
	if d.Empty() {
		fmt.Println("No people found")
		return
	}

	d.IterAll(func(k, v types.Value) {
		var p Person
		err := marshal.Unmarshal(v, &p)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		fmt.Printf("%s (id: %d, title: %s)\n", p.Name, p.Id, p.Title)
	})
}

func getPersons(ds dataset.Dataset) types.Map {
	hv, ok := ds.MaybeHeadValue()
	if ok {
		return hv.(types.Map)
	} else {
		return types.NewMap()
	}
}
