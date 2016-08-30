// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"

	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	flag "github.com/juju/gnuflag"
)

func main() {
	if !poke() {
		exit(1)
	}
}

func poke() (win bool) {
	var outDSStr = flag.String("out-ds-name", "", "output dataset to write to - if empty, defaults to input dataset")

	flag.Usage = usage
	flag.Parse(false)

	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	if flag.NArg() != 3 {
		fmt.Fprintln(os.Stderr, "Incorrect number of arguments")
		return
	}

	inDS, err := spec.GetDataset(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid input dataset '%s': %s\n", flag.Arg(0), err)
		return
	}

	inRoot, ok := inDS.MaybeHeadValue()
	if !ok {
		fmt.Fprintf(os.Stderr, "Input dataset '%s' does not exist\n", flag.Arg(0))
		return
	}

	inPath, err := types.ParsePath(flag.Arg(1))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid path '%s': %s\n", flag.Arg(1), err)
		return
	}

	oldVal := inPath.Resolve(inRoot)
	if oldVal == nil {
		fmt.Fprintf(os.Stderr, "No value at path '%s' - cannot update\n", inPath.String())
		return
	}

	val, _, rem, err := types.ParsePathIndex(flag.Arg(2))
	if err != nil || rem != "" {
		fmt.Fprintf(os.Stderr, "Invalid new value: '%s': %s\n", flag.Arg(2), err)
		return
	}

	var outDS dataset.Dataset
	if *outDSStr == "" {
		outDS = inDS
	} else if !dataset.DatasetFullRe.MatchString(*outDSStr) {
		fmt.Fprintf(os.Stderr, "Invalid output dataset name: %s\n", *outDSStr)
		return
	} else {
		outDS = dataset.NewDataset(inDS.Database(), *outDSStr)
	}
	defer outDS.Database().Close()

	outRoot, err := update(inRoot, inPath, val)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	_, err = outDS.CommitValue(outRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not commit: %s\n", err)
		return
	}

	win = true
	return
}

func update(subject types.Value, path types.Path, newVal types.Value) (types.Value, error) {
	if len(path) > 1 {
		oldChild := path[0].Resolve(subject)
		var err error
		newVal, err = update(oldChild, path[1:], newVal)
		if err != nil {
			return nil, err
		}
	}

	res, err := updatePath(path[0], subject, newVal)
	if err != nil {
		return nil, fmt.Errorf("Error updating path %s: %s", path.String(), err)
	}
	return res, nil
}

func updatePath(part types.PathPart, subject, newVal types.Value) (types.Value, error) {
	switch part := part.(type) {
	case types.FieldPath:
		return subject.(types.Struct).Set(part.Name, newVal), nil
	case types.IndexPath:
		if part.IntoKey {
			return nil, fmt.Errorf("@key paths not supported")
		}
		switch subject := subject.(type) {
		case types.List:
			return subject.Set(uint64(float64(part.Index.(types.Number))), newVal), nil
		case types.Map:
			return subject.Set(part.Index, newVal), nil
		case types.Set:
			return subject.Remove(part.Index).Insert(newVal), nil
		default:
			panic("Unexpected noms type:" + subject.Type().Describe())
		}
	}
	return nil, fmt.Errorf("Unsupported path type: %#v", part)
}

func usage() {
	fmt.Fprintf(os.Stderr, "Poke modifies a single value in a noms database.\n\n")
	fmt.Fprintf(os.Stderr, "Usage: %s [-out-ds-name=<name>] <ds> <path> <new-val>\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  <ds>      : Dataset to modify\n")
	fmt.Fprintf(os.Stderr, "  <path>    : Path to a value within <ds> to modify\n")
	fmt.Fprintf(os.Stderr, "  <new-val> : new value for <path>\n\n")
	fmt.Fprintln(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
}

var exit = func(status int) {
	os.Exit(status)
}
