// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/util/jsontonoms"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s <url> <dataset>\n", os.Args[0])
		flag.PrintDefaults()
	}

	spec.RegisterDatabaseFlags()
	flag.Parse()

	if len(flag.Args()) != 2 {
		d.CheckError(errors.New("expected url and dataset flags"))
	}

	ds, err := spec.GetDataset(flag.Arg(1))
	d.CheckError(err)

	url := flag.Arg(0)
	if url == "" {
		flag.Usage()
	}

	res, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error fetching %s: %+v\n", url, err)
	} else if res.StatusCode != 200 {
		log.Fatalf("Error fetching %s: %s\n", url, res.Status)
	}
	defer res.Body.Close()

	var jsonObject interface{}
	err = json.NewDecoder(res.Body).Decode(&jsonObject)
	if err != nil {
		log.Fatalln("Error decoding JSON: ", err)
	}

	_, err = ds.Commit(jsontonoms.NomsValueFromDecodedJSON(jsonObject, true))
	d.PanicIfError(err)
	ds.Database().Close()
}
