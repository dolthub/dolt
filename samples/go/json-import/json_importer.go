// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/util/jsontonoms"
	"github.com/attic-labs/noms/go/util/progressreader"
	"github.com/attic-labs/noms/go/util/status"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
)

func main() {
	performCommit := flag.Bool("commit", true, "commit the data to head of the dataset (otherwise only write the data to the dataset)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s <url> <dataset>\n", os.Args[0])
		flag.PrintDefaults()
	}

	spec.RegisterCommitMetaFlags(flag.CommandLine)
	spec.RegisterDatabaseFlags(flag.CommandLine)
	verbose.RegisterVerboseFlags(flag.CommandLine)
	flag.Parse(true)

	if len(flag.Args()) != 2 {
		d.CheckError(errors.New("expected url and dataset flags"))
	}

	cfg := config.NewResolver()
	db, ds, err := cfg.GetDataset(flag.Arg(1))
	d.CheckError(err)
	defer db.Close()

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
	start := time.Now()
	r := progressreader.New(res.Body, func(seen uint64) {
		elapsed := time.Since(start).Seconds()
		rate := uint64(float64(seen) / elapsed)
		status.Printf("%s decoded in %ds (%s/s)...", humanize.Bytes(seen), int(elapsed), humanize.Bytes(rate))
	})
	err = json.NewDecoder(r).Decode(&jsonObject)
	if err != nil {
		log.Fatalln("Error decoding JSON: ", err)
	}
	status.Done()

	if *performCommit {
		additionalMetaInfo := map[string]string{"url": url}
		meta, err := spec.CreateCommitMetaStruct(ds.Database(), "", "", additionalMetaInfo, nil)
		d.CheckErrorNoUsage(err)
		_, err = db.Commit(ds, jsontonoms.NomsValueFromDecodedJSON(jsonObject, true), datas.CommitOptions{Meta: meta})
		d.PanicIfError(err)
	} else {
		ref := db.WriteValue(jsontonoms.NomsValueFromDecodedJSON(jsonObject, true))
		fmt.Fprintf(os.Stdout, "#%s\n", ref.TargetHash().String())
	}
}
