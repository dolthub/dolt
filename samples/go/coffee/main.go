// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/verbose"
	flag "github.com/juju/gnuflag"
)

var people = []string{
	"aa",
	"arv",
	"dan",
	"ehalpern",
	"kalman",
	"rafael",
	"zane",
}

func main() {
	dataset := flag.String("ds", "", "Dataset to read/write from")
	wh := flag.String("slack", "", "Slack webhook to ping with update")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n", os.Args[0])
		flag.PrintDefaults()
	}

	verbose.RegisterVerboseFlags(flag.CommandLine)
	flag.Parse(true)

	if *dataset == "" || *wh == "" {
		fmt.Fprintln(os.Stderr, "Required arguments not present")
		return
	}

	cfg := config.NewResolver()
	db, ds, err := cfg.GetDataset(*dataset)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
		return
	}
	defer db.Close()

	pst, err := time.LoadLocation("America/Tijuana")
	d.CheckErrorNoUsage(err)
	now := time.Now().In(pst)
	lastTime, idx := getCurrent(ds)
	lastTime = lastTime.In(pst)

	if lastTime.YearDay() == now.YearDay() {
		fmt.Println("Same day as last run, nothing to do. Peace.")
		return
	}

	if now.Hour() < 10 {
		fmt.Println("Waiting until people show up at work.")
		return
	}

	if now.Weekday() == time.Sunday || now.Weekday() == time.Saturday {
		fmt.Println("No coffee on weekends.")
		return
	}

	idx += 1
	if idx == len(people) {
		idx = 0
	}
	winner := people[idx]

	pokeSlack(*wh, winner)

	_, err = db.Commit(ds, types.String(winner), datas.CommitOptions{
		Meta: types.NewStruct("", types.StructData{
			"date": types.String(now.Format(spec.CommitMetaDateFormat)),
		}),
	})
	d.CheckErrorNoUsage(err)
}

func getCurrent(ds datas.Dataset) (d time.Time, idx int) {
	idx = -1

	type Commit struct {
		Meta struct {
			Date string
		}
		Value string
	}

	h, ok := ds.MaybeHead()
	if !ok {
		return
	}

	var c Commit
	err := marshal.Unmarshal(h, &c)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Got error unmarshaling: %+v\n", err)
		return
	}

	t, err := time.Parse(spec.CommitMetaDateFormat, c.Meta.Date)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Got error parsing last date: %+v\n", err)
		return
	}

	for i, _ := range people {
		if people[i] == c.Value {
			return t, i
		}
	}

	fmt.Fprintf(os.Stderr, "Couldn't find person %s, resetting at beginning", c.Value)
	return
}

func pokeSlack(url, winner string) {
	r := strings.NewReader(fmt.Sprintf("{\"text\":\"Today @%s cleans the coffee machine. Thank-you! You are awesome!\"}", winner))
	_, err := http.Post(url, "application/json", r)
	d.CheckErrorNoUsage(err)
}
