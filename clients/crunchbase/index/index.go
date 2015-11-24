package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
)

var (
	inputID  = flag.String("in", "", "dataset to pull data from.")
	outputID = flag.String("out", "", "dataset to store data in.")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Usage = func() {
		fmt.Printf("Usage: %s -ldb=/path/to/db -in=<dataset> -out=<dataset>\n\n", os.Args[0])
		flag.PrintDefaults()
	}

	flags := datas.NewFlags()
	flag.Parse()

	ds, ok := flags.CreateDataStore()
	if !ok || *inputID == "" || *outputID == "" {
		flag.Usage()
		return
	}
	defer ds.Close()

	err := d.Try(func() {
		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}
		inputDataset := dataset.NewDataset(ds, *inputID)
		outputDataset := dataset.NewDataset(ds, *outputID)

		input := inputDataset.Head().Value() //.(MapOfStringToRefOfCompany)
		tr := input.(types.Ref)
		tv := tr.TargetValue(ds)
		v, ok := tv.(MapOfStringToRefOfCompany)
		d.Chk.True(ok, "Unexpected data in dataset. Found %T", tv)

		l := float64(v.Len())
		i := float64(0)
		mu := sync.Mutex{}

		mapOfRoundsDef := MapOfRefOfKeyToSetOfRefOfRoundDef{}

		addRound := func(key Key, r RefOfRound) {
			keyRef := key.Ref()

			var setDef SetOfRefOfRoundDef
			setDef, ok := mapOfRoundsDef[keyRef]
			if !ok {
				setDef = SetOfRefOfRoundDef{}
			}

			mu.Lock()
			setDef[r.TargetRef()] = true
			mapOfRoundsDef[keyRef] = setDef
			mu.Unlock()
		}

		addTimeRounds := func(tn int64, r RefOfRound) {
			t := time.Unix(tn, 0)
			year := int32(t.Year())
			yk := NewKey().SetYear(year)
			addRound(yk, r)

			var q QuarterEnum
			switch t.Month() {
			case time.January, time.February, time.March:
				q = Q1
			case time.April, time.May, time.June:
				q = Q2
			case time.July, time.August, time.September:
				q = Q3
			case time.October, time.November, time.December:
				q = Q4
			}

			qk := NewKey().SetQuarter(QuarterDef{Year: year, Quarter: q}.New())
			addRound(qk, r)
		}

		v.IterAllP(64, func(permalink string, r RefOfCompany) {
			mu.Lock()
			i++
			fmt.Printf("\rIndexing companies: %d/%d (%.f%%)", i, l, i/l*100)
			mu.Unlock()
			company := r.TargetValue(ds)
			categoryList := company.CategoryList()
			regionKey := NewKey().SetRegion(company.Region())
			company.Rounds().IterAll(func(r RefOfRound) {
				round := r.TargetValue(ds)
				categoryList.IterAllP(64, func(category string) {
					key := NewKey().SetCategory(category)
					addRound(key, r)
				})

				addRound(regionKey, r)
				addTimeRounds(round.FundedAt(), r)
			})
		})

		fmt.Printf("\r\033[KConverting to Noms...")

		output := mapOfRoundsDef.New()
		fmt.Printf("\r\033[KCommitting...\n")
		_, ok = outputDataset.Commit(output)
		d.Exp.True(ok, "Could not commit due to conflicting edit")

		util.MaybeWriteMemProfile()
		// fmt.Printf("\n")
	})
	if err != nil {
		log.Fatal(err)
	}
}
