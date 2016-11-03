// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/jsontonoms"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/go/util/verbose"
	"github.com/clbanning/mxj"
	flag "github.com/juju/gnuflag"
)

var (
	noIO          = flag.Bool("benchmark", false, "Run in 'benchmark' mode: walk directories and parse XML files but do not write to Noms")
	performCommit = flag.Bool("commit", true, "commit the data to head of the dataset (otherwise only write the data to the dataset)")
	customUsage   = func() {
		fmtString := `%s walks the given directory, looking for .xml files. When it finds one, the entity inside is parsed into nested Noms maps/lists and committed to the dataset indicated on the command line.`
		fmt.Fprintf(os.Stderr, fmtString, os.Args[0])
		fmt.Fprintf(os.Stderr, "\n\nUsage: %s [options] <path/to/root/directory> <dataset>\n", os.Args[0])
		flag.PrintDefaults()
	}
)

type fileIndex struct {
	path  string
	index int
}

type refIndex struct {
	ref   types.Ref
	index int
}

type refIndexList []refIndex

func (a refIndexList) Len() int           { return len(a) }
func (a refIndexList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a refIndexList) Less(i, j int) bool { return a[i].index < a[j].index }

func main() {
	err := d.Try(func() {
		spec.RegisterCommitMetaFlags(flag.CommandLine)
		spec.RegisterDatabaseFlags(flag.CommandLine)
		verbose.RegisterVerboseFlags(flag.CommandLine)
		profile.RegisterProfileFlags(flag.CommandLine)
		flag.Usage = customUsage
		flag.Parse(true)

		cfg := config.NewResolver()
		if flag.NArg() != 2 {
			d.CheckError(errors.New("Expected directory path followed by dataset"))
		}
		dir := flag.Arg(0)
		db, ds, err := cfg.GetDataset(flag.Arg(1))
		d.CheckError(err)
		defer db.Close()

		defer profile.MaybeStartProfile().Stop()

		cpuCount := runtime.NumCPU()

		filesChan := make(chan fileIndex, 1024)
		refsChan := make(chan refIndex, 1024)

		getFilePaths := func() {
			index := 0
			err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					d.Panic("Cannot traverse directories")
				}
				if !info.IsDir() && filepath.Ext(path) == ".xml" {
					filesChan <- fileIndex{path, index}
					index++
				}

				return nil
			})
			d.PanicIfError(err)
			close(filesChan)
		}

		wg := sync.WaitGroup{}
		importXML := func() {
			expectedType := types.NewMap()
			for f := range filesChan {
				file, err := os.Open(f.path)
				if err != nil {
					d.Panic("Error getting XML")
				}

				xmlObject, err := mxj.NewMapXmlReader(file)
				if err != nil {
					d.Panic("Error decoding XML")
				}
				object := xmlObject.Old()
				file.Close()

				nomsObj := jsontonoms.NomsValueFromDecodedJSON(object, false)
				d.Chk.IsType(expectedType, nomsObj)

				var r types.Ref
				if !*noIO {
					r = ds.Database().WriteValue(nomsObj)
				}

				refsChan <- refIndex{r, f.index}
			}

			wg.Done()
		}

		go getFilePaths()
		for i := 0; i < cpuCount*8; i++ {
			wg.Add(1)
			go importXML()
		}
		go func() {
			wg.Wait()
			close(refsChan) // done converting xml to noms
		}()

		refList := refIndexList{}
		for r := range refsChan {
			refList = append(refList, r)
		}
		sort.Sort(refList)

		refs := make([]types.Value, len(refList))
		for idx, r := range refList {
			refs[idx] = r.ref
		}

		rl := types.NewList(refs...)

		if !*noIO {
			if *performCommit {
				additionalMetaInfo := map[string]string{"inputDir": dir}
				meta, err := spec.CreateCommitMetaStruct(ds.Database(), "", "", additionalMetaInfo, nil)
				d.CheckErrorNoUsage(err)
				_, err = db.Commit(ds, rl, datas.CommitOptions{Meta: meta})
				d.PanicIfError(err)
			} else {
				ref := db.WriteValue(rl)
				fmt.Fprintf(os.Stdout, "#%s\n", ref.TargetHash().String())
			}
		}
	})
	if err != nil {
		log.Fatal(err)
	}
}
