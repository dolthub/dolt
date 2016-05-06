package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/types"
)

var (
	p = flag.Uint("p", 512, "parallelism")
)

func main() {
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s [options] <source-object> <dest-dataset>\n", os.Args[0])
		flag.PrintDefaults()
	}

	flags.RegisterDataStoreFlags()
	flag.Parse()

	if flag.NArg() != 2 {
		util.CheckError(errors.New("expected a source object and destination dataset"))
	}

	sourceSpec, err := flags.ParsePathSpec(flag.Arg(0))
	util.CheckError(err)
	sourceStore, sourceObj, err := sourceSpec.Value()
	util.CheckError(err)
	defer sourceStore.Close()

	sinkSpec, err := flags.ParseDatasetSpec(flag.Arg(1))
	util.CheckError(err)

	sinkDataset, err := sinkSpec.Dataset()
	util.CheckError(err)
	defer sinkDataset.Store().Close()

	err = d.Try(func() {
		if util.MaybeStartCPUProfile() {
			defer util.StopCPUProfile()
		}

		var err error
		sinkDataset, err = sinkDataset.Pull(sourceStore, types.NewTypedRefFromValue(sourceObj), int(*p))

		util.MaybeWriteMemProfile()
		d.Exp.NoError(err)
	})

	if err != nil {
		log.Fatal(err)
	}
}
