package main

import (
	"flag"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/pkg/profile"
	"log"
	"time"
)

type NextEdit func() (types.Value, types.Value)

type MEBenchmark interface {
	GetName() string
	AddEdits(nextEdit NextEdit)
	SortEdits()
	Map()
}

func main() {
	profPath := flag.String("profpath", "./", "")
	cpuProf := flag.Bool("cpuprof", false, "")
	memProf := flag.Bool("memprof", false, "")
	meBench := flag.Bool("me-bench", false, "")
	aseBench := flag.Bool("ase-bench", false, "")
	count := flag.Int("n", 1000000, "")
	flag.Parse()

	if *cpuProf {
		fmt.Println("cpu profiling enabled.")
		fmt.Println("writing cpu prof to", *profPath)
		defer profile.Start(profile.CPUProfile).Stop()
	}

	if *memProf {
		fmt.Println("mem profiling enabled.")
		fmt.Println("writing mem prof to", *profPath)
		defer profile.Start(profile.MemProfile).Stop()
	}

	var toBench []MEBenchmark
	if *meBench {
		toBench = append(toBench, NewNomsMEBench())
	}

	if *aseBench {
		toBench = append(toBench, NewASEBench(10000, 2, 8))
	}

	log.Printf("Running each benchmark for %d items\n", *count)
	tg := NewTupleGen(*count)
	run(tg, toBench)
}

func benchmark(meb MEBenchmark, nextKVP NextEdit) {
	startAdd := time.Now()
	meb.AddEdits(nextKVP)
	endAdd := time.Now()
	addDelta := endAdd.Sub(startAdd)

	log.Printf("%s - add time: %f\n", meb.GetName(), addDelta.Seconds())

	startSort := time.Now()
	meb.SortEdits()
	endSort := time.Now()
	sortDelta := endSort.Sub(startSort)

	log.Printf("%s - sort time: %f\n", meb.GetName(), sortDelta.Seconds())

	/*startMap := time.Now()
	meb.Map()
	endMap := time.Now()
	mapDelta := endMap.Sub(startMap)

	log.Printf("%s - map time: %f\n", meb.GetName(), mapDelta.Seconds())*/
}

func run(tg *TupleGen, toBench []MEBenchmark) {
	for _, currBench := range toBench {
		log.Println("Starting", currBench.GetName())
		tg.Reset()
		benchmark(currBench, tg.NextKVP)
		log.Println(currBench.GetName(), "completed")
	}
}
