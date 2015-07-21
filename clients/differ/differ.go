package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

var (
	customUsage = func() {
		fmtString := `%s emits hashes reachable from root <big> that cannot be reached from root <small>.`
		fmt.Fprintf(os.Stderr, fmtString, os.Args[0])
		fmt.Fprintf(os.Stderr, "\n\nUsage: %s [options] <small-root> <big-root>\n", os.Args[0])
		flag.PrintDefaults()
	}
)

func main() {
	flags := chunks.NewFlags()
	flag.Usage = customUsage
	flag.Parse()

	cs := flags.CreateStore()
	small := flag.Arg(0)
	big := flag.Arg(1)
	if cs == nil || small == "" || big == "" {
		flag.Usage()
		return
	}

	smallRef, err := ref.Parse(small)
	if err != nil {
		log.Fatalln("Could not parse small root hash:", err)
	}
	smallValue, err := types.ReadValue(smallRef, cs)
	if err != nil {
		log.Fatalln("Could not read value for small root hash:", err)
	}

	bigRef, err := ref.Parse(big)
	if err != nil {
		log.Fatalln("Could not parse big root hash:", err)
	}
	bigValue, err := types.ReadValue(bigRef, cs)
	if err != nil {
		log.Fatalln("Could not read value for big root hash:", err)
	}

	refs := walk.Diff(smallValue, bigValue)
	fmt.Println(refs)
}
