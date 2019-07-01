package main

import (
	"io/ioutil"
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/config"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/doltops"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/utils"
)

func main() {
	// Because this is a git smudge filter, the pointer file contents
	// are read through stdin.
	r := bufio.NewReader(os.Stdin)
	bs, err := ioutil.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}

	// Print the pointer file contents right back to stdout; the smudge filter
	// uses this output to replace the contents of the smudged file. In this case,
	// no changes to the file are desired (though this may change).
	fmt.Printf("%s", bs)

	cfg, err := config.Parse(string(bs))
	if err != nil {
		log.Fatalf("error parsing config: %v", err)
	}

	dirname := utils.LastSegment(cfg.Remote)

	// We send output intended for the console to stderr instead of stdout
	// or else it will end up in the pointer file.
	fmt.Fprintf(os.Stderr, "Found git-dolt pointer file. Cloning remote %s to revision %s in directory %s...", cfg.Remote, cfg.Revision, dirname)

	if err := doltops.CloneToRevisionSilent(cfg.Remote, cfg.Revision); err != nil {
		log.Fatalf("error cloning repository: %v", err)
	}

	fmt.Fprintln(os.Stderr, "done.")
}
