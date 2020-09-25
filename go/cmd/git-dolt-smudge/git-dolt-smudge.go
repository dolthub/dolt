// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/dolthub/dolt/go/cmd/git-dolt/config"
	"github.com/dolthub/dolt/go/cmd/git-dolt/doltops"
	"github.com/dolthub/dolt/go/cmd/git-dolt/utils"
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
