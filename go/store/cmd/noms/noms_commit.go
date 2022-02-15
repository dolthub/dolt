// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"

	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/config"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/util/verbose"
)

var allowDupe bool

var nomsCommit = &util.Command{
	Run:       runCommit,
	UsageLine: "commit [options] [absolute-path] <dataset>",
	Short:     "Commits a specified value as head of the dataset",
	Long:      "If absolute-path is not provided, then it is read from stdin. See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the dataset and absolute-path arguments.",
	Flags:     setupCommitFlags,
	Nargs:     1, // if absolute-path not present we read it from stdin
}

func setupCommitFlags() *flag.FlagSet {
	commitFlagSet := flag.NewFlagSet("commit", flag.ExitOnError)
	commitFlagSet.BoolVar(&allowDupe, "allow-dupe", false, "creates a new commit, even if it would be identical (modulo metadata and parents) to the existing HEAD.")
	spec.RegisterCommitMetaFlags(commitFlagSet)
	verbose.RegisterVerboseFlags(commitFlagSet)
	return commitFlagSet
}

func runCommit(ctx context.Context, args []string) int {
	cfg := config.NewResolver()
	db, vrw, ds, err := cfg.GetDataset(ctx, args[len(args)-1])
	util.CheckError(err)
	defer db.Close()

	var path string
	if len(args) == 2 {
		path = args[0]
	} else {
		readPath, _, err := bufio.NewReader(os.Stdin).ReadLine()
		util.CheckError(err)
		path = string(readPath)
	}
	absPath, err := spec.NewAbsolutePath(path)
	util.CheckError(err)

	value := absPath.Resolve(ctx, db, vrw)
	if value == nil {
		util.CheckErrorNoUsage(errors.New(fmt.Sprintf("Error resolving value: %s", path)))
	}

	oldCommitRef, oldCommitExists, err := ds.MaybeHeadRef()
	d.PanicIfError(err)

	if oldCommitExists {
		head, ok, err := ds.MaybeHeadValue()
		d.PanicIfError(err)

		if !ok {
			fmt.Fprintln(os.Stdout, "Commit has no head value.")
			return 1
		}

		hh, err := head.Hash(vrw.Format())
		d.PanicIfError(err)
		vh, err := value.Hash(vrw.Format())
		d.PanicIfError(err)

		if hh == vh && !allowDupe {
			fmt.Fprintf(os.Stdout, "Commit aborted - allow-dupe is set to off and this commit would create a duplicate\n")
			return 0
		}
	}

	meta, err := spec.CreateCommitMetaStruct(ctx, db, vrw, "", "", nil, nil)
	util.CheckErrorNoUsage(err)

	ds, err = db.Commit(ctx, ds, value, datas.CommitOptions{Meta: meta})
	util.CheckErrorNoUsage(err)

	headRef, ok, err := ds.MaybeHeadRef()

	d.PanicIfError(err)

	if !ok {
		panic("commit succeeded, but dataset has no head ref")
	}

	if oldCommitExists {

		if ok {
			fmt.Fprintf(os.Stdout, "New head #%v (was #%v)\n", headRef.TargetHash().String(), oldCommitRef.TargetHash().String())
		}
	} else {
		fmt.Fprintf(os.Stdout, "New head #%v\n", headRef.TargetHash().String())
	}
	return 0
}
