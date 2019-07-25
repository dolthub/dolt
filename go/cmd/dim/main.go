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
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime/debug"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
)

const (
	logFilePath = ".dedit.log"
)

var verboseOut bool
var logFile *os.File
var bufWr *bufio.Writer

func init() {
	var err error
	logFile, err = os.OpenFile(logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)

	if err != nil {
		panic(err)
	}

	bufWr = bufio.NewWriter(logFile)
	log.SetOutput(bufWr)
}

func closeLogging() {
	if r := recover(); r != nil {
		st := debug.Stack()
		log.Printf("%s\n", r)
		log.Println(string(st))
	}

	bufWr.Flush()
	logFile.Close()

	data, err := ioutil.ReadFile(logFilePath)

	if err == nil && verboseOut {
		iohelp.WriteAll(os.Stdout, data)
	}
}

func main() {
	defer closeLogging()

	flag.BoolVar(&verboseOut, "v", false, "output verbose logging after completion")
	flag.Parse()

	ctx := context.Background()

	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB)

	if !dEnv.HasDoltDataDir() {
		fmt.Fprintf(os.Stderr, "fatal: not a dolt data repository.")
		os.Exit(1)
	}

	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "missing required argument <table>")
		fmt.Fprintln(os.Stderr, "usage: dim [<flags>] <table>")
		flag.PrintDefaults()

		os.Exit(1)
	}

	tableName := flag.Arg(0)
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		panic(err)
	}

	tbl, ok := root.GetTable(ctx, tableName)

	if !ok {
		fmt.Fprintf(os.Stderr, "Could not find table '%s'.\n", tableName)
		os.Exit(1)
	}

	data := tbl.GetRowData(ctx)
	sch := tbl.GetSchema(ctx)

	dim := New(ctx, sch, data)
	updatedRows := dim.Run(ctx)

	if !data.Equals(updatedRows) {
		updatedTbl := tbl.UpdateRows(ctx, updatedRows)
		updatedRoot := root.PutTable(ctx, dEnv.DoltDB, tableName, updatedTbl)
		dEnv.UpdateWorkingRoot(ctx, updatedRoot)
	}
}
