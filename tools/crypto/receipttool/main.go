// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/attic-labs/noms/go/util/receipts"
	flag "github.com/juju/gnuflag"
)

var (
	databaseFlag = flag.String("database", "", "Name of the database this receipt is for")
	keyFlag      = flag.String("key", "", "Receipt key to encrypt the receipt as base64, 32 bytes when decoded")
	verifyFlag   = flag.String("verify", "", "Cipher text to verify (optional)")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, `receipttool generates or verifies database receipts.

A --database name and receipt --key are required.

By default, generates a receipt for --database, encrypted with --key.
If --verify is given, receipttool will instead verify that the
receipt matches --database and output "OK" on stdout if it does, or
nothing on stdout and an error string on stderr if it doesn't.
`)
		flag.PrintDefaults()
	}

	flag.Parse(true)

	if *databaseFlag == "" && *keyFlag == "" {
		flag.Usage()
		os.Exit(1)
	}

	if *databaseFlag == "" {
		exitIfError(fmt.Errorf("--database is required"))
	}

	if *keyFlag == "" {
		exitIfError(fmt.Errorf("--key is required"))
	}

	key, err := receipts.DecodeKey(*keyFlag)
	exitIfError(err)

	if *verifyFlag == "" {
		receipt, err := receipts.Generate(key, receipts.Data{
			Database:  *databaseFlag,
			IssueDate: time.Now(),
		})
		exitIfError(err)
		fmt.Println(receipt)
	} else {
		ok, err := receipts.Verify(key, *verifyFlag, &receipts.Data{
			Database: *databaseFlag,
		})
		exitIfError(err)
		if ok {
			fmt.Println("OK")
		} else {
			fmt.Println("FAIL")
		}
	}
}

func exitIfError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(1)
	}
}
