package util

import (
	"flag"
	"fmt"
	"os"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
)

type ClientFlags struct {
	dsFlags          dataset.DatasetFlags
	concurrencyFlag  *int
	progressFileFlag *string
	progressFile     *os.File
}

func NewFlags() ClientFlags {
	return NewFlagsWithPrefix("")
}

func NewFlagsWithPrefix(prefix string) ClientFlags {
	return ClientFlags{
		dataset.NewFlagsWithPrefix(prefix),
		flag.Int(prefix+"concurrency", 100, "degree of concurrency"),
		flag.String(prefix+"progress-file", "", "file for logging progress"),
		nil,
	}
}

func (cf *ClientFlags) CreateDataset() *dataset.Dataset {
	return cf.dsFlags.CreateDataset()
}

func (cf *ClientFlags) Concurrency() int {
	return *cf.concurrencyFlag
}

func (cf *ClientFlags) CreateProgressFile() error {
	if *cf.progressFileFlag == "" {
		return fmt.Errorf("No progress file specified")
	}
	pf, err := os.Create(*cf.progressFileFlag)
	if err != nil {
		return fmt.Errorf("Unable to create progress status file: %s, err: %s", *cf.progressFileFlag, err)
	}
	cf.progressFile = pf
	cf.UpdateProgress(0.0)
	return err
}

func (cf *ClientFlags) CloseProgressFile() {
	d.Chk.NotNil(cf.progressFile, "Progress file was never created")
	cf.progressFile.Close()
}

func (cf *ClientFlags) UpdateProgress(pct float32) {
	d.Chk.True(pct >= 0.0 && pct <= 1.0, "%.6f is not a valid percentage, must be in range [0, 1]", pct)
	if cf.progressFile != nil {
		cf.progressFile.WriteString(fmt.Sprintf("%.6f\n", pct))
	}
}
