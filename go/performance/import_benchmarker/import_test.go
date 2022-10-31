package import_benchmarker

import (
	"testing"
)

func TestImportSize(t *testing.T) {
	RunTestsFile(t, "testdata/size.yaml")
}

func TestExternalImport(t *testing.T) {
	RunTestsFile(t, "testdata/external.yaml")
}

func TestDoltImport(t *testing.T) {
	RunTestsFile(t, "testdata/dolt_server.yaml")
}

func TestShuffle(t *testing.T) {
	RunTestsFile(t, "testdata/shuffle.yaml")
}

func TestAll(t *testing.T) {
	RunTestsFile(t, "testdata/all.yaml")
}
