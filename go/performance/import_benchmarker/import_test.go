package import_benchmarker

import (
	"testing"
)

func TestImportSize(t *testing.T) {
	t.Skip()
	RunTestsFile(t, "testdata/size.yaml")
}

func TestExternalImport(t *testing.T) {
	t.Skip()
	RunTestsFile(t, "testdata/external.yaml")
}

func TestDoltImport(t *testing.T) {
	t.Skip()
	RunTestsFile(t, "testdata/dolt_server.yaml")
}

func TestShuffle(t *testing.T) {
	t.Skip()
	RunTestsFile(t, "testdata/shuffle.yaml")
}
