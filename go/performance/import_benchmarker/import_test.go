// Copyright 2022 Dolthub, Inc.
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

func TestCI(t *testing.T) {
	// this will be a lot slower than running `cmd/main.go -test testdata/ci.yaml`
	t.Skip()
	RunTestsFile(t, "testdata/ci.yaml")
}
