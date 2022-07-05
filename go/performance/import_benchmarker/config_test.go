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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func TestGeneratedConfigCanBeImported(t *testing.T) {
	config := NewDefaultImportBenchmarkConfig()

	tests := NewImportBenchmarkTests(config)
	results := RunBenchmarkTests(config, tests)

	assert.Equal(t, 1, len(results))
	assert.Equal(t, "dolt_import_small", results[0].name)

	// Sanity check: An import of 100,000 should never take more than 15 seconds
	assert.LessOrEqual(t, results[0].br.T, time.Second*15)

	wd, err := os.Getwd()
	if err != nil {
		panic(err.Error())
	}

	RemoveTempDoltDataDir(filesys.LocalFS, wd)
	os.RemoveAll(filepath.Join(wd, "testData.csv"))
}
