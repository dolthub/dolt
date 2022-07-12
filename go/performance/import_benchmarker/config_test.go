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
)

func TestGeneratedConfigCanBeImported(t *testing.T) {
	t.Skip() // Skipping since dolt isn't installed on the github actions vm

	config := NewDefaultImportBenchmarkConfig()
	wd := GetWorkingDir()

	results := RunBenchmarkTests(config, wd)

	assert.Equal(t, 1, len(results))
	assert.Equal(t, "dolt_import_small", results[0].name)

	// Sanity check: An import of 100,000 should never take more than 15 seconds
	assert.LessOrEqual(t, results[0].br.T, time.Second*15)

	os.RemoveAll(filepath.Join(wd, "testData.csv"))
}

func TestCanGenerateFilesForAllFormats(t *testing.T) {
	config := &ImportBenchmarkConfig{Jobs: make([]*ImportBenchmarkJob, 0)}

	// Create jobs for all configs
	for _, format := range supportedFormats {
		job := &ImportBenchmarkJob{
			Name:         "dolt_import_small",
			NumRows:      smallSet,
			Sorted:       false,
			Format:       format,
			DoltVersion:  "HEAD", // Use whatever dolt is installed locally
			DoltExecPath: "dolt", // Assumes dolt is installed locally
		}

		config.Jobs = append(config.Jobs, job)
	}

	assert.Equal(t, 3, len(config.Jobs))

	config = generateTestFilesIfNeeded(config)

	for _, job := range config.Jobs {
		file, err := os.Open(job.Filepath)
		assert.NoError(t, err)

		err = file.Close()
		assert.NoError(t, err)

		err = os.Remove(job.Filepath)
		assert.NoError(t, err)
	}
}
