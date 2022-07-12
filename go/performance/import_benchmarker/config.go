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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const (
	smallSet  = 100000
	mediumSet = 1000000
	largeSet  = 10000000
	testTable = "testTable"
)

type ImportBenchmarkJob struct {
	// Name of the job
	Name string

	// NumRows represents the number of rows being imported in the job.
	NumRows int

	// Sorted represents whether the data is sorted or not.
	Sorted bool

	// Format is either csv, json or sql.
	Format string

	// Filepath is the path to the csv file. If empty data is generated instead.
	Filepath string

	// DoltVersion tracks the current version of Dolt being used.
	DoltVersion string

	// DoltExecPath is a path towards a Dolt executable. This is useful for executing Dolt against a particular version.
	DoltExecPath string
}

type ImportBenchmarkConfig struct {
	Jobs []*ImportBenchmarkJob
}

// NewDefaultImportBenchmarkConfig returns a default import configuration where data is generated with accordance to
// the medium set.
func NewDefaultImportBenchmarkConfig() *ImportBenchmarkConfig {
	jobs := []*ImportBenchmarkJob{
		{
			Name:         "dolt_import_small",
			NumRows:      smallSet,
			Sorted:       false,
			Format:       csvExt,
			DoltVersion:  "HEAD", // Use whatever dolt is installed locally
			DoltExecPath: "dolt", // Assumes dolt is installed locally
		},
	}

	return &ImportBenchmarkConfig{
		Jobs: jobs,
	}
}

// FromFileConfig takes in a configuration file (encoded as JSON) and returns the relevant importBenchmark config
func FromFileConfig(configPath string) (*ImportBenchmarkConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := &ImportBenchmarkConfig{
		Jobs: make([]*ImportBenchmarkJob, 0),
	}

	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// generateTestFilesIfNeeded creates the test conditions for an import benchmark to execute. In the case that the config
// dictates that data needs to be generated, this function handles that
func generateTestFilesIfNeeded(config *ImportBenchmarkConfig) *ImportBenchmarkConfig {
	returnConfig := &ImportBenchmarkConfig{Jobs: make([]*ImportBenchmarkJob, 0)}

	for _, job := range config.Jobs {
		// Preset csv path
		if job.Filepath != "" {
			returnConfig.Jobs = append(returnConfig.Jobs, job)
		} else {
			filePath, fileFormat := getGeneratedBenchmarkTest(job)

			job.Filepath = filePath
			job.Format = fileFormat

			returnConfig.Jobs = append(returnConfig.Jobs, job)
		}
	}

	return returnConfig
}

// getGeneratedBenchmarkTest is used to create a generated test case with a randomly generated csv file.
func getGeneratedBenchmarkTest(job *ImportBenchmarkJob) (string, string) {
	sch := NewSeedSchema(job.NumRows, genSampleCols(), job.Format)
	testFilePath := generateTestFile(filesys.LocalFS, sch, GetWorkingDir())

	return testFilePath, sch.FileFormatExt
}

func generateTestFile(fs filesys.Filesys, sch *SeedSchema, wd string) string {
	pathToImportFile := filepath.Join(wd, fmt.Sprintf("testData.%s", sch.FileFormatExt))
	wc, err := fs.OpenForWrite(pathToImportFile, os.ModePerm)
	if err != nil {
		panic(err.Error())
	}

	defer wc.Close()

	ds := NewDSImpl(wc, sch, seedRandom, testTable)
	ds.GenerateData()

	return pathToImportFile
}

func RunBenchmarkTests(config *ImportBenchmarkConfig, workingDir string) []result {
	config = generateTestFilesIfNeeded(config)

	results := make([]result, 0)

	for _, job := range config.Jobs {
		results = append(results, RunDoltBenchmarkImportTest(job, workingDir))
	}

	return results
}
