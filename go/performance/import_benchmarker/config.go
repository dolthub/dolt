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

package main

import (
	"encoding/json"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"io/ioutil"
	"os"
	"path/filepath"
)

const testTable = "testTable"

type ImportBenchmarkConfig struct {
	Name string

	NumRows int

	Sorted bool

	Preset bool

	Format string
}

// NewDefaultImportBenchmarkConfig returns a default import configuration where data is generated with accordance to
// the medium set.
func NewDefaultImportBenchmarkConfig() *ImportBenchmarkConfig {
	return &ImportBenchmarkConfig{
		Name:    "dolt_import_medium",
		NumRows: mediumSet,
		Sorted:  false,
		Preset:  false,
		Format:  csvExt,
	}
}

// FromFileConfig takes in a configuration file (encoded as JSON) and returns the relevant importBenchmark config
func FromFileConfig(configPath string) (*ImportBenchmarkConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := &ImportBenchmarkConfig{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

type ImportBenchmarkTest struct {
	sch *SeedSchema // should only be needed in generated test cases

	filePath string // path to file
}

// NewImportBenchmarkTest creates the test conditions for an import benchmark to execute. In the case that the config
// dictates that data needs to be generated, this function handles that
func NewImportBenchmarkTest(config *ImportBenchmarkConfig) *ImportBenchmarkTest {
	if config.Preset {
		panic("Unsupported")
	}

	sch := NewSeedSchema(config.NumRows, genSampleCols(), config.Format)
	testFile := generateTestFile(config, filesys.LocalFS, sch)

	return &ImportBenchmarkTest{
		sch:      sch,
		filePath: testFile,
	}
}

func generateTestFile(config *ImportBenchmarkConfig, fs filesys.Filesys, sch *SeedSchema) string {
	pathToImportFile := filepath.Join(getWorkingDir(), fmt.Sprintf("testData.%s", config.Format))
	wc, err := fs.OpenForWrite(pathToImportFile, os.ModePerm)
	if err != nil {
		panic(err.Error())
	}

	defer wc.Close()

	ds := NewDSImpl(wc, sch, seedRandom, testTable)
	ds.GenerateData()

	return pathToImportFile
}
