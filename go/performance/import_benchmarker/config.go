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
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const (
	smallSet  = 100000
	mediumSet = 1000000
	largeSet  = 10000000
	testTable = "test"
)

var (
	ErrMissingMysqlSchemaFile  = errors.New("error: Must supply schema file for mysql jobs")
	ErrImproperMysqlFileFormat = errors.New("error: Improper schema file for mysql")
	ErrUnsupportedProgram      = errors.New("error: Unsupported program only dolt or mysql used")
	ErrUnsupportedFileFormat   = errors.New("error: Unsupport formated. Only csv, json or sql allowed")
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

	// Filepath is the path to the data file. If empty data is generated instead.
	Filepath string

	// Program is either Dolt or MySQL.
	Program string

	// Version tracks the current version of Dolt or MySQL being used
	Version string

	// ExecPath is a path towards a Dolt or MySQL executable. This is also useful when running different versions of Dolt.
	ExecPath string

	// SchemaPath is a path towards a generated schema. It is needed for MySQL testing and optional for Dolt testing
	SchemaPath string
}

type ImportBenchmarkConfig struct {
	Jobs []*ImportBenchmarkJob

	// MysqlConnectionProtocol is either tcp or unix. On our kubernetes benchmarking deployments unix is needed. To run this
	// locally you want tcp
	MysqlConnectionProtocol string

	// MysqlPort is used to connect with a MySQL port
	MysqlPort int

	// MysqlHost is used to connect with a MySQL host
	MysqlHost string

	// NbfVersion is used to turn what format to run Dolt against
	NbfVersion string
}

// NewDefaultImportBenchmarkConfig returns a default import configuration where data is generated with accordance to
// the medium set.
func NewDefaultImportBenchmarkConfig() (*ImportBenchmarkConfig, error) {
	jobs := []*ImportBenchmarkJob{
		{
			Name:     "dolt_import_small",
			NumRows:  smallSet,
			Sorted:   false,
			Format:   csvExt,
			Version:  "HEAD", // Use whatever dolt is installed locally
			ExecPath: "dolt", // Assumes dolt is installed locally
			Program:  "dolt",
		},
	}

	config := &ImportBenchmarkConfig{
		Jobs: jobs,
	}

	err := config.ValidateAndUpdateDefaults()
	if err != nil {
		return nil, err
	}

	return config, nil
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

	err = config.ValidateAndUpdateDefaults()
	if err != nil {
		return nil, err
	}

	return config, nil
}

func (c *ImportBenchmarkConfig) ValidateAndUpdateDefaults() error {
	if c.MysqlConnectionProtocol == "" {
		c.MysqlConnectionProtocol = "tcp"
	}

	if c.MysqlHost == "" {
		c.MysqlHost = defaultHost
	}

	if c.MysqlPort == 0 {
		c.MysqlPort = defaultPort
	}

	for _, job := range c.Jobs {
		err := job.updateDefaultsAndValidate()
		if err != nil {
			return err
		}
	}

	return nil
}

func (j *ImportBenchmarkJob) updateDefaultsAndValidate() error {
	j.Program = strings.ToLower(j.Program)

	programAsServerType := sysbench_runner.ServerType(j.Program)
	switch programAsServerType {
	case sysbench_runner.MySql:
		if j.SchemaPath == "" {
			return ErrMissingMysqlSchemaFile
		}

		if j.Format != csvExt {
			return ErrImproperMysqlFileFormat
		}
	case sysbench_runner.Dolt:
	default:
		return ErrUnsupportedProgram
	}

	j.Format = strings.ToLower(j.Format)

	seen := false
	for _, f := range supportedFormats {
		if f == j.Format {
			seen = true
		}
	}
	if !seen {
		return ErrUnsupportedFileFormat
	}

	return nil
}

func getMysqlConfigFromConfig(c *ImportBenchmarkConfig) sysbench_runner.MysqlConfig {
	return sysbench_runner.MysqlConfig{Socket: defaultSocket, Host: c.MysqlHost, ConnectionProtocol: c.MysqlConnectionProtocol, Port: c.MysqlPort}
}

// generateTestFilesIfNeeded creates the test conditions for an import benchmark to execute. In the case that the config
// dictates that data needs to be generated, this function handles that
func generateTestFilesIfNeeded(config *ImportBenchmarkConfig) *ImportBenchmarkConfig {
	jobs := make([]*ImportBenchmarkJob, 0)

	for _, job := range config.Jobs {
		// Preset csv path
		if job.Filepath != "" {
			jobs = append(jobs, job)
		} else {
			filePath, fileFormat := generateTestFile(job)

			job.Filepath = filePath
			job.Format = fileFormat

			jobs = append(jobs, job)
		}
	}

	config.Jobs = jobs
	return config
}

// generateTestFile is used to create a generated test case with a randomly generated csv file.
func generateTestFile(job *ImportBenchmarkJob) (string, string) {
	sch := NewSeedSchema(job.NumRows, genSampleCols(), job.Format)

	pathToImportFile := filepath.Join(GetWorkingDir(), fmt.Sprintf("testData.%s", sch.FileFormatExt))
	wc, err := filesys.LocalFS.OpenForWrite(pathToImportFile, os.ModePerm)
	if err != nil {
		log.Fatalf(err.Error())
	}

	defer wc.Close()

	ds := NewDSImpl(wc, sch, seedRandom, testTable)
	ds.GenerateData()

	return pathToImportFile, sch.FileFormatExt
}

func RunBenchmarkTests(config *ImportBenchmarkConfig, workingDir string) []result {
	config = generateTestFilesIfNeeded(config)

	// Split into the two jobs because we want
	doltJobs := make([]*ImportBenchmarkJob, 0)
	mySQLJobs := make([]*ImportBenchmarkJob, 0)

	for _, job := range config.Jobs {
		switch strings.ToLower(job.Program) {
		case "dolt":
			doltJobs = append(doltJobs, job)
		case "mysql":
			if job.Format != csvExt {
				log.Fatal("mysql import benchmarking only supports csv files")
			}
			mySQLJobs = append(mySQLJobs, job)
		default:
			log.Fatal("error: Invalid program. Must use dolt or mysql. See the sample config")
		}
	}

	results := make([]result, 0)
	for _, doltJob := range doltJobs {
		results = append(results, BenchmarkDoltImportJob(doltJob, workingDir, config.NbfVersion))
	}

	results = append(results, BenchmarkMySQLImportJobs(mySQLJobs, getMysqlConfigFromConfig(config))...)

	return results
}
