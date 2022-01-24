// Copyright 2019-2022 Dolthub, Inc.
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

package sysbench_runner

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type report struct {
	Results Results
}

// WriteResultsJson writes Results to a json file
func WriteResultsJson(filename string, results Results) (err error) {
	dir := filepath.Dir(filename)
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
	}
	var file *os.File
	file, err = os.Create(filename)
	if err != nil {
		return
	}
	defer func() {
		closeErr := file.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	d := report{
		Results: results,
	}

	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	_, err = file.Write(b)
	if err != nil {
		return err
	}

	return
}

// ReadResultsJson reads a json file into Results
func ReadResultsJson(filename string) (Results, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var textInput io.Reader = file

	b, err := ioutil.ReadAll(textInput)
	if err != nil {
		return nil, err
	}

	r := report{}
	err = json.Unmarshal(b, &r)
	if err != nil {
		return nil, err
	}

	return r.Results, nil
}
