// Copyright 2019 Liquidata, Inc.
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

package config

import (
	"encoding/json"
	"errors"
	"path/filepath"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
)

// FileConfig is backed by a file in the filesystem.
type FileConfig struct {
	// The path of the config file in the filesystem
	Path string

	fs         filesys.ReadWriteFS
	properties map[string]string
}

// NewFileConfig creates a new empty config and writes it to a newly created file.  If a file already exists at this
// location it will be overwritten. If a directory does not exist where this file should live, it will be created.
func NewFileConfig(path string, fs filesys.ReadWriteFS, properties map[string]string) (*FileConfig, error) {
	dir := filepath.Dir(path)
	err := fs.MkDirs(dir)

	if err != nil {
		return nil, err
	}

	fc := &FileConfig{path, fs, properties}
	err = fc.write()

	if err != nil {
		return nil, err
	}

	return fc, err
}

// FromFile reads configuration from a file on the given filesystem.  Calls to SetStrings will result in this file
// being updated.
func FromFile(path string, fs filesys.ReadWriteFS) (*FileConfig, error) {
	data, err := fs.ReadFile(path)

	if err != nil {
		return nil, err
	}

	properties := make(map[string]string)
	err = json.Unmarshal(data, &properties)

	if err != nil {
		return nil, err
	}

	return &FileConfig{path, fs, properties}, nil
}

// GetString retrieves a string from the cached config state
func (fc *FileConfig) GetString(k string) (string, error) {
	if val, ok := fc.properties[k]; ok {
		return val, nil
	}

	return "", ErrConfigParamNotFound
}

// SetStrings will set the value of configuration parameters in memory, and persist any changes to the backing file.
func (fc *FileConfig) SetStrings(updates map[string]string) error {
	modified := false
	for k, v := range updates {
		if val, ok := fc.properties[k]; !ok || val != v {
			fc.properties[k] = v
			modified = true
		}
	}

	if modified == false {
		return nil
	}

	return fc.write()
}

// Iter will perform a callback for ech value in a config until all values have been exhausted or until the
// callback returns true indicating that it should stop.
func (fc *FileConfig) Iter(cb func(string, string) (stop bool)) {
	for k, v := range fc.properties {
		stop := cb(k, v)

		if stop {
			break
		}
	}
}

func (fc *FileConfig) write() error {
	data, err := json.Marshal(fc.properties)

	if err != nil {
		return err
	}

	return fc.fs.WriteFile(fc.Path, data)
}

// Unset removes a configuration parameter from the config
func (fc *FileConfig) Unset(params []string) error {
	for _, param := range params {
		if _, ok := fc.properties[param]; !ok {
			return errors.New("key does not exist on this configuration")
		}
		delete(fc.properties, param)

	}

	return fc.write()
}

// Size returns the number of properties contained within the config
func (fc *FileConfig) Size() int {
	return len(fc.properties)
}
