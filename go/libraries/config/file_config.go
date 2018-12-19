package config

import (
	"encoding/json"
	"path/filepath"

	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
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
		delete(fc.properties, param)
	}

	return fc.write()
}

// Size returns the number of properties contained within the config
func (fc *FileConfig) Size() int {
	return len(fc.properties)
}
