package fsrepo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ipfs/go-ipfs/repo/config"
	"gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
	"gx/ipfs/QmdYwCmx8pZRkzdcd8MhmLJqYVoVTC1aGsy5Q4reMGLNLg/atomicfile"
)

// ReadConfigFile reads the config from `filename` into `cfg`.
func ReadConfigFile(filename string, cfg interface{}) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(cfg); err != nil {
		return fmt.Errorf("Failure to decode config: %s", err)
	}
	return nil
}

// WriteConfigFile writes the config from `cfg` into `filename`.
func WriteConfigFile(filename string, cfg interface{}) error {
	err := os.MkdirAll(filepath.Dir(filename), 0775)
	if err != nil {
		return err
	}

	f, err := atomicfile.New(filename, 0660)
	if err != nil {
		return err
	}
	defer f.Close()

	return encode(f, cfg)
}

// encode configuration with JSON
func encode(w io.Writer, value interface{}) error {
	// need to prettyprint, hence MarshalIndent, instead of Encoder
	buf, err := config.Marshal(value)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// Load reads given file and returns the read config, or error.
func Load(filename string) (*config.Config, error) {
	// if nothing is there, fail. User must run 'ipfs init'
	if !util.FileExists(filename) {
		return nil, errors.New("ipfs not initialized, please run 'ipfs init'")
	}

	var cfg config.Config
	err := ReadConfigFile(filename, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, err
}
