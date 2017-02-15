// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/attic-labs/noms/go/spec"
)

type Config struct {
	File string
	Db   map[string]DbConfig
}

type DbConfig struct {
	Url string
}

const (
	NomsConfigFile = ".nomsconfig"
	DefaultDbAlias = "default"
)

var NoConfig = errors.New(fmt.Sprintf("no %s found", NomsConfigFile))

// Find the closest directory containing .nomsconfig starting
// in cwd and then searching up ancestor tree.
// Look first looking in cwd and then up through its ancestors
func FindNomsConfig() (*Config, error) {
	curDir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	for {
		nomsConfig := filepath.Join(curDir, NomsConfigFile)
		info, err := os.Stat(nomsConfig)
		if err == nil && !info.IsDir() {
			// found
			return ReadConfig(nomsConfig)
		} else if err != nil && !os.IsNotExist(err) {
			// can't read
			return nil, err
		}
		nextDir := filepath.Dir(curDir)
		if nextDir == curDir {
			// stop at root
			return nil, NoConfig
		}
		curDir = nextDir
	}
}

func ReadConfig(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}
	c, err := NewConfig(string(data))
	if err != nil {
		return nil, err
	}
	c.File = name
	return qualifyPaths(name, c)
}

func NewConfig(data string) (*Config, error) {
	c := new(Config)
	if _, err := toml.Decode(data, c); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) WriteTo(configHome string) (string, error) {
	file := filepath.Join(configHome, NomsConfigFile)
	if err := os.MkdirAll(filepath.Dir(file), os.ModePerm); err != nil {
		return "", err
	}
	if err := ioutil.WriteFile(file, []byte(c.writeableString()), os.ModePerm); err != nil {
		return "", err
	}
	return file, nil
}

// Replace relative directory in path part of spec with an absolute
// directory. Assumes the path is relative to the location of the config file
func absDbSpec(configHome string, url string) string {
	dbSpec, err := spec.ForDatabase(url)
	if err != nil {
		return url
	}
	if dbSpec.Protocol != "nbs" {
		return url
	}
	dbName := dbSpec.DatabaseName
	if !filepath.IsAbs(dbName) {
		dbName = filepath.Join(configHome, dbName)
	}
	return "nbs:" + dbName
}

func qualifyPaths(configPath string, c *Config) (*Config, error) {
	file, err := filepath.Abs(configPath)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(file)
	qc := *c
	qc.File = file
	for k, r := range c.Db {
		qc.Db[k] = DbConfig{absDbSpec(dir, r.Url)}
	}
	return &qc, nil
}

func (c *Config) String() string {
	var buffer bytes.Buffer
	if c.File != "" {
		buffer.WriteString(fmt.Sprintf("file = %s\n", c.File))
	}
	buffer.WriteString(c.writeableString())
	return buffer.String()
}

func (c *Config) writeableString() string {
	var buffer bytes.Buffer
	for k, r := range c.Db {
		buffer.WriteString(fmt.Sprintf("[db.%s]\n", k))
		buffer.WriteString(fmt.Sprintf("\t"+`url = "%s"`+"\n", r.Url))
	}
	return buffer.String()
}
