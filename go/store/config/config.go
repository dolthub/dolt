// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"

	"github.com/dolthub/dolt/go/store/spec"
)

// All configuration
type Config struct {
	File string
	Db   map[string]DbConfig
	AWS  AWSConfig
}

// Configuration for a specific database
type DbConfig struct {
	Url     string
	Options map[string]string
}

// Global AWS Config
type AWSConfig struct {
	Region     string
	CredSource string `toml:"cred_source"`
	CredFile   string `toml:"cred_file"`
}

const (
	NomsConfigFile = ".nomsconfig"
	DefaultDbAlias = "default"

	awsRegionParam     = "aws_region"
	awsCredSourceParam = "aws_cred_source"
	awsCredFileParam   = "aws_cred_file"
	authParam          = "authorization"
)

var ErrNoConfig = fmt.Errorf("no %s found", NomsConfigFile)

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
			return nil, ErrNoConfig
		}
		curDir = nextDir
	}
}

func ReadConfig(name string) (*Config, error) {
	data, err := os.ReadFile(name)
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
	if err := os.WriteFile(file, []byte(c.writeableString()), os.ModePerm); err != nil {
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
		qc.Db[k] = DbConfig{absDbSpec(dir, r.Url), r.Options}
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

		for optKey, optVal := range r.Options {
			buffer.WriteString(fmt.Sprintf("\t[db.%s.options]\n", k))
			buffer.WriteString(fmt.Sprintf("\t\t%s = \"%s\"\n", optKey, optVal))
		}
	}

	buffer.WriteString("[aws]\n")

	if c.AWS.Region != "" {
		buffer.WriteString(fmt.Sprintf("\tregion = \"%s\"\n", c.AWS.Region))
	}

	if c.AWS.CredSource != "" {
		buffer.WriteString(fmt.Sprintf("\tcred_source = \"%s\"\n", c.AWS.CredSource))
	}

	if c.AWS.CredFile != "" {
		buffer.WriteString(fmt.Sprintf("\tcred_file = \"%s\"\n", c.AWS.CredFile))
	}

	return buffer.String()
}

func (c *Config) getAWSRegion(dbParams map[string]string) string {
	if dbParams != nil {
		if val, ok := dbParams[awsRegionParam]; ok {
			return val
		}
	}

	if c.AWS.Region != "" {
		return c.AWS.Region
	}

	return ""
}

func (c *Config) getAuthorization(dbParams map[string]string) string {
	if dbParams != nil {
		if val, ok := dbParams[authParam]; ok {
			return val
		}
	}

	return ""
}

func (c *Config) getAWSCredentialSource(dbParams map[string]string) spec.AWSCredentialSource {
	set := false
	credSourceStr := ""
	if dbParams != nil {
		if val, ok := dbParams[awsCredSourceParam]; ok {
			set = true
			credSourceStr = val
		}
	}

	if !set {
		credSourceStr = c.AWS.CredSource
	}

	ct := spec.AWSCredentialSourceFromStr(credSourceStr)

	if ct == spec.InvalidCS {
		panic(credSourceStr + " is not a valid aws credential source")
	}

	return ct
}

func (c *Config) getAWSCredFile(dbParams map[string]string) string {
	if dbParams != nil {
		if val, ok := dbParams[awsCredFileParam]; ok {
			return val
		}
	}

	return ""
}

// specOptsForConfig Uses config data from the global config and db configuration to
// generate the spec.SpecOptions which should be used in calls to spec.For*opts()
func specOptsForConfig(c *Config, dbc *DbConfig) spec.SpecOptions {
	dbParams := dbc.Options

	if c == nil {
		return spec.SpecOptions{}
	} else {
		return spec.SpecOptions{
			Authorization: c.getAuthorization(dbParams),
			AWSRegion:     c.getAWSRegion(dbParams),
			AWSCredSource: c.getAWSCredentialSource(dbParams),
			AWSCredFile:   c.getAWSCredFile(dbParams),
		}
	}
}
