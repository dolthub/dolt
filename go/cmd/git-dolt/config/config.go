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

// Package config contains types and functions for dealing with
// git-dolt configuration, including config file I/O.
package config

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/liquidata-inc/dolt/go/cmd/git-dolt/env"
	"github.com/liquidata-inc/dolt/go/cmd/git-dolt/utils"
)

// GitDoltConfig represents the configuration for a git-dolt integration.
type GitDoltConfig struct {
	// Version is the version of the git-dolt protocol being used.
	Version string
	// Remote is the url of the dolt remote.
	Remote string
	// Revision is the revision of the remote that this git-dolt pointer links to.
	Revision string
}

// Parse parses a git-dolt config string into a struct.
func Parse(c string) (GitDoltConfig, error) {
	lines := strings.Split(c, "\n")
	config := make(map[string]string)

	for _, line := range lines {
		setting := strings.Split(line, " ")
		if len(setting) == 2 {
			config[setting[0]] = setting[1]
		}
	}

	// default to the current version of git-dolt
	if config["version"] == "" {
		config["version"] = env.Version
	}

	requiredProps := []string{"remote", "revision"}

	for _, prop := range requiredProps {
		if _, ok := config[prop]; !ok {
			return GitDoltConfig{}, fmt.Errorf("no %s specified", prop)
		}
	}

	return GitDoltConfig{
		Version:  config["version"],
		Remote:   config["remote"],
		Revision: config["revision"],
	}, nil
}

// Load loads a GitDoltConfig from the pointer file with the given filename.
func Load(ptrFname string) (GitDoltConfig, error) {
	ptrFname = utils.EnsureSuffix(ptrFname, ".git-dolt")
	ptrData, err := ioutil.ReadFile(ptrFname)
	if err != nil {
		return GitDoltConfig{}, fmt.Errorf("can't find pointer file %s", ptrFname)
	}

	config, err := Parse(string(ptrData))
	if err != nil {
		return GitDoltConfig{}, fmt.Errorf("error parsing config file: %v", err)
	}

	return config, nil
}

// Write writes to the pointer file with the given filename,
// creating or overwriting it with the given contents.
func Write(ptrFname string, ptrContents string) error {
	ptrFname = utils.EnsureSuffix(ptrFname, ".git-dolt")
	if err := ioutil.WriteFile(ptrFname, []byte(ptrContents), 0644); err != nil {
		return fmt.Errorf("error writing git-dolt pointer file at %s: %v", ptrFname, err)
	}

	return nil
}

func (c GitDoltConfig) String() string {
	return fmt.Sprintf("version %s\nremote %s\nrevision %s\n", c.Version, c.Remote, c.Revision)
}
