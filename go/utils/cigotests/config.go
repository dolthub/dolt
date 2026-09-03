// Copyright 2026 Dolthub, Inc.
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
	_ "embed"
	"fmt"
	"os/exec"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed config.yaml
var configBytes []byte

type Config struct {
	DefaultOS []string `yaml:"default_os"`
	Shards    []Shard  `yaml:"shards"`
}

type Shard struct {
	Name        string            `yaml:"name"`
	Packages    []string          `yaml:"packages"`
	Exclude     []string          `yaml:"exclude,omitempty"`
	Timeout     string            `yaml:"timeout,omitempty"`
	Env         map[string]string `yaml:"env,omitempty"`
	EnvWithRace map[string]string `yaml:"env_with_race,omitempty"`
	RunsOn      *Condition        `yaml:"runs_on,omitempty"`
	RaceOn      *Condition        `yaml:"race_on,omitempty"`
}

// Condition is a predicate over (os, event). An empty list for either
// field means "any value." A nil Condition is true.
type Condition struct {
	OS     []string `yaml:"os,omitempty"`
	Events []string `yaml:"events,omitempty"`
}

// Combo is one cell of the GitHub Actions matrix.
type Combo struct {
	OS    string `json:"os"`
	Shard string `json:"shard"`
}

func LoadConfig() (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(configBytes, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if len(cfg.DefaultOS) == 0 {
		return nil, fmt.Errorf("config: default_os must be non-empty")
	}
	seen := map[string]bool{}
	for _, s := range cfg.Shards {
		if s.Name == "" {
			return nil, fmt.Errorf("config: shard with empty name")
		}
		if seen[s.Name] {
			return nil, fmt.Errorf("config: duplicate shard name %q", s.Name)
		}
		seen[s.Name] = true
		if len(s.Packages) == 0 {
			return nil, fmt.Errorf("config: shard %q has no packages", s.Name)
		}
	}
	return &cfg, nil
}

func (c *Config) FindShard(name string) *Shard {
	for i := range c.Shards {
		if c.Shards[i].Name == name {
			return &c.Shards[i]
		}
	}
	return nil
}

// ListCombos returns the {os, shard} cells the workflow should run.
// A shard's runs_on.os overrides default_os when specified.
func (c *Config) ListCombos() []Combo {
	var combos []Combo
	for _, s := range c.Shards {
		oses := c.DefaultOS
		if s.RunsOn != nil && len(s.RunsOn.OS) > 0 {
			oses = s.RunsOn.OS
		}
		for _, o := range oses {
			combos = append(combos, Combo{OS: o, Shard: s.Name})
		}
	}
	return combos
}

func (cond *Condition) Matches(osName, eventName string) bool {
	if cond == nil {
		return true
	}
	if len(cond.OS) > 0 && !slices.Contains(cond.OS, osName) {
		return false
	}
	if len(cond.Events) > 0 && !slices.Contains(cond.Events, eventName) {
		return false
	}
	return true
}

func (s *Shard) ResolveRace(osName, eventName string) bool {
	if s.RaceOn == nil {
		return false
	}
	return s.RaceOn.Matches(osName, eventName)
}

// ResolvePackages expands `./...` patterns and applies excludes by
// shelling out to `go list`. Cwd should be the Go module root.
func (s *Shard) ResolvePackages() ([]string, error) {
	if !needsExpansion(s.Packages) && len(s.Exclude) == 0 {
		return s.Packages, nil
	}
	includes, err := goList(s.Packages...)
	if err != nil {
		return nil, fmt.Errorf("expand includes: %w", err)
	}
	if len(s.Exclude) == 0 {
		return includes, nil
	}
	excludes, err := goList(s.Exclude...)
	if err != nil {
		return nil, fmt.Errorf("expand excludes: %w", err)
	}
	excludeSet := make(map[string]bool, len(excludes))
	for _, e := range excludes {
		excludeSet[e] = true
	}
	filtered := includes[:0]
	for _, p := range includes {
		if !excludeSet[p] {
			filtered = append(filtered, p)
		}
	}
	return filtered, nil
}

func needsExpansion(patterns []string) bool {
	for _, p := range patterns {
		if strings.Contains(p, "...") {
			return true
		}
	}
	return false
}

// goList runs `go list <pat>...` and returns the resolved package
// import paths, one per line.
func goList(pats ...string) ([]string, error) {
	args := append([]string{"list"}, pats...)
	cmd := exec.Command("go", args...)
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("go list failed: %s", strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, err
	}
	var lines []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, nil
}
