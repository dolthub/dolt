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
	"testing"

	"gopkg.in/yaml.v3"
)

func TestConditionMatches(t *testing.T) {
	cases := []struct {
		name  string
		cond  *Condition
		os    string
		event string
		want  bool
	}{
		{"nil condition matches anything", nil, "ubuntu-22.04", "push", true},
		{"empty condition matches anything", &Condition{}, "ubuntu-22.04", "push", true},
		{"os match", &Condition{OS: []string{"ubuntu-22.04"}}, "ubuntu-22.04", "push", true},
		{"os miss", &Condition{OS: []string{"ubuntu-22.04"}}, "macos-latest", "push", false},
		{"event match", &Condition{Events: []string{"push"}}, "ubuntu-22.04", "push", true},
		{"event miss", &Condition{Events: []string{"push"}}, "ubuntu-22.04", "pull_request", false},
		{
			"both match",
			&Condition{OS: []string{"ubuntu-22.04"}, Events: []string{"push", "workflow_dispatch"}},
			"ubuntu-22.04", "push", true,
		},
		{
			"os match, event miss",
			&Condition{OS: []string{"ubuntu-22.04"}, Events: []string{"push"}},
			"ubuntu-22.04", "pull_request", false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.cond.Matches(c.os, c.event); got != c.want {
				t.Fatalf("Matches(%q, %q) = %v, want %v", c.os, c.event, got, c.want)
			}
		})
	}
}

func TestListCombos(t *testing.T) {
	cfg := &Config{
		DefaultOS: []string{"macos-latest", "ubuntu-22.04", "windows-latest"},
		Shards: []Shard{
			{Name: "all", Packages: []string{"./..."}},
			{Name: "ubuntu_only", Packages: []string{"./..."}, RunsOn: &Condition{OS: []string{"ubuntu-22.04"}}},
		},
	}
	combos := cfg.ListCombos()
	want := []Combo{
		{OS: "macos-latest", Shard: "all"},
		{OS: "ubuntu-22.04", Shard: "all"},
		{OS: "windows-latest", Shard: "all"},
		{OS: "ubuntu-22.04", Shard: "ubuntu_only"},
	}
	if len(combos) != len(want) {
		t.Fatalf("got %d combos, want %d: %+v", len(combos), len(want), combos)
	}
	for i, c := range combos {
		if c != want[i] {
			t.Errorf("combo[%d] = %+v, want %+v", i, c, want[i])
		}
	}
}

func TestResolveRace(t *testing.T) {
	s := Shard{
		Name:   "x",
		RaceOn: &Condition{OS: []string{"ubuntu-22.04"}, Events: []string{"push"}},
	}
	if !s.ResolveRace("ubuntu-22.04", "push") {
		t.Error("expected race on ubuntu push")
	}
	if s.ResolveRace("ubuntu-22.04", "pull_request") {
		t.Error("expected no race on ubuntu pull_request")
	}
	if s.ResolveRace("macos-latest", "push") {
		t.Error("expected no race on macos push")
	}

	noRace := Shard{Name: "no_race"}
	if noRace.ResolveRace("ubuntu-22.04", "push") {
		t.Error("expected no race when RaceOn is nil")
	}
}

func TestEmbeddedConfigParses(t *testing.T) {
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.Shards) == 0 {
		t.Fatal("embedded config has no shards")
	}
	if len(cfg.DefaultOS) == 0 {
		t.Fatal("embedded config has no default_os")
	}
	// Sanity: every shard has a name and at least one package.
	for _, s := range cfg.Shards {
		if s.Name == "" {
			t.Error("shard with empty name")
		}
		if len(s.Packages) == 0 {
			t.Errorf("shard %q has no packages", s.Name)
		}
	}
}

func TestLoadConfigRejectsDuplicateShardNames(t *testing.T) {
	bad := []byte(`
default_os: [ubuntu-22.04]
shards:
  - name: x
    packages: [./a]
  - name: x
    packages: [./b]
`)
	var cfg Config
	if err := yaml.Unmarshal(bad, &cfg); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	// Re-route through LoadConfig path by swapping configBytes.
	old := configBytes
	configBytes = bad
	defer func() { configBytes = old }()
	_, err := LoadConfig()
	if err == nil {
		t.Fatal("expected duplicate-name error")
	}
}
