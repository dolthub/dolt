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

// cigotests is the test runner used by .github/workflows/ci-go-tests.yaml.
//
// It reads config.yaml (embedded at build time) and supports two
// subcommands:
//
//	list
//	  Prints a JSON array of {"os": "...", "shard": "..."} entries to
//	  stdout, one per matrix cell the workflow should run.
//
//	run --shard <name> --os <os> --event <github-event>
//	  Resolves the shard's package list, decides whether to enable -race
//	  based on the (os, event) pair, sets per-shard env vars, and execs
//	  `go test`. Cwd must be the Go module root (./go in this repo).
//
// Editing the matrix shape is a config-only operation: edit config.yaml
// and the workflow re-discovers shards on the next run.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "list":
		cmdList()
	case "plan":
		cmdPlan(os.Args[2:])
	case "run":
		cmdRun(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  cigotests list")
	fmt.Fprintln(os.Stderr, "  cigotests plan --shard <name> --os <os> --event <event>")
	fmt.Fprintln(os.Stderr, "  cigotests run  --shard <name> --os <os> --event <event>")
}

func cmdList() {
	cfg, err := LoadConfig()
	if err != nil {
		die("load config: %v", err)
	}
	combos := cfg.ListCombos()
	out, err := json.Marshal(combos)
	if err != nil {
		die("marshal combos: %v", err)
	}
	fmt.Println(string(out))
}

// resolvedPlan is the materialized go-test invocation for a (shard, os,
// event) triple: the resolved package list, the final go-test args, and
// the env additions that should be layered on os.Environ().
type resolvedPlan struct {
	Shard   *Shard
	OSName  string
	Event   string
	RaceOn  bool
	Pkgs    []string
	Args    []string
	EnvAdds []string
}

func resolveShardArgs(args []string) (*resolvedPlan, error) {
	fs := flag.NewFlagSet("", flag.ExitOnError)
	shardName := fs.String("shard", "", "shard name (matches a name in config.yaml)")
	osName := fs.String("os", "", "matrix os value, e.g. ubuntu-22.04")
	eventName := fs.String("event", "", "github event name, e.g. pull_request, push")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if *shardName == "" || *osName == "" || *eventName == "" {
		fs.Usage()
		return nil, fmt.Errorf("--shard, --os, and --event are required")
	}

	cfg, err := LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	shard := cfg.FindShard(*shardName)
	if shard == nil {
		return nil, fmt.Errorf("unknown shard %q", *shardName)
	}

	pkgs, err := shard.ResolvePackages()
	if err != nil {
		return nil, fmt.Errorf("resolve packages: %w", err)
	}
	if len(pkgs) == 0 {
		return nil, fmt.Errorf("shard %q resolved to zero packages", shard.Name)
	}

	raceOn := shard.ResolveRace(*osName, *eventName)

	testArgs := []string{"test", "-vet=off"}
	if shard.Timeout != "" {
		testArgs = append(testArgs, "-timeout", shard.Timeout)
	}
	if raceOn {
		testArgs = append(testArgs, "-race")
	}
	testArgs = append(testArgs, pkgs...)

	var envAdds []string
	for k, v := range shard.Env {
		envAdds = append(envAdds, k+"="+v)
	}
	if raceOn {
		for k, v := range shard.EnvWithRace {
			envAdds = append(envAdds, k+"="+v)
		}
	}

	return &resolvedPlan{
		Shard: shard, OSName: *osName, Event: *eventName,
		RaceOn: raceOn, Pkgs: pkgs, Args: testArgs, EnvAdds: envAdds,
	}, nil
}

func (p *resolvedPlan) describe(w *os.File) {
	fmt.Fprintf(w, "shard=%s os=%s event=%s race=%v packages=%d\n",
		p.Shard.Name, p.OSName, p.Event, p.RaceOn, len(p.Pkgs))
	for _, e := range p.EnvAdds {
		fmt.Fprintf(w, "env: %s\n", e)
	}
	fmt.Fprintf(w, "+ go %s\n", strings.Join(p.Args, " "))
}

func cmdPlan(args []string) {
	plan, err := resolveShardArgs(args)
	if err != nil {
		die("%v", err)
	}
	plan.describe(os.Stdout)
}

func cmdRun(args []string) {
	plan, err := resolveShardArgs(args)
	if err != nil {
		die("%v", err)
	}
	plan.describe(os.Stderr)

	cmd := exec.Command("go", plan.Args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), plan.EnvAdds...)
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
