// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/sirupsen/logrus"
)

const (
	// doltGoMemLimitEnv is the environment variable for explicitly setting GOMEMLIMIT.
	// Accepts byte values (e.g. "1073741824") or human-readable values (e.g. "1GiB", "512MiB").
	doltGoMemLimitEnv = "DOLT_GOMEMLIMIT"

	// defaultMemLimitRatio is the fraction of the cgroup memory limit to use as GOMEMLIMIT.
	// 0.9 is the standard recommendation, matching CockroachDB and other Go services.
	defaultMemLimitRatio = 0.9
)

// configureMemoryLimit sets GOMEMLIMIT based on the environment:
//  1. If DOLT_GOMEMLIMIT is set, use that value explicitly.
//  2. Otherwise, auto-detect cgroup memory limits and set GOMEMLIMIT to 90% of the limit.
//  3. If no cgroup limit is detected (bare metal), GOMEMLIMIT remains unset.
func configureMemoryLimit() {
	if v := os.Getenv(doltGoMemLimitEnv); v != "" {
		limit, err := parseMemoryValue(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: invalid %s value %q: %v\n", doltGoMemLimitEnv, v, err)
			return
		}
		prev := debug.SetMemoryLimit(limit)
		logrus.Infof("GOMEMLIMIT set to %d bytes via %s (previous: %d)", limit, doltGoMemLimitEnv, prev)
		return
	}

	// Auto-detect cgroup memory limit. On bare metal or when no cgroup limit is set,
	// this returns an error and GOMEMLIMIT stays at its default (math.MaxInt64).
	limit, err := memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(defaultMemLimitRatio),
		memlimit.WithLogger(nil), // suppress automemlimit's own logging
	)
	if err != nil {
		// Not in a cgroup or no memory limit set — this is normal on bare metal.
		logrus.Debugf("automemlimit: no cgroup memory limit detected: %v", err)
		return
	}
	if limit > 0 {
		logrus.Infof("GOMEMLIMIT auto-configured to %d bytes (%.0f%% of cgroup limit)", limit, defaultMemLimitRatio*100)
	}
}

// parseMemoryValue parses a memory value string. Accepts plain byte counts or
// human-readable suffixes: B, KiB, MiB, GiB, TiB (case-insensitive).
func parseMemoryValue(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty value")
	}

	// Try plain integer first
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n <= 0 {
			return 0, fmt.Errorf("value must be positive")
		}
		return n, nil
	}

	upper := strings.ToUpper(s)
	suffixes := []struct {
		suffix string
		mult   float64
	}{
		{"TIB", 1 << 40},
		{"GIB", 1 << 30},
		{"MIB", 1 << 20},
		{"KIB", 1 << 10},
		{"TB", 1e12},
		{"GB", 1e9},
		{"MB", 1e6},
		{"KB", 1e3},
		{"B", 1},
	}

	for _, sf := range suffixes {
		if strings.HasSuffix(upper, sf.suffix) {
			numStr := strings.TrimSpace(s[:len(s)-len(sf.suffix)])
			n, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid number %q", numStr)
			}
			if n <= 0 {
				return 0, fmt.Errorf("value must be positive")
			}
			result := int64(math.Round(n * sf.mult))
			if result <= 0 {
				return 0, fmt.Errorf("value too small")
			}
			return result, nil
		}
	}

	return 0, fmt.Errorf("unrecognized format %q; use bytes or suffix like MiB, GiB", s)
}
