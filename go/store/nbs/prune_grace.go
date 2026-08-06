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

package nbs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/hash"
)

// pruneProbePrefix names the zero-length file a grace prune drops in a
// directory in order to learn what time that directory's filesystem thinks
// it is. See pruneDirWithGrace.
const pruneProbePrefix = ".dolt_prune_probe_"

// ErrGracePruneUnsupported is returned by PruneUnreferencedWithGrace for
// stores that are not plain local-file stores. Journaled stores hold an
// exclusive cross-process lock and have no need for a quiescence heuristic;
// blobstore and AWS stores are not directories.
var ErrGracePruneUnsupported = errors.New("store does not support pruning unreferenced table files with a grace period")

// GracePruner is implemented by chunk stores that can reclaim table files
// which no manifest references, without holding a cross-process lock against
// concurrent writers.
//
// This exists for backup destinations. A file:// backup destination is opened
// as a bare local store: it has no chunk journal, and therefore no exclusive
// write lock that would let a pruner know whether another process is midway
// through a sync. An interrupted sync leaves complete, unreferenced table
// files behind, and nothing else reclaims them.
//
// Safety comes from directory-wide quiescence rather than from locking: see
// pruneDirWithGrace. Callers must size |grace| well above the window in which
// a writer can hold a reference to a file it is no longer touching — see
// [github.com/dolthub/dolt/go/cmd/dolt/cli.BackupPruneMinGracePeriod].
type GracePruner interface {
	PruneUnreferencedWithGrace(ctx context.Context, grace time.Duration) (PruneStats, error)
}

// test hook: called after the candidate snapshot is taken and before any
// unlink, so a test can simulate a writer that starts a sync in that window.
var _testPruneAfterSnapshotHook func()

var _ GracePruner = &NomsBlockStore{}
var _ GracePruner = &GenerationalNBS{}
var _ GracePruner = &NBSMetricWrapper{}

// PruneStats reports the outcome of one or more PruneUnreferencedWithGrace
// passes.
type PruneStats struct {
	FilesDeleted   int
	BytesReclaimed int64
	// Skipped holds one message per directory that declined to prune, either
	// because it was not quiescent or because its clock could not be trusted.
	// A skip is a normal outcome, not an error.
	Skipped []string
}

func (s *PruneStats) add(o PruneStats) {
	s.FilesDeleted += o.FilesDeleted
	s.BytesReclaimed += o.BytesReclaimed
	s.Skipped = append(s.Skipped, o.Skipped...)
}

// String renders PruneStats as a single line suitable for logging.
func (s PruneStats) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "deleted %d file(s), reclaimed %d bytes", s.FilesDeleted, s.BytesReclaimed)
	for _, skip := range s.Skipped {
		b.WriteString("; skipped ")
		b.WriteString(skip)
	}
	return b.String()
}

// pruneDirWithGrace deletes table files in |dir| that |keep| does not vouch
// for, along with abandoned temp files, but only if nothing in |dir| has been
// modified within |grace| of now.
//
// The quiescence test is the whole safety argument. |dir| may be written
// concurrently by another process holding no lock we can see, so we cannot ask
// "is this file unreferenced?" — a writer that has renamed a table file into
// place but not yet committed its manifest would answer yes. We ask instead
// "has anything in this directory been touched recently?" A writer mid-sync is
// continuously touching something: a temp file whose mtime advances as bytes
// land, or a fresh rename. Nothing recent means no sync is in flight, and
// everything unreferenced is garbage. This needs no cooperation from the
// writer, so it holds across dolt versions.
//
// The residual hole is a writer that has landed its last table file and is
// sitting in index load, ref checks and the manifest write with no directory
// activity. That tail is bounded — seconds to minutes — and is what |grace| is
// sized against.
//
// One recent mtime vetoes the entire pass. Pruning the rest anyway would
// reintroduce the per-file age reasoning this design exists to avoid: the
// first file of a long sync is separated from that sync's manifest commit by
// the whole remaining transfer, so no per-file age threshold is safe.
//
// Time comparisons are made against the mtime of a probe file we create in
// |dir| rather than against the local clock. On NFS and SMB the server stamps
// mtimes, so a pruning host whose clock runs ahead of the file server would
// see every file as older than it is and prune too eagerly. Taking the cutoff
// from a file the server just stamped puts both sides of every comparison in
// the same time base, and constant drift cancels.
func pruneDirWithGrace(ctx context.Context, dir string, keep func(hash.Hash) bool, grace time.Duration) (stats PruneStats, err error) {
	probePath, probeMtime, err := dropPruneProbe(dir)
	if err != nil {
		return PruneStats{}, err
	}
	defer func() {
		rmErr := file.Remove(probePath)
		if err == nil && rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			err = fmt.Errorf("error removing prune probe %s: %w", probePath, rmErr)
		}
	}()

	return pruneDirAsOf(ctx, dir, keep, grace, probeMtime, time.Now())
}

// pruneDirAsOf is pruneDirWithGrace with the two clock readings supplied by the
// caller: |probeMtime| as the directory filesystem reported it, and |now| as
// the local clock reported it.
func pruneDirAsOf(ctx context.Context, dir string, keep func(hash.Hash) bool, grace time.Duration, probeMtime, now time.Time) (stats PruneStats, err error) {
	// If the directory's clock and ours disagree by more than the grace
	// period, refuse rather than delete on the strength of mtimes we cannot
	// interpret. A grossly misconfigured clock should produce a message, not
	// a silent deletion.
	if skew := probeMtime.Sub(now); skew > grace || skew < -grace {
		stats.Skipped = append(stats.Skipped, fmt.Sprintf(
			"%s: filesystem clock differs from local clock by %s, more than the %s grace period",
			dir, skew.Round(time.Second), grace))
		return stats, nil
	}

	// The probe was created before the scan below, so the cutoff is slightly
	// early with respect to the files we are about to stat. That errs toward
	// pruning less.
	cutoff := probeMtime.Add(-grace)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return stats, err
	}

	// A candidate is a file we may delete. This snapshot is the only set of
	// deletion candidates: a file that appears in |dir| after this scan is
	// never touched, however it is named.
	type candidate struct {
		name    string
		modTime time.Time
		size    int64
	}
	var candidates []candidate
	var staleProbes []string
	var newest time.Time
	var newestName string

	for _, e := range entries {
		// Subdirectories — notably oldgen/ — belong to a different manifest
		// and are pruned, if at all, by their own pass.
		if e.IsDir() {
			continue
		}
		name := e.Name()
		info, err := e.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return stats, err
		}

		if strings.HasPrefix(name, pruneProbePrefix) {
			// Probes are dropped by pruners, not by writers, so a fresh one
			// is not evidence of a sync in flight and must not veto the pass.
			// Sweep the ones left behind by a pruner that died.
			if info.ModTime().Before(cutoff) {
				staleProbes = append(staleProbes, name)
			}
			continue
		}

		// Every entry counts toward quiescence, referenced or not. The point
		// is to detect activity, and a writer's most recent touch may well
		// have been to a file we would otherwise keep.
		if mt := info.ModTime(); mt.After(newest) {
			newest, newestName = mt, name
		}

		if isPruneDebris(name, keep) {
			candidates = append(candidates, candidate{name: name, modTime: info.ModTime(), size: info.Size()})
		}
	}

	if _testPruneAfterSnapshotHook != nil {
		_testPruneAfterSnapshotHook()
	}

	if newest.After(cutoff) {
		// A file can be newer than the probe if a writer touched it while we
		// were scanning, which reads as a negative age. Report that as 0.
		age := max(probeMtime.Sub(newest).Round(time.Second), 0)
		stats.Skipped = append(stats.Skipped, fmt.Sprintf(
			"%s: not quiescent, %q was modified %s ago (grace period %s)",
			dir, newestName, age, grace))
		return stats, nil
	}

	var errs []error
	for _, c := range candidates {
		if err := ctx.Err(); err != nil {
			return stats, errors.Join(append(errs, err)...)
		}

		p := filepath.Join(dir, c.name)

		// Re-stat immediately before the unlink. A writer that started a sync
		// after the quiescence check passed can rename a live file over a
		// name in our snapshot; deleting it would corrupt the destination.
		// POSIX has no atomic unlink-if-unchanged, so this narrows the window
		// to the stat/unlink gap rather than closing it. Reaching it requires
		// a new sync to land a content-identical table file — identical name,
		// so identical chunks — inside that gap.
		info, err := os.Stat(p)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, fmt.Errorf("error stat'ing %s before prune: %w", p, err))
			}
			continue
		}
		if !info.ModTime().Equal(c.modTime) || info.Size() != c.size {
			continue
		}

		if err := file.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("error removing unreferenced file %s: %w", p, err))
			continue
		}
		stats.FilesDeleted++
		stats.BytesReclaimed += c.size
	}

	for _, name := range staleProbes {
		p := filepath.Join(dir, name)
		if err := file.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("error removing stale prune probe %s: %w", p, err))
		}
	}

	return stats, errors.Join(errs...)
}

// isPruneDebris reports whether |name| may be deleted by a grace prune: an
// abandoned temp file, or a table file that |keep| does not vouch for.
//
// Anything we do not positively recognize is left alone. In particular the
// manifest, the LOCK file and a chunk journal are never candidates, so
// pointing a prune at a directory it does not fully understand costs disk
// rather than data.
func isPruneDebris(name string, keep func(hash.Hash) bool) bool {
	if strings.HasPrefix(name, tempTablePrefix) || strings.HasPrefix(name, tempManifestPrefix) {
		return true
	}
	// A chunk journal's name is a valid table file address, so it would
	// otherwise classify as a table file. A journaled store never reaches this
	// code — it uses a different persister, and a bare local store refuses to
	// open a directory holding a journal at all — but deleting one would be
	// catastrophic, so say so explicitly.
	if name == chunkJournalName {
		return false
	}
	h, ok := fileNameToAddr(name)
	if !ok {
		return false
	}
	return !keep(h)
}

// dropPruneProbe creates and stats a probe file in |dir|, returning its path
// and the mtime the filesystem stamped it with.
func dropPruneProbe(dir string) (string, time.Time, error) {
	f, err := os.CreateTemp(dir, pruneProbePrefix+"*")
	if err != nil {
		return "", time.Time{}, err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		_ = file.Remove(path)
		return "", time.Time{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		_ = file.Remove(path)
		return "", time.Time{}, err
	}
	return path, info.ModTime(), nil
}
