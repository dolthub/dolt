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

	"github.com/dolthub/fslock"

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
// which no manifest references, without holding an exclusive lock against
// concurrent writers for the duration.
//
// This exists for backup destinations. A file:// backup destination is opened
// as a bare local store: it has no chunk journal, and therefore no exclusive
// write lock that would let a pruner know whether another process is midway
// through a sync. An interrupted sync leaves complete, unreferenced table
// files behind, and nothing else reclaims them.
//
// Safety has two layers. Directory-wide quiescence decides whether to prune at
// all, and holds for writers of any vintage; see pruneDirWithGrace. The
// manifest lock then bounds the damage a wrong decision can do, by keeping the
// unlinks and a manifest update from overlapping; see unlinkCandidates.
// Callers must size |grace| well above the window in which a writer can hold a
// reference to a file it is no longer touching — see
// [github.com/dolthub/dolt/go/cmd/dolt/cli.BackupPruneMinGracePeriod].
type GracePruner interface {
	PruneUnreferencedWithGrace(ctx context.Context, grace time.Duration) (PruneStats, error)
}

// withLockedReferences runs |del| while holding the destination's manifest
// lock, passing the set of table files the manifest on disk references at that
// moment. Implementations of this are what tie a prune to a particular store's
// manifest; see NomsBlockStore.PruneUnreferencedWithGrace.
type withLockedReferences func(ctx context.Context, del func(referenced map[hash.Hash]struct{}) error) error

// withLockedKeep is withLockedReferences with the persister's own in-process
// protections folded in, leaving a single predicate that answers "must this
// table file survive?".
type withLockedKeep func(ctx context.Context, del func(keep func(hash.Hash) bool) error) error

// pruneUnlinkBatchSize bounds how many files are deleted per acquisition of
// the manifest lock. A concurrent manifest update gives up after
// lockFileTimeout — 100ms — and an unlink on a network filesystem is a
// round trip, so a long run of them under one acquisition would fail writers
// that have done nothing wrong. Between batches the lock is released and the
// manifest re-read.
const pruneUnlinkBatchSize = 16

// test hook: called after the candidate snapshot is taken and before the
// manifest lock is acquired, so a test can simulate a writer that starts a
// sync in that window.
var _testPruneAfterSnapshotHook func()

// test hook: called while the manifest lock is held, immediately before the
// first batch of unlinks.
var _testPruneUnderLockHook func()

var _ GracePruner = &NomsBlockStore{}
var _ GracePruner = &GenerationalNBS{}
var _ GracePruner = &NBSMetricWrapper{}

// PruneStats reports the outcome of one or more PruneUnreferencedWithGrace
// passes.
type PruneStats struct {
	FilesDeleted   int
	BytesReclaimed int64
	// Skipped holds one message per directory that stopped short: it was not
	// quiescent, its clock could not be trusted, or a writer took the manifest
	// lock or updated the manifest partway through. A skip is a normal
	// outcome, not an error, and may accompany files already deleted.
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

// pruneDirWithGrace deletes table files in |dir| that no manifest references,
// along with abandoned temp files, but only if nothing in |dir| has been
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
// activity. In the usually case, that period of time is bounded, and that
// bound is what |grace| is sized against.
//
// One recent mtime vetoes the entire pass. Pruning the rest anyway would
// reintroduce the per-file age reasoning this design exists to avoid: the
// first file of a long sync is separated from that sync's manifest commit by
// the whole remaining transfer, so no per-file age threshold is safe. The rule
// is all-or-nothing after deleting starts too — a candidate found changed on
// disk, or a manifest update landing partway through, abandons the rest of the
// pass rather than skipping the one file.
//
// Quiescence is a judgment about a writer we cannot see, so it can be wrong.
// |underLock| bounds what that costs: the unlinks run inside the destination's
// manifest lock, which no manifest update can be published outside of. See
// unlinkCandidates. A concurrent update with a recent Dolt version will then
// fail, since it will be attempting to add a reference to a file which no
// longer exists. In the case of arbitrary chunk writes through
// NomsBlockStore.memtable and accumulating dependencies through things that
// prolly tree writers, dropped writes are problematic. In the current case
// where we are pruning - push / backup sync - these are less problematic
// because there are no in-memory dependencies between writes.
//
// Time comparisons are made against the mtime of a probe file we create in
// |dir| rather than against the local clock. On NFS and SMB the server stamps
// mtimes, and a pruning host's clock can sit anywhere relative to it. Taking
// the cutoff from a file that filesystem just stamped puts both sides of every
// comparison in the same time base: any constant offset between the two clocks
// cancels, and the local clock does not enter the decision at all. It is read
// once, only to sanity-check the probe — see pruneDirAsOf.
func pruneDirWithGrace(ctx context.Context, dir string, grace time.Duration, underLock withLockedKeep) (stats PruneStats, err error) {
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

	return pruneDirAsOf(ctx, dir, grace, probeMtime, time.Now(), underLock)
}

// pruneDirAsOf is pruneDirWithGrace with the two clock readings supplied by the
// caller: |probeMtime| as the directory filesystem reported it, and |now| as
// the local clock reported it.
func pruneDirAsOf(ctx context.Context, dir string, grace time.Duration, probeMtime, now time.Time, underLock withLockedKeep) (stats PruneStats, err error) {
	// Sanity check the filesystem's mtime reporting. A directory that stamps a
	// file we created a moment ago with a time nothing like our own is a
	// directory whose metadata we do not understand, or a host whose clock
	// we cannot trust to tell us so. A prune is optimistic housekeeping that
	// nobody is waiting on; when things look weird, say so and leave it for the
	// next run.
	if skew := probeMtime.Sub(now); skew > grace || skew < -grace {
		stats.Skipped = append(stats.Skipped, fmt.Sprintf(
			"%s: a file created just now was stamped %s away from the local clock, more than the %s "+
				"grace period; not pruning on filesystem metadata this hard to interpret",
			dir, skew.Round(time.Second), grace))
		return stats, nil
	}

	cutoff := probeMtime.Add(-grace)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return stats, err
	}

	// This snapshot is the only set of deletion candidates: a file that appears
	// in |dir| after this scan is never touched.
	var candidates []pruneCandidate
	var staleProbes []string
	var newest time.Time
	var newestName string
	// Zero if there is no manifest yet.
	var manifestMtime time.Time

	for _, e := range entries {
		// Subdirectories — notably oldgen/ — would belong to a different manifest
		// and would be pruned, if at all, by their own pass.
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
			// is not evidence of an in-flight write. It doesn't veto the
			// prune. Sweep the ones left behind by a pruner that died.
			if info.ModTime().Before(cutoff) {
				staleProbes = append(staleProbes, name)
			}
			continue
		}

		// Every entry counts toward quiescence, referenced or not. The point
		// is to detect activity, and a writer's most recent touch may well
		// have been to a file which is now in the manifest.
		if mt := info.ModTime(); mt.After(newest) {
			newest, newestName = mt, name
		}

		if name == manifestFileName {
			manifestMtime = info.ModTime()
			continue
		}

		if addr, isTemp, ok := classifyPruneCandidate(name); ok {
			candidates = append(candidates, pruneCandidate{
				name:    name,
				addr:    addr,
				isTemp:  isTemp,
				modTime: info.ModTime(),
				size:    info.Size(),
			})
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
	for len(candidates) > 0 {
		if err := ctx.Err(); err != nil {
			return stats, errors.Join(append(errs, err)...)
		}

		batch := candidates[:min(len(candidates), pruneUnlinkBatchSize)]
		candidates = candidates[len(batch):]

		var stop bool
		lockErr := underLock(ctx, func(keep func(hash.Hash) bool) error {
			// The quiescence check ran outside the lock. Re-assert the
			// manifest mtime with the lock held.
			changed, err := manifestMtimeChanged(dir, manifestMtime)
			if err != nil {
				return err
			}
			if changed {
				stats.Skipped = append(stats.Skipped, fmt.Sprintf(
					"%s: the manifest was updated while pruning", dir))
				stop = true
				return nil
			}

			if _testPruneUnderLockHook != nil {
				_testPruneUnderLockHook()
			}

			batchStats, batchStop, batchErrs := unlinkCandidates(ctx, dir, batch, keep)
			stats.add(batchStats)
			errs = append(errs, batchErrs...)
			stop = batchStop
			return nil
		})
		if lockErr != nil {
			if errors.Is(lockErr, fslock.ErrTimeout) {
				// Someone else is updating the manifest. No need to prune.
				stats.Skipped = append(stats.Skipped, fmt.Sprintf(
					"%s: could not take the manifest lock", dir))
			} else {
				errs = append(errs, lockErr)
			}
			break
		}
		if stop {
			break
		}
	}

	for _, name := range staleProbes {
		p := filepath.Join(dir, name)
		if err := file.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("error removing stale prune probe %s: %w", p, err))
		}
	}

	return stats, errors.Join(errs...)
}

// pruneCandidate is a file the scan is willing to delete on the strength of
// what the manifest says. Whether the manifest actually references it is
// decided later, under the manifest lock.
type pruneCandidate struct {
	name string
	// addr is the table file address |name| encodes, and is meaningless for a
	// temp file, which no manifest can reference by name.
	addr    hash.Hash
	isTemp  bool
	modTime time.Time
	size    int64
}

// classifyPruneCandidate reports whether |name| names a file a grace prune may
// consider: an abandoned temp file, or a table file.
//
// Anything we do not positively recognize is left alone. In particular the
// LOCK file and a chunk journal are never candidates, so pointing a prune at a
// directory it does not fully understand costs disk rather than data. The
// manifest is excluded by the caller, which has its own use for it.
func classifyPruneCandidate(name string) (addr hash.Hash, isTemp, ok bool) {
	if strings.HasPrefix(name, tempTablePrefix) || strings.HasPrefix(name, tempManifestPrefix) {
		return hash.Hash{}, true, true
	}
	// A chunk journal's name is a valid table file address, so it would
	// otherwise classify as a table file. A journaled store never reaches this
	// code — it uses a different persister, and a bare local store refuses to
	// open a directory holding a journal at all — but deleting one would be
	// catastrophic, so say so explicitly.
	if name == chunkJournalName {
		return hash.Hash{}, false, false
	}
	addr, ok = fileNameToAddr(name)
	return addr, false, ok
}

// unlinkCandidates deletes the members of |candidates| that |keep| does not
// vouch for. Callers hold the manifest lock.
//
// |stop| reports that a candidate changed on disk since the scan, which means
// the directory stopped being quiescent while this pass was acting on the
// conclusion that it was. The caller must abandon the rest of the pass: the
// grace period was violated for the whole directory, not for one file, and
// every remaining decision rests on the same stale reading. This is the same
// all-or-nothing rule the quiescence check applies up front, enforced again
// once deleting has started.
func unlinkCandidates(ctx context.Context, dir string, candidates []pruneCandidate, keep func(hash.Hash) bool) (stats PruneStats, stop bool, errs []error) {
	for _, c := range candidates {
		if err := ctx.Err(); err != nil {
			return stats, false, append(errs, err)
		}
		if !c.isTemp && keep(c.addr) {
			continue
		}

		p := filepath.Join(dir, c.name)

		// Re-stat immediately before the unlink. A writer running older code
		// can rename a live file over a name in our snapshot without the lock,
		// and deleting it would corrupt the destination. POSIX has no atomic
		// unlink-if-unchanged, so this only narrows the window to the
		// stat/unlink gap rather than closing it.
		info, err := os.Stat(p)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, fmt.Errorf("error stat'ing %s before prune: %w", p, err))
			}
			// A candidate that is simply gone is the outcome we wanted, and
			// nobody lands data by unlinking. Not evidence of a writer.
			continue
		}
		if !info.ModTime().Equal(c.modTime) || info.Size() != c.size {
			stats.Skipped = append(stats.Skipped, fmt.Sprintf(
				"%s: %q changed after the scan; the directory is no longer quiescent", dir, c.name))
			return stats, true, errs
		}

		if err := file.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("error removing unreferenced file %s: %w", p, err))
			continue
		}
		stats.FilesDeleted++
		stats.BytesReclaimed += c.size
	}
	return stats, false, errs
}

// manifestMtimeChanged reports whether the manifest in |dir| still carries the
// mtime |was|, treating an absent manifest as the zero time.
func manifestMtimeChanged(dir string, was time.Time) (bool, error) {
	info, err := os.Stat(filepath.Join(dir, manifestFileName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return !was.IsZero(), nil
		}
		return false, err
	}
	return !info.ModTime().Equal(was), nil
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
