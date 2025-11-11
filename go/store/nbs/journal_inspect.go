// Copyright 2025 Dolthub, Inc.
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
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

func JournalInspect(journalPath string, seeRoots, seeChunks bool) int {
	var f *os.File
	f, err := os.Open(journalPath)
	if err != nil {
		panic("could not open journal file")
	}
	defer f.Close()

	buf, err := io.ReadAll(f)
	if err != nil {
		panic("could not read journal file")
	}

	// Counters for various conditions we want to report on. These counters require a little understanding of the
	// scan logic below. For example, when we hit block of null butes, the number of zero length records will be high,
	// but of we his a block of random non-null bytes, the number of large records will be very high. Look at the printed
	// scan log and the stats, and reason about what they mean together.
	numZeros := 0    // number of zero length records seen
	numTooLarge := 0 // number of over-large records seen
	numBad := 0      // number of validation failures.
	cons := 0        // current consecutive good records
	maxCons := 0     // maximum consecutive good records seen
	readErrs := 0    // number of read errors seen. These happen after validation passes but reading the record fails.
	numGood := 0     // total good records seen
	numChunks := 1   // total chunk records seen
	numRoots := 1    // total root records seen

	// When there are successful reads, we record them so that when we hit a section of bad data we can report how
	// much good data we saw. see |endHappiness|
	happyRecords := 0
	happyBytes := 0

	// This flag indicates whether we think we are in a healthy state or not. It is set to false when we can't read
	// a record, and set to true when we successfully read one. It effects how much we log based on how things are going.
	// For example, if we read one record successfully, we want to log that, but if we read 100 successfully in a row,
	// we don't need to log each one.
	healthyState := false
	// exitStatus will be set to non-zero if we encounter any errors during inspection. Set to one whenever healthyState
	// changes for false, and never reset to zero once set.
	exitStatus := 0

	shasum := sha256.Sum256(buf)
	logrus.Infof("Read %d bytes with sha256 sum: %s", len(buf), hex.EncodeToString(shasum[:]))

	logrus.Infof("--------------- Beginning Journal Scan ----------------")

	// Linear scan of the journal file. If we read a good record, skip to the end of it. If we hit a bad record,
	// move forward one byte and try again. And print reasonable information along our journey of discovery.
	for offset := 0; offset < len(buf)-4; {
		// First four bytes are the record size. 0 should never be valid
		size := readUint32(buf[offset:])
		if size == 0 {
			numZeros += 1
			if cons > maxCons {
				maxCons = cons
			}

			if healthyState {
				happyRecords, happyBytes = endHappiness(happyRecords, happyBytes)
				logrus.Errorf("Encountered zero size record at offset %d [$od -j %d -x %s]", offset, offset, journalPath)
				healthyState = false
				exitStatus = 1
			}

			cons = 0
			offset += 1
			continue
		}
		if size >= journalWriterBuffSize {
			numTooLarge += 1
			cons = 0

			// Large records are not strictly forbidden but are suspicious enough to log. We will not change the healthy state
			// as that will happen when we try to read the record and fail.
			if healthyState {
				logrus.Warnf("Encountered unusually large record of %d at offset %d [$od -j %d -x %s]", size, offset, offset, journalPath)
			}
		}

		if offset+int(size) <= len(buf) {
			recordBuf := buf[offset : offset+int(size)]
			firstHealthy := false
			if err = validateJournalRecord(recordBuf); err == nil {
				if !healthyState {
					logrus.Infof("Resumed healthy reads at offset %d", offset)
					healthyState = true
					firstHealthy = true
				}

				rec, err := readJournalRecord(recordBuf)
				if err != nil {
					readErrs += 1
					// healthyState is always true here.
					happyRecords, happyBytes = endHappiness(happyRecords, happyBytes)
					logrus.Errorf("Lost healthy reads at offset %d (read error: %v) [$od -j %d -x %s]", offset, err, offset, journalPath)
					healthyState = false
					exitStatus = 1

					cons = 0
					numBad += 1
					offset += 1
					continue
				}
				if rec.kind == chunkJournalRecKind {
					numChunks += 1
					if firstHealthy {
						logrus.Infof("First Chunk Record Found %s (%d bytes)", rec.address.String(), len(recordBuf))
					} else if seeChunks {
						logrus.Infof("Chunk Record Found %s (%d bytes)", rec.address.String(), len(recordBuf))
					}
				} else if rec.kind == rootHashJournalRecKind {
					numRoots += 1
					if firstHealthy {
						logrus.Infof("First Root Record Found %s (%d bytes)", rec.address.String(), len(recordBuf))
					} else if seeRoots {
						logrus.Infof("Root Record Found %s (%d bytes)", rec.address.String(), len(recordBuf))
					}
				} else {
					// Hard to imagine how this would happen if validation passed, but log and go unhealthy just in case.
					happyRecords, happyBytes = endHappiness(happyRecords, happyBytes)
					logrus.Errorf("Unexpected Record Kind: %d", rec.kind)
					healthyState = false
					exitStatus = 1
					continue
				}

				// The only happy path is this one!
				numGood += 1
				happyRecords += 1
				happyBytes += int(size)
				cons += 1
				if cons > maxCons {
					maxCons = cons
				}
				// Skip to the end of this valid record.
				offset += int(size)
			} else {
				// Could not validate the record.
				if healthyState {
					happyRecords, happyBytes = endHappiness(happyRecords, happyBytes)
					logrus.Errorf("Lost healthy reads at offset %d (%v) [$od -j %d -x %s]", offset, err, offset, journalPath)
					healthyState = false
					exitStatus = 1
				}

				cons = 0
				numBad += 1
				offset += 1
			}
		} else {
			// Too many bytes requested.
			if healthyState {
				happyRecords, happyBytes = endHappiness(happyRecords, happyBytes)
				logrus.Errorf("Lost healthy reads at offset %d (read %d - past EOF) [$od -j %d -x %s]", offset, size, offset, journalPath)
				healthyState = false
				exitStatus = 1
			}
			
			cons = 0
			numBad += 1
			offset += 1
		}
	}
	if happyRecords > 0 {
		logrus.Infof("Successfully read %d records (%d bytes)", happyRecords, happyBytes)
	}

	logrus.Infof("----- Journal Inspection Report -----")
	logrus.Infof("Healthy End State           : %t", healthyState)
	logrus.Infof("0 len records               : %d", numZeros)
	logrus.Infof("5mb or larger records       : %d", numTooLarge)
	logrus.Infof("Failed Attempts to validate : %d", numBad)
	logrus.Infof("Read Error Count            : %d", readErrs)
	logrus.Infof("Successfully read records   : %d", numGood)
	logrus.Infof("Successfully read chunks    : %d", numChunks)
	logrus.Infof("Successfully read roots     : %d", numRoots)
	logrus.Infof("Maximum Consecutive Records : %d", maxCons)

	return exitStatus
}

// endHappiness logs the number of successfully read records and bytes, if any, and returns 0 to allow the caller to reset their counters.
func endHappiness(happyRecords, happyBytes int) (int, int) {
	if happyRecords == 0 {
		return 0, 0
	}
	logrus.Errorf("Successfully read %d records (%d bytes) before encountering an error", happyRecords, happyBytes)

	return 0, 0
}
