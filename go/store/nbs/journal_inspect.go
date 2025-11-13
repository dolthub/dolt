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
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/sirupsen/logrus"
)

func JournalInspect(journalPath string, seeRoots, seeChunks, crcScan, snapScan bool) int {
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
	// scan logic below. For example, when we hit a block of null bytes, the number of zero length records will be high,
	// but if we hit a block of random non-null bytes, the number of large records will be very high. Look at the printed
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

	// suspectRegionStart is the offset of an unparsable region. Used to process bad regions when they come up.
	suspectRegionStart := -1

	var lastRootTs time.Time

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
				suspectRegionStart = offset
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
					healthyState = true
					firstHealthy = true
					if suspectRegionStart != -1 {
						if crcScan {
							crcMatches := scanMysteryBytesForCRCs(0, buf[suspectRegionStart:offset])
							if len(crcMatches) > 0 {
								logrus.Infof("Scanned suspect region %d to %d for possible CRC matches. Found %v", suspectRegionStart, offset, crcMatches)
							}
						}
						if snapScan {
							snappyFuzzyDecode(buf[0:offset], suspectRegionStart)
						}
					}
					suspectRegionStart = -1
					logrus.Infof("Resumed healthy reads at offset %d", offset)
				}

				rec, err := readJournalRecord(recordBuf)
				if err != nil {
					readErrs += 1
					// healthyState is always true here.
					happyRecords, happyBytes = endHappiness(happyRecords, happyBytes)
					logrus.Errorf("Lost healthy reads at offset %d (read error: %v) [$od -j %d -x %s]", offset, err, offset, journalPath)
					healthyState = false
					suspectRegionStart = offset
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
					if lastRootTs.IsZero() {
						lastRootTs = rec.timestamp
					}
					if rec.timestamp.Before(lastRootTs) {
						logrus.Warnf("Root record timestamp went backwards: last %s, this %s", lastRootTs.String(), rec.timestamp.String())
					}
					lastRootTs = rec.timestamp

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
					suspectRegionStart = offset
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
					suspectRegionStart = offset
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
				suspectRegionStart = offset
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
	if !healthyState && suspectRegionStart != -1 {
		if crcScan {
			crcMatches := scanMysteryBytesForCRCs(0, buf[suspectRegionStart:])
			if len(crcMatches) > 0 {
				logrus.Infof("Scanned suspect region %d to EOF for possible CRC matches. Found %v", suspectRegionStart, crcMatches)
			}
		}
		if snapScan {
			// Scan to the end of the buffer.
			snappyFuzzyDecode(buf[:], suspectRegionStart)
		}
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

type validCRCResult struct {
	start  uint32
	end    uint32
	nested []validCRCResult
}

// scanMysteryBytesForCRCs attempts to interpret the given byte slice for anything which looks like a CRC check summed section.
// Journal records have a CRC checksum at the end of them, and in the case of chunk records, there is an inner CRC checksum
// as well. This function scans the given byte slice for anything that looks like a valid checksum starting from the end
// of the slice and working backwards.
//
// We are only calling this method on data that already failed validation, so we expect what we will find here to be a clue
// into what is wrong with the journal file. But it's also very likely it will find nothing and possibly false positives.
//
// Always call with startIdx of 0. It's exposed for recursion purposes. The |buf| slice is the data to scan, and the
// result offsets will be relative to the start of the slice.
func scanMysteryBytesForCRCs(start int, buf []byte) []validCRCResult {
	var results []validCRCResult
	endIdx := len(buf) - 4
	startIdx := start
	for startIdx < endIdx {
		outerCrc := readUint32(buf[endIdx:])
		for startIdx < endIdx {
			computedCrc := crc(buf[startIdx:endIdx])
			if outerCrc == computedCrc {
				// Found something! Could be random chance, could be real.
				found := validCRCResult{
					start:  uint32(startIdx),
					end:    uint32(endIdx + 4),
					nested: scanMysteryBytesForCRCs(startIdx, buf[0:endIdx]),
				}
				results = append(results, found)

				endIdx = startIdx
				startIdx = start
			} else {
				startIdx += 1
			}
		}
		endIdx -= 1
		startIdx = start
	}
	slices.Reverse(results)
	return results
}

const (
	tagLiteral = 0x00
	tagCopy1   = 0x01
	tagCopy2   = 0x02
	tagCopy4   = 0x03
)

// snappyFuzzyDecode performs a scan of |src|, attempting to find portions of valid snappy-compressed data.
//
// See: https://github.com/golang/snappy/blob/master/decode_other.go#L14
func snappyFuzzyDecode(src []byte, start int) {
	if start < 0 || start >= len(src) {
		logrus.Errorf("snappyFuzzyDecode: start %d out of range for src len %d", start, len(src))
		return
	}

	logrus.Infof("------ Beginning Snappy Fuzzy Decode from offset %d ------", start)

	var MaxEnc = 8 << 20      // max encoded bytes to consume from start (default 8 MiB)
	var MaxOut = 32 << 20     // max decoded bytes to produce (default 32 MiB)
	var MaxBackward = 2 << 20 // max backref distance (default 2 MiB)
	var dots = []byte("<.>")  // bytes inserted when we run into errors

	// History window for backrefs (decoded bytes so far).
	window := make([]byte, 0, min(MaxOut, 1<<20))

	w := LineLogger{}

	lastWroteDots := false
	writeOut := func(p []byte) bool {
		if len(p) == 0 || MaxOut <= 0 {
			return true
		}
		// Avoid writing placeholders repeatedly.
		if slices.Equal(p, dots) {
			if lastWroteDots {
				return true
			}
			lastWroteDots = true
		} else {
			lastWroteDots = false
		}

		// Clip to remaining output budget (note: history maintenance must match this).
		remain := MaxOut - len(window)
		if remain <= 0 {
			return false
		}
		if len(p) > remain {
			p = p[:remain]
		}
		_, err := w.Write(p)
		return err == nil && len(window) < MaxOut
	}

	offset := start
	end := start + MaxEnc
	if end > len(src) {
		end = len(src)
	}
	errs := 0

	for offset < end && len(window) < MaxOut {
		currentByte := src[offset]
		switch currentByte & 0x03 {
		case tagLiteral: // LITERAL
			litLen, adv, ok := literalLenFromTag(src[offset:])
			if !ok || litLen < 0 {
				_ = writeOut(dots)
				errs++
				offset++ // skip 1 byte and try to realign
				continue
			}
			if offset+adv+litLen > len(src) {
				_ = writeOut(dots)
				errs++
				offset++
				continue
			}

			offset += adv

			// Clip literal to remaining decoded budget for history and sink.
			remain := MaxOut - len(window)
			writeLen := min(litLen, remain)
			if writeLen > 0 {
				// Append to history first, then write the same bytes.
				window = append(window, src[offset:offset+writeLen]...)
				if !writeOut(src[offset : offset+writeLen]) {
					logrus.Errorf("snappyFuzzyDecode: hit maxout or sink error during literal write")
					return
				}
			}
			// Consume the whole literal from the encoded stream (even if clipped).
			offset += litLen

		case tagCopy1: // COPY-1: len=4+((currentByte>>2)&7), off: 11 bits across currentByte+next
			if offset+2 > len(src) {
				_ = writeOut(dots)
				errs++
				offset++
				continue
			}
			length := int(4 + ((currentByte >> 2) & 0x7))
			off := int(src[offset+1]) | (int(currentByte&0xE0) << 3)
			offset += 2
			if off <= 0 || off > len(window) || off > MaxBackward {
				_ = writeOut(dots)
				errs++
				continue
			}
			// Clip to remaining decoded budget.
			if rem := MaxOut - len(window); rem <= 0 {
				logrus.Errorf("snappyFuzzyDecode: hit maxout during copy1")
				return
			} else if length > rem {
				length = rem
			}
			if length > 0 {
				// Expand into history; get just-appended tail to write
				tail := appendCopy(&window, off, length)
				if !writeOut(tail) {
					logrus.Errorf("snappyFuzzyDecode: hit maxout or sink error during copy1 write")
					return
				}
			}

		case tagCopy2: // COPY-2: len=1+(currentByte>>2), off: next 2 LE
			if offset+3 > len(src) {
				_ = writeOut(dots)
				errs++
				offset++
				continue
			}
			length := int(1 + (currentByte >> 2))
			off := int(binary.LittleEndian.Uint16(src[offset+1 : offset+3]))
			offset += 3
			if off <= 0 || off > len(window) || off > MaxBackward {
				_ = writeOut(dots)
				errs++
				continue
			}
			if rem := MaxOut - len(window); rem <= 0 {
				logrus.Errorf("snappyFuzzyDecode: hit maxout during copy2")
				return
			} else if length > rem {
				length = rem
			}
			if length > 0 {
				tail := appendCopy(&window, off, length)
				if !writeOut(tail) {
					logrus.Errorf("snappyFuzzyDecode: hit maxout or sink error during copy2 write")
					return
				}
			}

		case tagCopy4: // COPY-4: len=1+(currentByte>>2), off: next 4 LE
			if offset+5 > len(src) {
				_ = writeOut(dots)
				errs++
				offset++
				continue
			}
			length := int(1 + (currentByte >> 2))
			off := int(binary.LittleEndian.Uint32(src[offset+1 : offset+5]))
			offset += 5
			if off <= 0 || off > len(window) || off > MaxBackward {
				_ = writeOut(dots)
				errs++
				continue
			}
			if rem := MaxOut - len(window); rem <= 0 {
				logrus.Errorf("snappyFuzzyDecode: hit maxout during copy4")
				return
			} else if length > rem {
				length = rem
			}
			if length > 0 {
				tail := appendCopy(&window, off, length)
				if !writeOut(tail) {
					logrus.Errorf("snappyFuzzyDecode: hit maxout or sink error during copy4 write")
					return
				}
			}
		}
	}

	w.Flush()
	logrus.Infof("------ Snappy Fuzzy Decode Complete: processed %d bytes, encountered %d errors ------", offset-start, errs)
}

// --- helpers ---
// literalLenFromTag decodes a literal length from the tag byte(s) at the start of b.
// It returns the length, number of bytes consumed, and whether decoding succeeded.
func literalLenFromTag(b []byte) (int, int, bool) {
	if len(b) == 0 || (b[0]&0x03) != 0x00 {
		return 0, 0, false
	}
	h := int(b[0] >> 2)
	switch {
	case h < 60:
		return h + 1, 1, true
	case h == 60:
		if len(b) < 2 {
			return 0, 0, false
		}
		return int(b[1]) + 1, 2, true
	case h == 61:
		if len(b) < 3 {
			return 0, 0, false
		}
		return int(binary.LittleEndian.Uint16(b[1:3])) + 1, 3, true
	case h == 62:
		if len(b) < 4 {
			return 0, 0, false
		}
		v := uint32(b[1]) | uint32(b[2])<<8 | uint32(b[3])<<16
		return int(v) + 1, 4, true
	case h == 63:
		if len(b) < 5 {
			return 0, 0, false
		}
		return int(binary.LittleEndian.Uint32(b[1:5])) + 1, 5, true
	}
	return 0, 0, false
}

// appendCopy appends 'n' bytes to *dst by back-referencing 'off' bytes from the end.
// It returns the slice that was appended (so caller can stream it to w).
func appendCopy(dst *[]byte, off, n int) []byte {
	base := *dst
	start := len(base) - off
	writtenAt := len(base)
	for n > 0 {
		chunk := n
		if chunk > off {
			chunk = off
		}
		base = append(base, base[start:start+chunk]...)
		start += chunk
		n -= chunk
	}
	*dst = base
	return base[writtenAt:]
}

// LineLogger buffers data until ~80 bytes or an explicit newline,
// then logs it via logrus.Infof(). Newlines are rendered as "\n",
// and non-ASCII bytes as "\xNN".
type LineLogger struct {
	buf bytes.Buffer
}

func (w *LineLogger) Write(p []byte) (int, error) {
	total := 0
	for _, b := range p {
		// treat '\n' as data marker, not newline
		if b == '\n' {
			w.buf.WriteString(`\n`)
		} else {
			if b < 0x20 || b > 0x7e {
				w.buf.WriteString(fmt.Sprintf(`\x%02X`, b))
			} else {
				w.buf.WriteByte(b)
			}
		}

		total++
		if w.buf.Len() >= 80 {
			w.flush()
		}
	}
	return total, nil
}

func (w *LineLogger) flush() {
	if w.buf.Len() == 0 {
		return
	}
	logrus.Infof("%s", w.buf.String())
	w.buf.Reset()
}

// Flush can be called manually to force any remaining bytes out.
func (w *LineLogger) Flush() { w.flush() }
