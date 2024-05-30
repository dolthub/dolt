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

package binlogreplication

import (
	"encoding/binary"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
)

// maxBinlogSize is the maximum size of a binlog file, before the binlog writer rotates to a new binlog file. Once
// a binlog file reaches this size, or greater, the events from the next transaction should be written to a new file.
// Note that all events in a single transaction should be written to the same binlog file.
// This corresponds to the @@max_binlog_size system variable in MySQL:
// https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_max_binlog_size
const maxBinlogSize = 1024 * 1024 * 1024

var binlogDirectory = filepath.Join(".dolt", "binlog")

// binlogFileMagicNumber holds the four bytes that start off every
// MySQL binlog file and identify the file as a MySQL binlog.
var binlogFileMagicNumber = []byte{0xfe, 0x62, 0x69, 0x6e}

type LogManager struct {
	currentBinlogFile     *os.File
	currentBinlogFileName string
	fs                    filesys.Filesys
}

// NewLogManager creates a new LogManager instance where binlog files are stored in the .dolt/binlog directory
// underneath the specified |fs| filesystem. The |binlogFormat| and |binlogStream| are used to initialize the
// new binlog file.
func NewLogManager(fs filesys.Filesys, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) *LogManager {
	// TODO: On server startup, we need to find the most recent binlog file, add a rotate event at the end (if necessary?), and start a new file. Documentation seems to indicate that a rotate event is added at the end of a binlog file, so that the streamer can jump to the next file, but I don't see this in our MySQL sample binlog files. Need to do more testing here.

	lm := &LogManager{
		fs: fs,
	}

	// TODO: Could resolve the base dir for the binlog file directory here; would it help us avoid returning errors in other APIs?

	// Initialize binlog file storage (extract to function!)
	err := fs.MkDirs(binlogDirectory)
	if err != nil {
		panic(err)
	}

	// Initialize current binlog file
	nextLogFilename, err := lm.nextLogFile()
	if err != nil {
		panic(err)
	}
	lm.currentBinlogFileName = nextLogFilename

	// Ugh... we need binlogFormat and binlogEventMeta in order to do this...
	// Actually... Do we need binlogEventMeta, or could we fake it? We only need binlogEventMeta so that
	// Vitess can call a function on that instance, and for the server Id. The position in the file
	// should always be zero at this point, so maybe we could clean this up more?
	err = lm.initializeCurrentLogFile(binlogFormat, binlogEventMeta)
	if err != nil {
		panic(err)
	}

	return lm
}

func (lm *LogManager) nextLogFile() (filename string, err error) {
	mostRecentLogfile, err := lm.mostRecentLogFileForBranch(BinlogBranch)
	if err != nil {
		return "", err
	}

	if mostRecentLogfile == "" {
		return formatBinlogFilename(BinlogBranch, 1), nil
	} else {
		branch, sequence, err := parseBinlogFilename(mostRecentLogfile)
		if err != nil {
			return "", err
		}
		return formatBinlogFilename(branch, sequence+1), nil
	}
}

func (lm *LogManager) logFilesOnDisk() (files []string, err error) {
	err = lm.fs.Iter(binlogDirectory, false, func(path string, size int64, isDir bool) (stop bool) {
		base := filepath.Base(path)
		if strings.HasPrefix(base, "binlog-") {
			files = append(files, base)
		}

		return false
	})
	if err != nil {
		return nil, err
	}

	return files, nil
}

func (lm *LogManager) logFilesOnDiskForBranch(branch string) (files []string, err error) {
	branch = strings.ToLower(branch)
	err = lm.fs.Iter(binlogDirectory, false, func(path string, size int64, isDir bool) (stop bool) {
		base := filepath.Base(path)
		if strings.HasPrefix(base, "binlog-"+branch) {
			files = append(files, base)
		}

		return false
	})
	if err != nil {
		return nil, err
	}

	return files, nil
}

func (lm *LogManager) mostRecentLogfile() (logFile string, err error) {
	logFiles, err := lm.logFilesOnDisk()
	if err != nil {
		return "", err
	}

	return logFiles[len(logFiles)-1], nil
}

func (lm *LogManager) mostRecentLogFileForBranch(branch string) (logFile string, err error) {
	logFiles, err := lm.logFilesOnDiskForBranch(branch)
	if err != nil {
		return "", err
	}

	// TODO: This assumes the list comes back sorted by time or by filename
	if len(logFiles) == 0 {
		return "", nil
	} else {
		return logFiles[len(logFiles)-1], nil
	}
}

func (lm *LogManager) RotateLogFile() error {
	// TODO: Handle logfile rotation:
	// - Manual rotation from the FLUSH LOGS statement (Out of scope)
	// - Automatically when the binary log file reaches the maximum size defined by the max_binlog_size configuration parameter.
	//   https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_max_binlog_size
	// - During a server shutdown or restart when the binary log is enabled.
	return nil
}

func (lm *LogManager) PurgeLogFiles() error {
	// TODO: implement support for purging older binlog files
	//       This also requires setting gtid_purged
	// https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_gtid_purged
	// Need to test the case where the GTID requested is not
	// available –has been executed, but has been purged
	return nil
}

func (lm *LogManager) initializeCurrentLogFile(binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	// Open the file in append mode
	// TODO: we should probably create this file as soon as possible, like when we construct LogManager
	// TODO: But, we should only construct log manager when binlogging is enabled
	file, err := os.OpenFile(lm.currentBinlogFilepath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	lm.currentBinlogFile = file

	// Write Magic Number
	_, err = file.Write(binlogFileMagicNumber)
	if err != nil {
		return err
	}

	// TODO: Do we need to do this?
	binlogFilePosition := uint64(0)
	binlogEventMeta.NextLogPosition = uint32(binlogFilePosition)

	// Write Format Event
	binlogEvent := mysql.NewFormatDescriptionEvent(*binlogFormat, binlogEventMeta)
	binlogEventMeta.NextLogPosition += binlogEvent.Length()
	_, err = lm.currentBinlogFile.Write(binlogEvent.Bytes())
	return err
}

// WriteEvents writes |binlogEvents| to the current binlog file.
func (lm *LogManager) WriteEvents(binlogEvents []mysql.BinlogEvent) error {
	// Write to the file
	rotateLogFile := false
	for _, event := range binlogEvents {
		nextPosition := binary.LittleEndian.Uint32(event.Bytes()[13 : 13+4])
		if nextPosition > maxBinlogSize {
			rotateLogFile = true
		}

		if _, err := lm.currentBinlogFile.Write(event.Bytes()); err != nil {
			return err
		}
	}

	if rotateLogFile {
		// TODO: We need to rotate after this transaction... (i.e. set of events)
		// TODO: How do the streamers react when we rotate the binlog file?
		//       If a streamer is reading a file, it needs to continue reading
		// NOTE: FormatDescription event should be the very first entry in the binlog file,
		//       and a Rotate event should be the very last entry in the (completed) binlog file.
		//       Streamers will read the rotate event and know what file to open next.
		return lm.RotateLogFile()
	}

	return nil
}

func (lm *LogManager) resolveLogFile(filename string) (string, error) {
	binlogBaseDir, err := lm.fs.Abs(binlogDirectory)
	if err != nil {
		return "", err
	}

	// TODO: Should we make sure it exists?
	return filepath.Join(binlogBaseDir, filename), nil
}

func (lm *LogManager) currentBinlogFilepath() string {
	logFile, err := lm.resolveLogFile(lm.currentBinlogFileName)
	if err != nil {
		// TODO: return an error, or handle this err somewhere else where we do return an error
		panic(err)
	}

	return logFile
}

// formatBinlogFilename formats a binlog filename using the specified |branch| and |sequence| number. The
// returned filename will be of the form "binlog-main.000001".
func formatBinlogFilename(branch string, sequence int) string {
	return fmt.Sprintf("binlog-%s.%06d", branch, sequence)
}

// parseBinlogFilename parses a binlog filename, of the form "binlog-main.000001", into its branch and
// sequence number.
func parseBinlogFilename(filename string) (branch string, sequence int, err error) {
	if !strings.HasPrefix(filename, "binlog-") {
		return "", 0, fmt.Errorf("invalid binlog filename: %s; must start with 'binlog-'", filename)
	}

	filename = strings.TrimPrefix(filename, "binlog-")

	splits := strings.Split(filename, ".")
	if len(splits) != 2 {
		return "", 0, fmt.Errorf(
			"unable to parse binlog filename: %s; expected format 'binlog-branch.sequence'", filename)
	}

	branch = splits[0]
	sequenceString := splits[1]

	sequence, err = strconv.Atoi(sequenceString)
	if err != nil {
		return "", 0, fmt.Errorf(
			"unable to parse binlog sequence number: %s; %s", sequenceString, err.Error())
	}

	return branch, sequence, nil
}
