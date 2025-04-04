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
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var binlogDirectory = filepath.Join(".dolt", "binlog")

// logManager is responsible for the binary log files on disk, including actually writing events to the log files,
// rotating the log files, listing the available log files, purging old log files, and keeping track of what GTIDs
// are available in the log files.
type logManager struct {
	binlogFormat    mysql.BinlogFormat
	binlogEventMeta mysql.BinlogEventMetadata

	mu                    *sync.Mutex
	currentBinlogFile     *os.File
	currentBinlogFileName string
	currentPosition       int
	fs                    filesys.Filesys
	binlogDirectory       string
	availableGtids        mysql.GTIDSet
}

// NewLogManager creates a new logManager instance where binlog files are stored in the .dolt/binlog directory
// underneath the specified |fs| filesystem. This method also initializes the binlog logging system, including
// rotating to a new log file and purging expired log files.
func NewLogManager(ctx *sql.Context, fs filesys.Filesys) (*logManager, error) {
	binlogFormat := createBinlogFormat()
	binlogEventMeta, err := createBinlogEventMetadata(ctx)
	if err != nil {
		return nil, err
	}

	lm := &logManager{
		mu:              &sync.Mutex{},
		fs:              fs,
		binlogFormat:    *binlogFormat,
		binlogEventMeta: *binlogEventMeta,
	}

	// Initialize binlog file storage directory
	if err := fs.MkDirs(binlogDirectory); err != nil {
		return nil, err
	}
	abs, err := fs.Abs(binlogDirectory)
	if err != nil {
		return nil, err
	}
	lm.binlogDirectory = abs

	// Initialize a new binlog file
	if err := lm.createNewBinlogFile(ctx); err != nil {
		return nil, err
	}

	// Purge any expired log files
	if err := lm.purgeExpiredLogFiles(ctx); err != nil {
		return nil, err
	}

	// Ensure the previous log file has a Rotate event that points to the new log file
	if err := lm.addRotateEventToPreviousLogFile(); err != nil {
		return nil, err
	}

	// Initialize @@gtid_purged based on the first GTID we see available in the available binary logs
	// NOTE that we assume that all GTIDs are available after the first GTID we find in the logs. This won't
	// be true if someone goes directly to the file system and deletes binary log files, but that isn't
	// how we expect people to manage the binary log files.
	if err := lm.initializePurgedGtids(ctx); err != nil {
		return nil, err
	}

	// Initialize the set of GTIDs that are available in the current log files
	if err := lm.initializeAvailableGtids(); err != nil {
		return nil, err
	}

	return lm, nil
}

// initializeAvailableGtids sets the value of availableGtids by seeing what GTIDs have been executed (@@gtid_executed)
// and subtracting any GTIDs that have been marked as purged (@@gtid_purged).
func (lm *logManager) initializeAvailableGtids() (err error) {
	// Initialize availableGtids from @@gtid_executed – we start by assuming we have all executed GTIDs available
	// in the logs, and then adjust availableGtids based on which GTIDs we detect have been purged.
	_, gtidExecutedValue, ok := sql.SystemVariables.GetGlobal("gtid_executed")
	if !ok {
		return fmt.Errorf("unable to find system variable @@gtid_executed")
	}
	if _, ok := gtidExecutedValue.(string); !ok {
		return fmt.Errorf("unexpected type for @@gtid_executed: %T", gtidExecutedValue)
	}
	lm.availableGtids, err = mysql.ParseMysql56GTIDSet(gtidExecutedValue.(string))
	if err != nil {
		return err
	}

	_, gtidPurgedValue, ok := sql.SystemVariables.GetGlobal("gtid_purged")
	if !ok {
		return fmt.Errorf("unable to find system variable @@gtid_purged")
	}
	if _, ok := gtidExecutedValue.(string); !ok {
		return fmt.Errorf("unexpected type for @@gtid_purged: %T", gtidPurgedValue)
	}
	purgedGtids, err := mysql.ParseMysql56GTIDSet(gtidPurgedValue.(string))
	if err != nil {
		return err
	}

	lm.availableGtids = lm.availableGtids.Subtract(purgedGtids)
	logrus.Debugf("setting availableGtids to %s after removing purgedGtids %s", lm.availableGtids, purgedGtids)
	return nil
}

// purgeExpiredLogFiles removes any binlog files that are older than @@binlog_expire_logs_seconds. This automatic
// binlog purging currently happens only on server startup. Eventually this should also be hooked into the `FLUSH LOGS`
// (or `FLUSH BINARY LOGS`) statement as well to match MySQL's behavior.
//
// When this method is called, it is expected that the new, current binlog file has already been initialized and that
// adding a Rotate event to the previous log has NOT occurred yet (otherwise adding the Rotate event would update the
// log file's last modified time and would not be purged).
func (lm *logManager) purgeExpiredLogFiles(ctx *sql.Context) error {
	expireLogsSeconds, err := lookupBinlogExpireLogsSeconds(ctx)
	if expireLogsSeconds == 0 {
		// If @@binlog_expire_logs_seconds is set to 0, then binlog files are never automatically expired
		return nil
	}

	purgeThresholdTime := time.Now().Add(-time.Duration(expireLogsSeconds) * time.Second)

	filenames, err := lm.logFilesOnDiskForBranch(BinlogBranch)
	if err != nil {
		return err
	}

	logrus.WithField("logfiles", strings.Join(filenames, ", ")).Tracef("examining available log files for expiration...")

	for _, filename := range filenames {
		fullLogFilepath := lm.resolveLogFile(filename)
		stat, err := os.Stat(fullLogFilepath)
		if err != nil {
			return err
		}

		logrus.WithField("file", filename).
			WithField("mod_time", stat.ModTime()).
			WithField("purge_threshold", purgeThresholdTime).
			Tracef("checking log file")

		if stat.ModTime().Before(purgeThresholdTime) {
			logrus.Debugf("purging expired binlog filename: %s", filename)
			if err := os.Remove(fullLogFilepath); err != nil {
				return err
			}
		}
	}
	return nil
}

// initializePurgedGtids searches through the available binary logs to find the first GTID available
// in the binary logs. If a GTID is found in the available logs, then @@gtid_purged is set to the GTID immediately
// preceding the found GTID, unless the found GTID is sequence number 1. If no GTIDs are found in the available
// binary logs, then it is assumed that all GTIDs have been purged, so @@gtid_purged is set to the same value
// held in @@gtid_executed.
func (lm *logManager) initializePurgedGtids(ctx *sql.Context) error {
	filenames, err := lm.logFilesOnDiskForBranch(BinlogBranch)
	if err != nil {
		return err
	}

	for _, filename := range filenames {
		gtid, err := lm.findFirstGtidInFile(filename)
		if err == io.EOF {
			continue
		} else if err != nil {
			return err
		}

		// If the first found GTID in the available binary logs is anything other than sequence number 1,
		// then we need to set @@gtid_purged to indicate that not all GTIDs are available in the logs, and
		// all GTIDs before the first sequence number found have been purged.
		sequenceNumber := gtid.SequenceNumber().(int64)
		if sequenceNumber > 1 {
			gtidPurged := fmt.Sprintf("%s:%d", gtid.SourceServer(), sequenceNumber-1)
			logrus.Debugf("setting gtid_purged to: %s", gtidPurged)
			return sql.SystemVariables.SetGlobal(ctx, "gtid_purged", gtidPurged)
		} else {
			return nil
		}
	}

	// If there are no GTID events in any of the files, then all GTIDs have been purged, so
	// initialize @@gtid_purged with the value of @@gtid_executed.
	_, gtidExecutedValue, ok := sql.SystemVariables.GetGlobal("gtid_executed")
	if !ok {
		return fmt.Errorf("unable to find system variable @@gtid_executed")
	}
	logrus.Debugf("no available GTIDs found in logs, setting gtid_purged to: %s", gtidExecutedValue)
	return sql.SystemVariables.SetGlobal(ctx, "gtid_purged", gtidExecutedValue)
}

// findLogFileForPosition searches through the available binlog files on disk for the first log file that
// contains GTIDs that are NOT present in |executedGtids|. This is determined by reading the first GTID event
// from each log file and selecting the previous file when the first GTID not in |executedGtids| is found. If
// the first GTID event in all available logs files is in |executedGtids|, then the current log file is returned.
func (lm *logManager) findLogFileForPosition(executedGtids mysql.GTIDSet) (string, error) {
	files, err := lm.logFilesOnDiskForBranch(BinlogBranch)
	if err != nil {
		return "", err
	}

	for i, f := range files {
		binlogFilePath := filepath.Join(lm.binlogDirectory, f)
		file, err := openBinlogFileForReading(binlogFilePath)
		if err != nil {
			return "", err
		}

		binlogEvent, err := readFirstGtidEventFromFile(file)
		if fileCloseErr := file.Close(); fileCloseErr != nil {
			logrus.Errorf("unable to cleanly close binlog file %s: %s", f, err.Error())
		}
		if err == io.EOF {
			continue
		} else if err != nil {
			return "", err
		}

		if binlogEvent.IsGTID() {
			gtid, _, err := binlogEvent.GTID(lm.binlogFormat)
			if err != nil {
				return "", err
			}
			// If the first GTID in this file is contained in the set of GTIDs that the replica
			// has already executed, then move on to check the next file.
			if executedGtids.ContainsGTID(gtid) {
				continue
			}

			// If we found an unexecuted GTID in the first binlog file, return the first file,
			// otherwise return the previous file
			if i == 0 {
				return binlogFilePath, nil
			} else {
				return filepath.Join(lm.binlogDirectory, files[i-1]), nil
			}
		}
	}

	// If we don't find any GTIDs that are missing from |executedGtids|, then return the current
	// log file so the streamer can reply events from it, potentially finding GTIDs later in the
	// file that need to be sent to the connected replica, or waiting for new events to be written.
	return lm.currentBinlogFilepath(), nil
}

// findFirstGtidInFile opens the file with the base name |filename| in the binlog directory and
// reads the events until a GTID event is found, then extracts the GTID value and returns it.
func (lm *logManager) findFirstGtidInFile(filename string) (gtid mysql.GTID, err error) {
	openFile, err := openBinlogFileForReading(lm.resolveLogFile(filename))
	if err != nil {
		return nil, err
	}
	defer openFile.Close()

	binlogEvent, err := readFirstGtidEventFromFile(openFile)
	if err != nil {
		return nil, err
	}

	gtid, _, err = binlogEvent.GTID(lm.binlogFormat)
	if err != nil {
		return nil, err
	}

	return gtid, nil
}

// addRotateEventToPreviousLogFile finds the previous binlog file and appends a Rotate event that points to the
// next binlog file. This is necessary so that as streamers are reading from the binlog files, they have a pointer
// to follow to the next binlog file. This function MUST be called after the new log file has been initialized,
// and after any expired log files have been purged.
func (lm *logManager) addRotateEventToPreviousLogFile() error {
	previousLogFileName, err := lm.previousLogFile()
	if err != nil {
		return err
	}

	// If the previous log file in the sequence has been purged, then there's nothing to do
	if !fileExists(lm.resolveLogFile(previousLogFileName)) {
		return nil
	}

	// Open the log file and append a Rotate event
	previousLogFile, err := os.OpenFile(lm.resolveLogFile(previousLogFileName), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer previousLogFile.Close()

	rotateEvent := mysql.NewRotateEvent(lm.binlogFormat, lm.binlogEventMeta, 0, lm.currentBinlogFileName)
	_, err = previousLogFile.Write(rotateEvent.Bytes())
	return err
}

// createNewBinlogFile creates a new binlog file and initializes it with the binlog magic number, a Format Description
// event, and a Previous GTIDs event. The new binlog file is opened for append only writing.
func (lm *logManager) createNewBinlogFile(ctx *sql.Context) error {
	nextLogFilename, err := lm.nextLogFile()
	if err != nil {
		return err
	}
	lm.currentBinlogFileName = nextLogFilename

	return lm.initializeCurrentLogFile(ctx, lm.binlogFormat, lm.binlogEventMeta)
}

// nextLogFile returns the filename of the next bin log file in the current sequence. For example, if the
// current log file is "binlog-main.000008" the nextLogFile() method would return "binlog-main.000009".
// Note that this function returns the file name only, not the full file path.
func (lm *logManager) nextLogFile() (filename string, err error) {
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

// previousLogFile returns the filename of the previous bin log file in the current sequence. For example, if
// the current log file is "binlog-main.000008" the previousLogFile() method would return "binlog-main.000007".
// Note that this function returns the file name only, not the full path, and doesn't guarantee that the named
// file actually exists on disk or not.
func (lm *logManager) previousLogFile() (filename string, err error) {
	branch, sequence, err := parseBinlogFilename(lm.currentBinlogFileName)
	if err != nil {
		return "", err
	}
	return formatBinlogFilename(branch, sequence-1), nil
}

// TODO: consider moving these to the helper function file

func (lm *logManager) logFilesOnDisk() (files []string, err error) {
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

func (lm *logManager) logFilesOnDiskForBranch(branch string) (files []string, err error) {
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

func (lm *logManager) mostRecentLogfile() (logFile string, err error) {
	logFiles, err := lm.logFilesOnDisk()
	if err != nil {
		return "", err
	}

	return logFiles[len(logFiles)-1], nil
}

func (lm *logManager) mostRecentLogFileForBranch(branch string) (logFile string, err error) {
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

// RotateLogFile rotates the current log file that is actively being written to. A new binlog file is created and
// initialized, including writing the first four bytes with the binlog magic number, and the old binlog file is closed.
// Rotation should occur when an administrator explicitly requests it with the `FLUSH LOGS` statement, during server
// shutdown or restart, or when the current binary log file size exceeds the maximum size defined by the
// @@max_binlog_size system variable.
func (lm *logManager) RotateLogFile(ctx *sql.Context) error {
	nextLogFile, err := lm.nextLogFile()
	if err != nil {
		return err
	}
	logrus.Tracef("Rotating bin log file to: %s", nextLogFile)

	binlogEvent := mysql.NewRotateEvent(lm.binlogFormat, lm.binlogEventMeta, 0, nextLogFile)
	if err = lm.writeEventsHelper(ctx, binlogEvent); err != nil {
		return err
	}

	// Close the current binlog file
	if err = lm.currentBinlogFile.Close(); err != nil {
		logrus.Errorf("error closing current binlog file before rotating to new file: %s", err.Error())
	}

	// Open and initialize a new binlog file
	lm.currentBinlogFileName = nextLogFile
	return lm.initializeCurrentLogFile(ctx, lm.binlogFormat, lm.binlogEventMeta)
}

// initializeCurrentLogFile creates and opens the current binlog file for append only writing, writes the first four
// bytes with the binlog magic numbers, then writes a Format Description event and a Previous GTIDs event.
func (lm *logManager) initializeCurrentLogFile(ctx *sql.Context, binlogFormat mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	logrus.Tracef("Initializing binlog file: %s", lm.currentBinlogFilepath())

	// Open the file in append mode
	file, err := os.OpenFile(lm.currentBinlogFilepath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	lm.currentBinlogFile = file
	lm.currentPosition = 0

	// Write Magic Number
	_, err = file.Write(binlogFileMagicNumber)
	if err != nil {
		return err
	}
	lm.currentPosition += len(binlogFileMagicNumber)

	// Write Format Description Event, the first event in each binlog file
	binlogEvent := mysql.NewFormatDescriptionEvent(binlogFormat, binlogEventMeta)
	if err = lm.writeEventsHelper(ctx, binlogEvent); err != nil {
		return err
	}

	// Write the Previous GTIDs event
	// TODO: Instead of using the @@gtid_executed system variable, logManager could keep track of which GTIDs
	//       it has seen logged and use that as the source of truth for the Previous GTIDs event. This would
	//       eliminate a race condition.
	_, rawValue, ok := sql.SystemVariables.GetGlobal("gtid_executed")
	if !ok {
		panic("unable to find @@gtid_executed system variable")
	}
	stringValue, ok := rawValue.(string)
	if !ok {
		panic(fmt.Sprintf("unexpected type for @@gtid_executed system variable: %T", rawValue))
	}

	gtidSet, err := mysql.ParseMysql56GTIDSet(stringValue)
	if err != nil {
		return err
	}
	return lm.writeEventsHelper(ctx, mysql.NewPreviousGtidsEvent(binlogFormat, binlogEventMeta, gtidSet.(mysql.Mysql56GTIDSet)))
}

// WriteEvents writes |binlogEvents| to the current binlog file. Access to write to the binary log is synchronized,
// so that only one thread can write to the log file at a time.
func (lm *logManager) WriteEvents(ctx *sql.Context, binlogEvents ...mysql.BinlogEvent) error {
	// synchronize on WriteEvents so that only one thread is writing to the log file at a time
	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.writeEventsHelper(ctx, binlogEvents...)
}

// writeEventsHelper writes |binlogEvents| to the current binlog file. This function is NOT synchronized, and is only
// intended to be used from code inside logManager that needs to be called transitively from the WriteEvents method.
func (lm *logManager) writeEventsHelper(ctx *sql.Context, binlogEvents ...mysql.BinlogEvent) error {
	maxBinlogSize, err := lookupMaxBinlogSize(ctx)
	if err != nil {
		return err
	}

	// Write to the file
	rotateLogFile := false
	for _, event := range binlogEvents {
		// NOTE: When we write the event to file, we need to ensure the next log position field
		// is correct. That means we have to serialize the events going into the log file and
		// we update their NextLogPosition field in the header to ensure it's correct. Because
		// we change the packet, we must recompute the checksum.
		nextPosition := lm.currentPosition + len(event.Bytes())
		binary.LittleEndian.PutUint32(event.Bytes()[13:13+4], uint32(nextPosition))
		mysql.UpdateChecksum(lm.binlogFormat, event)

		lm.currentPosition = nextPosition
		if nextPosition > maxBinlogSize && !event.IsRotate() {
			rotateLogFile = true
		}

		// Write the event to file
		if _, err := lm.currentBinlogFile.Write(event.Bytes()); err != nil {
			return err
		}

		if event.IsGTID() {
			gtid, _, err := event.GTID(lm.binlogFormat)
			if err != nil {
				return err
			}
			// TODO: Consider locking around lm.availableGtids
			lm.availableGtids = lm.availableGtids.AddGTID(gtid)
		}
	}

	if rotateLogFile {
		// NOTE: Rotate event should be the very last entry in the (completed) binlog file.
		//       Streamers will read the rotate event and know what file to open next.
		return lm.RotateLogFile(ctx)
	}

	return nil
}

// calculateMissingGtids takes the set of GTIDs that a replica reports it has executed, |replicaExecutedGtids|,
// and the full set of GTIDs executed in this primary server, |primaryExecutedGtids|, and determines which GTIDs
// are needed to bring the replica in sync with this server, but not available in the current binary logs. The
// results are a returned as the set of GTIDs that this server is unable to provide to the replica, since they are
// no longer available in this server's binary logs. If the returned set of GTIDs is empty, then this server can
// accept the connection from the replica and start streaming it the GTIDs needed to get it in sync with this
// primary.
func (lm *logManager) calculateMissingGtids(replicaExecutedGtids mysql.GTIDSet, primaryExecutedGtids mysql.GTIDSet) mysql.GTIDSet {
	// First, subtract all the GTIDs that the replica has executed from the GTIDs this server has executed,
	// in order to determine which GTIDs are needed to get the replica in sync with the primary.
	neededGtids := primaryExecutedGtids.Subtract(replicaExecutedGtids)

	// Next subtract all the GTIDs that are available in the logs to determine which GTIDs are missing.
	missingGtids := neededGtids.Subtract(lm.availableGtids)

	logrus.Debugf("calculateMissingGtids: replicaExecutedGtids: %s, primaryExecutedGtids: %s, neededGtids: %s, availableGtids: %s, missingGtids: %s", replicaExecutedGtids, primaryExecutedGtids, neededGtids, lm.availableGtids, missingGtids)

	return missingGtids
}

// resolveLogFile accepts a base filename of a binlog file and returns a fully qualified file
// path to the file in the binlog storage directory.
func (lm *logManager) resolveLogFile(filename string) string {
	return filepath.Join(lm.binlogDirectory, filename)
}

// TODO: Consider moving lookup SystemVar helper functions into a separate file

func (lm *logManager) currentBinlogFilepath() string {
	return lm.resolveLogFile(lm.currentBinlogFileName)
}

// lookupMaxBinlogSize looks up the value of the @@max_binlog_size system variable and returns it, along with any
// errors encountered while looking it up.
func lookupMaxBinlogSize(ctx *sql.Context) (int, error) {
	_, value, ok := sql.SystemVariables.GetGlobal("max_binlog_size")
	if !ok {
		return 0, fmt.Errorf("system variable @@max_binlog_size not found")
	}

	intValue, _, err := gmstypes.Int32.Convert(ctx, value)
	if err != nil {
		return 0, err
	}
	return int(intValue.(int32)), nil
}

// lookupBinlogExpireLogsSeconds looks up the value of the @@binlog_expire_logs_seconds system variable and returns
// it, along with any errors encountered while looking it up.
func lookupBinlogExpireLogsSeconds(ctx *sql.Context) (int, error) {
	_, value, ok := sql.SystemVariables.GetGlobal("binlog_expire_logs_seconds")
	if !ok {
		return -1, fmt.Errorf("unable to find system variable @@binlog_expire_logs_seconds")
	}

	int32Value, _, err := gmstypes.Int32.Convert(ctx, value)
	if err != nil {
		return -1, fmt.Errorf("unable to convert @@binlog_expire_logs_seconds value to integer: %s", err.Error())
	}

	return int(int32Value.(int32)), nil
}
