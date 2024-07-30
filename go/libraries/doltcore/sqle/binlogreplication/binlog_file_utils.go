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
	"os"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
)

// binlogFileMagicNumber holds the four bytes that start off every
// MySQL binlog file and identify the file as a MySQL binlog.
var binlogFileMagicNumber = []byte{0xfe, 0x62, 0x69, 0x6e}

// fileExists returns true if the specified |filepath| exists on disk and is not a directory,
// otherwise returns false. |filepath| is a fully specified path to a file on disk.
func fileExists(filepath string) bool {
	info, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// openBinlogFileForReading opens the specified |logfile| for reading and reads the first four bytes to make sure they
// are the expected binlog file magic numbers. If any problems are encountered opening the file or reading the first
// four bytes, an error is returned.
func openBinlogFileForReading(logfile string) (*os.File, error) {
	file, err := os.Open(logfile)
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, len(binlogFileMagicNumber))
	bytesRead, err := file.Read(buffer)
	if err != nil {
		return nil, err
	}
	if bytesRead != len(binlogFileMagicNumber) || string(buffer) != string(binlogFileMagicNumber) {
		return nil, fmt.Errorf("invalid magic number in binlog file!")
	}

	return file, nil
}

// readBinlogEventFromFile reads the next binlog event from the specified, open |file| and
// returns it. If no more events are available in the file, then io.EOF is returned.
func readBinlogEventFromFile(file *os.File) (mysql.BinlogEvent, error) {
	headerBuffer := make([]byte, 4+1+4+4+4+2)
	_, err := file.Read(headerBuffer)
	if err != nil {
		return nil, err
	}

	// Event Header:
	//timestamp := headerBuffer[0:4]
	//eventType := headerBuffer[4]
	//serverId := binary.LittleEndian.Uint32(headerBuffer[5:5+4])
	eventSize := binary.LittleEndian.Uint32(headerBuffer[9 : 9+4])

	payloadBuffer := make([]byte, eventSize-uint32(len(headerBuffer)))
	_, err = file.Read(payloadBuffer)
	if err != nil {
		return nil, err
	}

	return mysql.NewMysql56BinlogEvent(append(headerBuffer, payloadBuffer...)), nil
}

// readFirstGtidEventFromFile reads events from |file| until a GTID event is found, then
// returns that GTID event. If |file| has been read completely and no GTID events were found,
// then io.EOF is returned.
func readFirstGtidEventFromFile(file *os.File) (mysql.BinlogEvent, error) {
	for {
		binlogEvent, err := readBinlogEventFromFile(file)
		if err != nil {
			return nil, err
		}

		if binlogEvent.IsGTID() {
			return binlogEvent, nil
		}
	}
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
