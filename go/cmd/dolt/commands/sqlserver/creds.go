// Copyright 2023 Dolthub, Inc.
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

package sqlserver

import (
	"errors"
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const ServerLocalCredsFile = "sql-server.info"

// ErrInvalidLockFileFormat indicates a malformed server info file.
var ErrInvalidLockFileFormat = errors.New("invalid lock file format")

// LocalCreds contains connection information for accessing a locally
// running sql-server from a CLI process. It contains the pid of the
// process that created the lockfile, the port the server listens on,
// and the shared secret for local authentication.
//
// Pid is used to detect server liveness and automatically clean up
// stale credentials files when the server process has terminated.
type LocalCreds struct {
	Pid    int
	Port   int
	Secret string
}

// NewLocalCreds initializes a new LocalCreds struct for a sql-server
// listening on the given port, populated with the current process ID
// and a newly generated authentication secret.
func NewLocalCreds(port int) *LocalCreds {
	return &LocalCreds{os.Getpid(), port, uuid.New().String()}
}

// LocalCredsFilePath returns the relative path to the server info file
// within the dbfactory.DoltDir.
func LocalCredsFilePath() string {
	return filepath.Join(dbfactory.DoltDir, ServerLocalCredsFile)
}

// RemoveLocalCreds makes a best-effort attempt to remove the server
// info file from the provided |fs|.
func RemoveLocalCreds(fs filesys.Filesys) {
	credsFilePath, err := fs.Abs(LocalCredsFilePath())
	if err != nil {
		return
	}
	_ = fs.Delete(credsFilePath, false)
}

// WriteLocalCreds writes a server info file containing the serialized
// |creds| to the dbfactory.DoltDir of the provided |fs|.
func WriteLocalCreds(fs filesys.Filesys, creds *LocalCreds) error {
	// if the DoltDir doesn't exist, create it.
	doltDir, err := fs.Abs(dbfactory.DoltDir)
	if err != nil {
		return err
	}
	err = fs.MkDirs(doltDir)
	if err != nil {
		return err
	}

	credsFile, err := fs.Abs(LocalCredsFilePath())
	if err != nil {
		return err
	}

	portStr := strconv.Itoa(creds.Port)
	if creds.Port < 0 {
		portStr = "-"
	}

	return fs.WriteFile(credsFile, []byte(fmt.Sprintf("%d:%s:%s", creds.Pid, portStr, creds.Secret)), 0600)
}

// FindAndLoadLocalCreds traverses upward from |fs| to locate, parse, and
// validate a server info file.
//
// If a credentials file is found but its recorded process is dead, the
// stale file is removed automatically. If no valid credentials file is
// found, nil is returned.
func FindAndLoadLocalCreds(fs filesys.Filesys) (creds *LocalCreds, err error) {
	root, err := fs.Abs(".")
	if err != nil {
		return nil, err
	}
	for root != "" && root[len(root)-1] != filepath.Separator {
		creds, err := LoadLocalCreds(fs)
		if err == nil {
			if !ProcessExists(creds.Pid) {
				RemoveLocalCreds(fs)
				return nil, nil
			}
			return creds, nil
		}
		// If we have an error that is not ErrNotExist, for example, a
		// permission error opening the credentials file, or an error
		// indicating that the contents of the file were malformed, go
		// ahead and return the error and terminate our search here.
		if !errors.Is(err, iofs.ErrNotExist) {
			return nil, err
		}
		fs, err = fs.WithWorkingDir("..")
		if err != nil {
			return nil, err
		}
		root, err = fs.Abs(".")
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// LoadLocalCreds reads and parses the server info file from the
// dbfactory.DoltDir directory of the given |fs|.
func LoadLocalCreds(fs filesys.Filesys) (creds *LocalCreds, err error) {
	rd, err := fs.OpenForRead(LocalCredsFilePath())
	if err != nil {
		return nil, err
	}
	defer rd.Close()

	b := make([]byte, 256)
	n, err := rd.Read(b)
	if err != nil {
		return nil, err
	}

	data := strings.TrimSpace(string(b[:n]))

	parts := strings.Split(data, ":")
	if len(parts) != 3 {
		return nil, ErrInvalidLockFileFormat
	}

	pid, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, err
	}
	port := -1
	if parts[1] != "-" {
		port, err = strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		}
	}
	secret := parts[2]
	return &LocalCreds{Pid: pid, Port: port, Secret: secret}, nil
}
