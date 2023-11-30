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

// LocalCreds is a struct that contains information about how to access the
// locally running server for a CLI process which wants to operate against a
// database while a sql-server process is running. It contains the pid of the
// process that created the lockfile, the port that the server is running on
// and the password to be used connecting to the server.
//
// Pid is a legacy field which is retained for compatibility but no longer
// influences the behavior of the running program(s).
type LocalCreds struct {
	Pid    int
	Port   int
	Secret string
}

func NewLocalCreds(port int) *LocalCreds {
	return &LocalCreds{os.Getpid(), port, uuid.New().String()}
}

// Best effort attempt to remove local creds file persisted as the Filesys
// rooted there.
func RemoveLocalCreds(fs filesys.Filesys) {
	credsFilePath, err := fs.Abs(filepath.Join(dbfactory.DoltDir, ServerLocalCredsFile))
	if err != nil {
		return
	}
	_ = fs.Delete(credsFilePath, false)
}

// WriteLocalCreds writes a file containing the contents of LocalCreds to the
// DoltDir rooted at the provided Filesys.
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

	credsFile, err := fs.Abs(filepath.Join(dbfactory.DoltDir, ServerLocalCredsFile))
	if err != nil {
		return err
	}

	portStr := strconv.Itoa(creds.Port)
	if creds.Port < 0 {
		portStr = "-"
	}

	return fs.WriteFile(credsFile, []byte(fmt.Sprintf("%d:%s:%s", creds.Pid, portStr, creds.Secret)), 0600)
}

// Starting at `fs`, look for the a ServerLocalCredsFile in the .dolt directory
// of this directory and every parent directory, until we find one. When we
// find one, we return its contents if we can open and parse it successfully.
// Otherwise, we return an error associated with attempting to read it. If we
// do not find anything all the way up to the root of the filesystem, returns
// `nil` *LocalCreds and a `nil` error.
func FindAndLoadLocalCreds(fs filesys.Filesys) (creds *LocalCreds, err error) {
	root, err := fs.Abs(".")
	if err != nil {
		return nil, err
	}
	for root != "" && root[len(root)-1] != '/' {
		creds, err := LoadLocalCreds(fs)
		if err == nil {
			return creds, err
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

func LoadLocalCreds(fs filesys.Filesys) (creds *LocalCreds, err error) {
	rd, err := fs.OpenForRead(filepath.Join(dbfactory.DoltDir, ServerLocalCredsFile))
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
		return nil, fmt.Errorf("invalid lock file format")
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
