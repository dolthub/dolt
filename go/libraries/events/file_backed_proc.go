// Copyright 2019 Dolthub, Inc.
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

package events

import (
	"crypto/md5"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"google.golang.org/protobuf/proto"

	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	filesys "github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

const (
	eventsDir    = "eventsData"
	localPath    = "temp"
	evtDataExt   = ".devts"
	doltLockFile = "dolt.lock"
)

// fileNamingFunc is the signature used for functions used to create
// dynamic file names in the FileBackedProc
type fileNamingFunc func(bytes []byte) string

// fileChecingFunc is the signature used for functions used to authenticate
// filenames created by the fileNamingFunc
type fileCheckingFunc func(data []byte, path string) (bool, error)

// MD5Str returns the standard base64 encoding of the md5 hash with padding
func MD5Str(bytes []byte) string {
	md5Bytes := md5.Sum(bytes)
	str := base64.StdEncoding.EncodeToString(md5Bytes[:])
	return str
}

// MD5StrUrl returns the base64 url encoding of the md5 hash with the padding removed
func MD5StrUrl(bytes []byte) string {
	md5Bytes := md5.Sum(bytes)
	str := base64.URLEncoding.EncodeToString(md5Bytes[:])
	return str[:22]
}

// MD5FileNamer names files after the base64 url encoding of the md5 hash of the contents of the file
func MD5FileNamer(bytes []byte) string {
	return MD5StrUrl(bytes) + evtDataExt
}

// CheckFilenameMD5 is the fileNamingFunc used when instantiating a FileBackedProc
func CheckFilenameMD5(data []byte, path string) (bool, error) {
	filename := filepath.Base(path)
	ext := filepath.Ext(filename)

	if ext != evtDataExt {
		return false, nil
	}

	md5FromFilename := filename[:len(filename)-len(ext)]
	md5FromData := MD5StrUrl(data)

	if md5FromFilename != md5FromData {
		return false, nil
	}

	return true, nil
}

var errEventsDirNotExists = errors.New("no events data directory exists")

// eventsDataDir is the directory used to store the events requests files
type eventsDataDir struct {
	fs   filesys.Filesys
	path string
}

// newEventsDataDir creates a new eventsDataDir
func newEventsDataDir(fs filesys.Filesys, homeDir string, doltDir string) *eventsDataDir {
	path := filepath.Join(homeDir, doltDir, eventsDir)

	return &eventsDataDir{fs: fs, path: path}
}

//  MakeEventsDir creates a new events data dir in the main dolt dir
func (evd *eventsDataDir) MakeEventsDir() error {
	if exists, _ := evd.fs.Exists(evd.path); !exists {
		if err := evd.fs.MkDirs(evd.path); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (evd *eventsDataDir) getPath() string {
	return evd.path
}

// FileBackedProc writes events requests to files in an events data dir
type FileBackedProc struct {
	ed           *eventsDataDir
	namingFunc   fileNamingFunc
	CheckingFunc fileCheckingFunc
	LockPath     string
}

// NewFileBackedProc creates a new FileBackedProc
func NewFileBackedProc(fs filesys.Filesys, userHomeDir string, doltDir string, nf fileNamingFunc, cf fileCheckingFunc) *FileBackedProc {
	eventsDataDir := newEventsDataDir(fs, userHomeDir, doltDir)

	if err := eventsDataDir.MakeEventsDir(); err != nil {
		panic(err)
	}

	lp := filepath.Join(eventsDataDir.getPath(), doltLockFile)

	exists, _ := fs.Exists(lp)

	if !exists {
		if err := fs.WriteFile(lp, []byte("lockfile for dolt \n")); err != nil {
			panic(err)
		}
	}

	return &FileBackedProc{ed: eventsDataDir, namingFunc: nf, CheckingFunc: cf, LockPath: lp}
}

// renameFile renames the request events file using the namingFunc
func (fbp *FileBackedProc) renameFile(dir string, oldName string) error {
	oldPath := filepath.Join(dir, oldName)

	data, err := fbp.ed.fs.ReadFile(oldPath)
	if err != nil {
		return err
	}

	filename := fbp.namingFunc(data)
	newPath := filepath.Join(dir, filename)

	if err := fbp.ed.fs.MoveFile(oldPath, newPath); err != nil {
		return nil
	}

	return nil
}

// EventsDirExists returns true iff the events data dir exists
func (fbp *FileBackedProc) EventsDirExists() bool {
	if exists, _ := fbp.ed.fs.Exists(fbp.ed.getPath()); exists {
		return true
	}
	return false
}

// GetEventsDirPath returns the path to the events data dir
func (fbp *FileBackedProc) GetEventsDirPath() string {
	return fbp.ed.getPath()
}

// GetFileSys returns the current filesys being used
func (fbp *FileBackedProc) GetFileSys() filesys.Filesys {
	return fbp.ed.fs
}

// WriteEvents writes events requests to the events data dir
func (fbp *FileBackedProc) WriteEvents(version string, evts []*eventsapi.ClientEvent) error {
	if len(evts) < 1 {
		return nil
	}

	var plat eventsapi.Platform
	switch strings.ToLower(runtime.GOOS) {
	case "darwin":
		plat = eventsapi.Platform_DARWIN
	case "linux":
		plat = eventsapi.Platform_LINUX
	case "windows":
		plat = eventsapi.Platform_WINDOWS
	}

	if dirExists := fbp.EventsDirExists(); dirExists {
		eventsPath := fbp.ed.getPath()
		tempFilename := filepath.Join(eventsPath, localPath)

		f, err := fbp.ed.fs.OpenForWrite(tempFilename, os.ModePerm)

		if err != nil {
			return err
		}

		req := &eventsapi.LogEventsRequest{
			MachineId: getMachineID(),
			Version:   version,
			Platform:  plat,
			Events:    evts,
			App:       eventsapi.AppID_APP_DOLT,
		}

		data, err := proto.Marshal(req)
		if err != nil {
			return err
		}

		if err := iohelp.WriteAll(f, data); err != nil {
			return err
		}

		if err := f.Close(); err != nil {
			return err
		}

		if err = fbp.renameFile(eventsPath, localPath); err != nil {
			return err
		}

		return nil
	}

	return errEventsDirNotExists
}
