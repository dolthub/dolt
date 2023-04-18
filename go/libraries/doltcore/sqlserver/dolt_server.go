// Copyright 2022 Dolthub, Inc.
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
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"sync"

	"github.com/dolthub/go-mysql-server/server"
)

var lockedDetails *serverAndLockfile
var mutex sync.Mutex

// Struct for holding a *server.Server and a *env.DBLock
type serverAndLockfile struct {
	Server   *server.Server
	Lockfile *env.DBLock
}

// RunningInServerMode returns true if the current process is running a SQL server.
func RunningInServerMode() bool {
	mutex.Lock()
	defer mutex.Unlock()
	return lockedDetails != nil
}

// GetRunningServer returns the Server instance running in this process, or nil if no SQL server is running.
func GetRunningServer() (*server.Server, *env.DBLock) {
	mutex.Lock()
	defer mutex.Unlock()

	if lockedDetails == nil {
		return nil, nil
	}
	return lockedDetails.Server, lockedDetails.Lockfile
}

// SetRunningServer sets the specified Server as the running SQL server for this process.
func SetRunningServer(server *server.Server, lockfile *env.DBLock) error {
	if server == nil || lockfile == nil {
		return fmt.Errorf("server and lockfile must be non-nil")
	}

	mutex.Lock()
	defer mutex.Unlock()
	lockedDetails = &serverAndLockfile{Server: server, Lockfile: lockfile}

	return nil
}

func UnsetRunningServer() {
	mutex.Lock()
	defer mutex.Unlock()
	lockedDetails = nil
}
