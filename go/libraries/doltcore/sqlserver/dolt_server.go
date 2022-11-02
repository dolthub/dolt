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
	"sync"

	"github.com/dolthub/go-mysql-server/server"
)

var mySQLServer *server.Server
var mySQLServerMutex sync.Mutex

// RunningInServerMode returns true if the current process is running a SQL server.
func RunningInServerMode() bool {
	mySQLServerMutex.Lock()
	defer mySQLServerMutex.Unlock()
	return mySQLServer != nil
}

// GetRunningServer returns the Server instance running in this process, or nil if no SQL server is running.
func GetRunningServer() *server.Server {
	mySQLServerMutex.Lock()
	defer mySQLServerMutex.Unlock()
	return mySQLServer
}

// SetRunningServer sets the specified Server as the running SQL server for this process.
func SetRunningServer(server *server.Server) {
	mySQLServerMutex.Lock()
	defer mySQLServerMutex.Unlock()
	mySQLServer = server
}
