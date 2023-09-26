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

package doltdb

// Environment variables used by the Dolt CLI and server
const (
	EnvPassword                 = "DOLT_CLI_PASSWORD"
	EnvUser                     = "DOLT_CLI_USER"
	EnvSilenceUserReqForTesting = "DOLT_SILENCE_USER_REQ_FOR_TESTING"
)

const EnvOpenAiKey = "OPENAI_API_KEY"
const EnvDoltRemotePassword = "DOLT_REMOTE_PASSWORD"
const EnvEditor = "EDITOR"
const EnvSqlDebugLogVerbose = "DOLT_SQL_DEBUG_LOG_VERBOSE"
const EnvSqlDebugLog = "DOLT_SQL_DEBUG_LOG"
const EnvHome = "HOME"
const EnvDoltRootPath = "DOLT_ROOT_PATH"
const EnvRemoteVersionDownloadStats = "DOLT_REMOTE_VERBOSE_DOWNLOAD_STATS"
const EnvPushLog = "PUSH_LOG"
const EnvDefaultBinFormat = "DOLT_DEFAULT_BIN_FORMAT"

