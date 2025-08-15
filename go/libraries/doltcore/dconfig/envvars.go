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

package dconfig

const (
	EnvPassword                      = "DOLT_CLI_PASSWORD"
	EnvUser                          = "DOLT_CLI_USER"
	EnvSilenceUserReqForTesting      = "DOLT_SILENCE_USER_REQ_FOR_TESTING"
	EnvOpenAiKey                     = "OPENAI_API_KEY"
	EnvDoltRemotePassword            = "DOLT_REMOTE_PASSWORD"
	EnvEditor                        = "EDITOR"
	EnvSqlDebugLogVerbose            = "DOLT_SQL_DEBUG_LOG_VERBOSE"
	EnvSqlDebugLog                   = "DOLT_SQL_DEBUG_LOG"
	EnvHome                          = "HOME"
	EnvDoltRootPath                  = "DOLT_ROOT_PATH"
	EnvRemoteVersionDownloadStats    = "DOLT_REMOTE_VERBOSE_DOWNLOAD_STATS"
	EnvPushLog                       = "PUSH_LOG"
	EnvDefaultBinFormat              = "DOLT_DEFAULT_BIN_FORMAT"
	EnvTestForceOpenEditor           = "DOLT_TEST_FORCE_OPEN_EDITOR"
	EnvDisableChunkJournal           = "DOLT_DISABLE_CHUNK_JOURNAL"
	EnvDisableReflog                 = "DOLT_DISABLE_REFLOG"
	EnvReflogRecordLimit             = "DOLT_REFLOG_RECORD_LIMIT"
	EnvOssEndpoint                   = "OSS_ENDPOINT"
	EnvOssAccessKeyID                = "OSS_ACCESS_KEY_ID"
	EnvOssAccessKeySecret            = "OSS_ACCESS_KEY_SECRET"
	EnvVerboseAssertTableFilesClosed = "DOLT_VERBOSE_ASSERT_TABLE_FILES_CLOSED"
	EnvDisableGcProcedure            = "DOLT_DISABLE_GC_PROCEDURE"
	EnvEditTableBufferRows           = "DOLT_EDIT_TABLE_BUFFER_ROWS"
	EnvDisableFixedAccess            = "DOLT_DISABLE_FIXED_ACCESS"
	EnvDoltAssistAgree               = "DOLT_ASSIST_AGREE"
	EnvDoltAuthorDate                = "DOLT_AUTHOR_DATE"
	EnvDoltCommitterDate             = "DOLT_COMMITTER_DATE"
	EnvDbNameReplace                 = "DOLT_DBNAME_REPLACE"
	EnvDoltRootHost                  = "DOLT_ROOT_HOST"
	EnvDoltRootPassword              = "DOLT_ROOT_PASSWORD"

	// If set, must be "kill_connections" or "session_aware"
	// Will go away after session_aware is made default-and-only.
	EnvGCSafepointControllerChoice = "DOLT_GC_SAFEPOINT_CONTROLLER_CHOICE"

	// Used for tests. If set, Dolt will error if it would rebuild a table's row data.
	EnvAssertNoTableRewrite         = "DOLT_TEST_ASSERT_NO_TABLE_REWRITE"
	EnvAssertNoInMemoryArchiveIndex = "DOLT_TEST_ASSERT_NO_IN_MEMORY_ARCHIVE_INDEX"
)
