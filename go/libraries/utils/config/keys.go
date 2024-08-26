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

package config

var ConfigOptions = map[string]struct{}{
	UserEmailKey:          {},
	UserNameKey:           {},
	UserCreds:             {},
	DoltEditor:            {},
	InitBranchName:        {},
	RemotesApiHostKey:     {},
	RemotesApiHostPortKey: {},
	AddCredsUrlKey:        {},
	DoltLabInsecureKey:    {},
	MetricsDisabled:       {},
	MetricsHost:           {},
	MetricsPort:           {},
	MetricsInsecure:       {},
	PushAutoSetupRemote:   {},
	ProfileKey:            {},
	VersionCheckDisabled:  {},
}

const UserEmailKey = "user.email"

const UserNameKey = "user.name"

const UserCreds = "user.creds"

const DoltEditor = "core.editor"

const InitBranchName = "init.defaultbranch"

const RemotesApiHostKey = "remotes.default_host"

const RemotesApiHostPortKey = "remotes.default_port"

const AddCredsUrlKey = "creds.add_url"

const DoltLabInsecureKey = "doltlab.insecure"

const MetricsDisabled = "metrics.disabled"

const MetricsHost = "metrics.host"

const MetricsPort = "metrics.port"

const MetricsInsecure = "metrics.insecure"

const PushAutoSetupRemote = "push.autosetupremote"

const ProfileKey = "profile"

const VersionCheckDisabled = "versioncheck.disabled"

const SignCommitsKey = "commit.gpgsign"

const GPGSigningKeyKey = "user.signingkey"
