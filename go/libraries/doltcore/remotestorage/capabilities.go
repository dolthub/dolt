// Copyright 2026 Dolthub, Inc.
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

package remotestorage

import (
	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
)

// clientCapabilities is the set of capabilities this dolt client
// declares on every outbound remotesapi request that carries a
// client_capabilities field. Centralized here so the answer to
// "what does this client advertise?" lives in one place and
// adding a new capability is a one-line change. Treat as
// immutable once the package is loaded.
var clientCapabilities = []remotesapi.ClientCapability{
	remotesapi.ClientCapability_CLIENT_CAPABILITY_HTTP2_DOWNLOAD,
}
