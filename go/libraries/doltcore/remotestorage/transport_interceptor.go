// Copyright 2025 Dolthub, Inc.
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
	"net/http"
	"sync"
)

// TransportInterceptor wraps the default transport and allows injecting
// custom transports for specific hosts
type TransportInterceptor struct {
	baseTransport    http.RoundTripper
	customTransports map[string]http.RoundTripper
	mu               sync.RWMutex
}

// globalInterceptor is the singleton interceptor used by the global HTTP client
var globalInterceptor = &TransportInterceptor{
	baseTransport:    defaultTransport, // Use the existing defaultTransport
	customTransports: make(map[string]http.RoundTripper),
}

// RoundTrip implements http.RoundTripper
func (t *TransportInterceptor) RoundTrip(req *http.Request) (*http.Response, error) {
	// Check if we have a custom transport for this host
	t.mu.RLock()
	if customTransport, ok := t.customTransports[req.Host]; ok {
		t.mu.RUnlock()
		return customTransport.RoundTrip(req)
	}
	t.mu.RUnlock()
	
	// Fall back to the base transport
	return t.baseTransport.RoundTrip(req)
}

// RegisterCustomTransport registers a custom transport for a specific host.
// This allows routing requests to that host through a custom transport
// (e.g., SMUX streams for SSH connections).
func RegisterCustomTransport(host string, transport http.RoundTripper) {
	globalInterceptor.mu.Lock()
	defer globalInterceptor.mu.Unlock()
	globalInterceptor.customTransports[host] = transport
}

// UnregisterCustomTransport removes a custom transport for a host
func UnregisterCustomTransport(host string) {
	globalInterceptor.mu.Lock()
	defer globalInterceptor.mu.Unlock()
	delete(globalInterceptor.customTransports, host)
}