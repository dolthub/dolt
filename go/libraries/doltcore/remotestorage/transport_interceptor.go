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

// TransportInterceptor wraps the default HTTP transport and allows injecting
// custom transports for specific hosts. This enables routing HTTP requests
// through non-HTTP transports (e.g., SMUX streams for SSH connections) based
// on the request's Host header.
type TransportInterceptor struct {
	baseTransport    http.RoundTripper
	customTransports map[string]http.RoundTripper
	mu               sync.RWMutex
}

// globalInterceptor is the singleton interceptor used by the global HTTP client.
var globalInterceptor = &TransportInterceptor{
	baseTransport:    defaultTransport,
	customTransports: make(map[string]http.RoundTripper),
}

// RoundTrip implements http.RoundTripper. It checks for a custom transport
// registered for the request's Host, falling back to the base transport.
func (t *TransportInterceptor) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.RLock()
	customTransport, ok := t.customTransports[req.Host]
	t.mu.RUnlock()

	if ok {
		return customTransport.RoundTrip(req)
	}
	return t.baseTransport.RoundTrip(req)
}

// RegisterCustomTransport registers a custom transport for a specific host.
// Requests to this host will be routed through the custom transport instead
// of the default HTTP transport.
func RegisterCustomTransport(host string, transport http.RoundTripper) {
	globalInterceptor.mu.Lock()
	defer globalInterceptor.mu.Unlock()
	globalInterceptor.customTransports[host] = transport
}

// UnregisterCustomTransport removes a custom transport for a host, reverting
// requests to that host back to the default HTTP transport.
func UnregisterCustomTransport(host string) {
	globalInterceptor.mu.Lock()
	defer globalInterceptor.mu.Unlock()
	delete(globalInterceptor.customTransports, host)
}
