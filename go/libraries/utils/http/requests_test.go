package http

import (
	nethttp "net/http"
	"testing"
)

func TestIsLocalRequest(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		headers    map[string]string
		wantLocal  bool
		wantErr    bool
	}{
		{
			name:       "ipv4 loopback",
			remoteAddr: "127.0.0.1:12345",
			headers:    nil,
			wantLocal:  true,
		},
		{
			name:       "ipv6 loopback",
			remoteAddr: "[::1]:54321",
			headers:    nil,
			wantLocal:  true,
		},
		{
			name:       "unix socket path",
			remoteAddr: "/var/run/socket",
			headers:    nil,
			wantLocal:  true,
		},
		{
			name:       "abstract unix socket",
			remoteAddr: "@abstractsocket",
			headers:    nil,
			wantLocal:  true,
		},
		{
			name:       "non-local ip",
			remoteAddr: "192.168.1.10:8080",
			headers:    nil,
			wantLocal:  false,
		},
		{
			name:       "forwarded header present",
			remoteAddr: "127.0.0.1:1111",
			headers: map[string]string{
				"X-Forwarded-For": "203.0.113.1",
			},
			wantLocal: false,
		},
		{
			name:       "malformed remote addr (no IP)",
			remoteAddr: "not-an-ip",
			headers:    nil,
			wantErr:    true,
		},
		{
			name:       "hostname remote addr",
			remoteAddr: "localhost:9999",
			headers:    nil,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := &nethttp.Request{
				Header:     make(nethttp.Header),
				RemoteAddr: tc.remoteAddr,
			}
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}

			got, err := IsLocalRequest(req)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error but got nil (got=%v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.wantLocal {
				t.Fatalf("unexpected result: got=%v want=%v", got, tc.wantLocal)
			}
		})
	}
}
