package http

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

// IsLocalRequest determines if an HTTP request originated from a local source.
// It checks for common proxy headers to rule out forwarded requests and
// inspects the RemoteAddr to see if it corresponds to a loopback address
// or a unix socket path.
func IsLocalRequest(r *http.Request) (bool, error) {
	// If any common proxy/forwarding headers are present, consider the request forwarded.
	proxyHeaders := []string{
		"X-Forwarded-For",
		"X-Real-IP",
		"Forwarded",
		"Via",
		"True-Client-IP",
		"X-Cluster-Client-Ip",
	}
	for _, h := range proxyHeaders {
		if v := r.Header.Get(h); v != "" {
			return false, nil
		}
	}

	remote := r.RemoteAddr
	if remote == "" {
		return false, fmt.Errorf("empty RemoteAddr")
	}

	// remote can be "host:port" or a raw address. Try SplitHostPort first.
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		// If SplitHostPort fails, treat the whole value as the host (could be a unix socket path or raw IP).
		host = remote
	}

	// Treat obvious unix-socket paths as local.
	if strings.HasPrefix(host, "/") || strings.HasPrefix(host, "@") {
		return true, nil
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false, fmt.Errorf("invalid remote IP: %s", host)
	}

	// Consider loopback addresses local.
	if ip.IsLoopback() {
		return true, nil
	}
	return false, nil
}
