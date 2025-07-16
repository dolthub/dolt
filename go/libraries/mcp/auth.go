package mcp

import (
	"fmt"
	"net/http"
)

type sqlIdentityAuthenticationHTTPMiddleware struct {
	next http.Handler
}

var _ http.Handler = &sqlIdentityAuthenticationHTTPMiddleware{}

func SqlIdentityAuthenticationHTTPMiddleware(next http.Handler) http.Handler {
	if next == nil {
		panic("must supply next http.Handler")
	}
	return &sqlIdentityAuthenticationHTTPMiddleware{next}
}

func (a *sqlIdentityAuthenticationHTTPMiddleware) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	// TODO: add auth stuff here
	a.next.ServeHTTP(responseWriter, request)
}

type noopAuthenticationHTTPMiddleware struct {
	next http.Handler
}

var _ http.Handler = &noopAuthenticationHTTPMiddleware{}

func NoopAuthenticationHTTPMiddleware(next http.Handler) http.Handler {
	if next == nil {
		panic("must supply next http.Handler")
	}
	return &noopAuthenticationHTTPMiddleware{next}
}

func (a *noopAuthenticationHTTPMiddleware) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	// TODO: add auth stuff here
	fmt.Println("DUSTIN: received request in noop middleware")
	a.next.ServeHTTP(responseWriter, request)
}
