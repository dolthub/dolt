// Copyright 2019 Dolthub, Inc.
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
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"strings"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var HttpError = errors.New("http")

// getStackTrace returns a formatted stack trace
func getStackTrace() string {
	buf := make([]byte, 1024*4)
	n := runtime.Stack(buf, false)
	stackLines := strings.Split(string(buf[:n]), "\n")
	// Skip first few lines (runtime.Stack, getStackTrace, processHttpResp)
	if len(stackLines) > 6 {
		return strings.Join(stackLines[6:], "\n")
	}
	return string(buf[:n])
}

// ProcessHttpResp converts an http.Response, and error into a RetriableCallState
func processHttpResp(resp *http.Response, err error) error {
	if errors.Is(err, context.Canceled) {
		return backoff.Permanent(err)
	}

	if err == nil {
		if resp.StatusCode/100 == 2 {
			return nil
		}

		// DEBUG: Enhanced logging for HTTP errors
		log.Printf("DEBUG: HTTP Error Details:")
		log.Printf("  Status Code: %d", resp.StatusCode)
		log.Printf("  Status: %s", resp.Status)
		log.Printf("  URL: %s", resp.Request.URL.String())
		log.Printf("  Method: %s", resp.Request.Method)
		
		// Log request headers
		log.Printf("  Request Headers:")
		for k, v := range resp.Request.Header {
			log.Printf("    %s: %s", k, strings.Join(v, ", "))
		}
		
		// Log response headers
		log.Printf("  Response Headers:")
		for k, v := range resp.Header {
			log.Printf("    %s: %s", k, strings.Join(v, ", "))
		}
		
		// Log response body (first 2KB)
		if resp.Body != nil {
			bodyBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 2048))
			if readErr == nil && len(bodyBytes) > 0 {
				log.Printf("  Response Body (first 2KB): %s", string(bodyBytes))
			} else if readErr != nil {
				log.Printf("  Error reading response body: %v", readErr)
			}
		}
		
		// Log stack trace to see where this error originated
		log.Printf("  Stack Trace:\n%s", getStackTrace())

		return fmt.Errorf("error: %w %d", HttpError, resp.StatusCode)
	}

	return err
}

// ProcessGrpcErr converts an error from a Grpc call into a RetriableCallState
func processGrpcErr(err error) error {
	st, _ := status.FromError(err)
	if statusCodeIsPermanentError(st) {
		return backoff.Permanent(err)
	}
	return err
}

func statusCodeIsPermanentError(s *status.Status) bool {
	if s == nil {
		return false
	}
	switch s.Code() {
	case codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.FailedPrecondition,
		codes.Unimplemented,
		codes.OutOfRange,
		codes.Unauthenticated:
		return true
	}
	return false
}
