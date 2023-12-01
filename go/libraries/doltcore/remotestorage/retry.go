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
	"net/http"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var HttpError = errors.New("http")

// ProcessHttpResp converts an http.Response, and error into a RetriableCallState
func processHttpResp(resp *http.Response, err error) error {
	if errors.Is(err, context.Canceled) {
		return backoff.Permanent(err)
	}

	if err == nil {
		if resp.StatusCode/100 == 2 {
			return nil
		}

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
