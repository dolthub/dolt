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

	"github.com/cenkalti/backoff"
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
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)

	if !ok {
		return err
	}

	switch st.Code() {
	case codes.OK:
		return nil

	case codes.Canceled,
		codes.Unknown,
		codes.DeadlineExceeded,
		codes.Aborted,
		codes.Internal,
		codes.DataLoss,
		codes.ResourceExhausted,
		codes.Unavailable:
		return err

	case codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.FailedPrecondition,
		codes.Unimplemented,
		codes.OutOfRange,
		codes.Unauthenticated:
		return backoff.Permanent(err)
	}

	return err
}
