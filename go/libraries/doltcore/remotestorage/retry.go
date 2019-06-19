package remotestorage

import (
	"fmt"
	"github.com/cenkalti/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
)

var RetriableHTTPStatusCodes = map[int]struct{}{
	http.StatusRequestTimeout:      {},
	http.StatusTooEarly:            {},
	http.StatusTooManyRequests:     {},
	http.StatusInternalServerError: {},
	http.StatusBadGateway:          {},
	http.StatusServiceUnavailable:  {},
	http.StatusGatewayTimeout:      {},
}

// ProcessHttpResp converts an http.Response, and error into a RetriableCallState
func processHttpResp(resp *http.Response, err error) error {
	if err == nil {
		if resp.StatusCode/100 == 2 {
			return nil
		}

		httpErr := fmt.Errorf("error: http %d", resp.StatusCode)

		if _, ok := RetriableHTTPStatusCodes[resp.StatusCode]; ok {
			return httpErr
		}

		return backoff.Permanent(httpErr)
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
