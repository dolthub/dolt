package retry

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProcessGrpcErr converts an error from a Grpc call into a RetriableCallState
func ProcessGrpcErr(err error) RetriableCallState {
	if err == nil {
		return Success
	}

	st, ok := status.FromError(err)

	if !ok {
		return RetriableFailure
	}

	switch st.Code() {
	case codes.OK:
		return Success

	case codes.Canceled,
		codes.Unknown,
		codes.DeadlineExceeded,
		codes.Aborted,
		codes.Internal,
		codes.DataLoss,
		codes.Unavailable:
		return RetriableFailure

	case codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.ResourceExhausted,
		codes.FailedPrecondition,
		codes.Unimplemented,
		codes.OutOfRange,
		codes.Unauthenticated:
		return PermanentFailure
	}

	return RetriableFailure
}
