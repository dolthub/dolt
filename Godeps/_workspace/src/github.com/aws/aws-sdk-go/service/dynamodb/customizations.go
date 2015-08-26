package dynamodb

import (
	"bytes"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"strconv"
	"time"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/request"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/service"
)

type retryer struct {
	service.DefaultRetryer
}

func (d retryer) RetryRules(r *request.Request) time.Duration {
	delay := time.Duration(math.Pow(2, float64(r.RetryCount))) * 50
	return delay * time.Millisecond
}

func init() {
	initService = func(s *service.Service) {
		s.DefaultMaxRetries = 10
		s.Retryer = retryer{service.DefaultRetryer{s}}

		s.Handlers.Build.PushBack(disableCompression)
		s.Handlers.Unmarshal.PushFront(validateCRC32)
	}
}

func drainBody(b io.ReadCloser) (out *bytes.Buffer, err error) {
	var buf bytes.Buffer
	if _, err = buf.ReadFrom(b); err != nil {
		return nil, err
	}
	if err = b.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

func disableCompression(r *request.Request) {
	r.HTTPRequest.Header.Set("Accept-Encoding", "identity")
}

func validateCRC32(r *request.Request) {
	if r.Error != nil {
		return // already have an error, no need to verify CRC
	}

	// Checksum validation is off, skip
	if aws.BoolValue(r.Service.Config.DisableComputeChecksums) {
		return
	}

	// Try to get CRC from response
	header := r.HTTPResponse.Header.Get("X-Amz-Crc32")
	if header == "" {
		return // No header, skip
	}

	expected, err := strconv.ParseUint(header, 10, 32)
	if err != nil {
		return // Could not determine CRC value, skip
	}

	buf, err := drainBody(r.HTTPResponse.Body)
	if err != nil { // failed to read the response body, skip
		return
	}

	// Reset body for subsequent reads
	r.HTTPResponse.Body = ioutil.NopCloser(bytes.NewReader(buf.Bytes()))

	// Compute the CRC checksum
	crc := crc32.ChecksumIEEE(buf.Bytes())

	if crc != uint32(expected) {
		// CRC does not match, set a retryable error
		r.Retryable = aws.Bool(true)
		r.Error = awserr.New("CRC32CheckFailed", "CRC32 integrity check failed", nil)
	}
}
