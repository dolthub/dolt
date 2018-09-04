// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// AUTO-GENERATED CODE. DO NOT EDIT.

package trace

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	cloudtracepb "google.golang.org/genproto/googleapis/devtools/cloudtrace/v2"
)

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	status "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	gstatus "google.golang.org/grpc/status"
)

var _ = io.EOF
var _ = ptypes.MarshalAny
var _ status.Status

type mockTraceServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	cloudtracepb.TraceServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockTraceServer) BatchWriteSpans(ctx context.Context, req *cloudtracepb.BatchWriteSpansRequest) (*emptypb.Empty, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*emptypb.Empty), nil
}

func (s *mockTraceServer) CreateSpan(ctx context.Context, req *cloudtracepb.Span) (*cloudtracepb.Span, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*cloudtracepb.Span), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockTrace mockTraceServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	cloudtracepb.RegisterTraceServiceServer(serv, &mockTrace)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		log.Fatal(err)
	}
	go serv.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	clientOpt = option.WithGRPCConn(conn)

	os.Exit(m.Run())
}

func TestTraceServiceBatchWriteSpans(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockTrace.err = nil
	mockTrace.reqs = nil

	mockTrace.resps = append(mockTrace.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var spans []*cloudtracepb.Span = nil
	var request = &cloudtracepb.BatchWriteSpansRequest{
		Name:  formattedName,
		Spans: spans,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.BatchWriteSpans(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockTrace.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestTraceServiceBatchWriteSpansError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockTrace.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var spans []*cloudtracepb.Span = nil
	var request = &cloudtracepb.BatchWriteSpansRequest{
		Name:  formattedName,
		Spans: spans,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.BatchWriteSpans(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestTraceServiceCreateSpan(t *testing.T) {
	var name2 string = "name2-1052831874"
	var spanId2 string = "spanId2-643891741"
	var parentSpanId string = "parentSpanId-1757797477"
	var expectedResponse = &cloudtracepb.Span{
		Name:         name2,
		SpanId:       spanId2,
		ParentSpanId: parentSpanId,
	}

	mockTrace.err = nil
	mockTrace.reqs = nil

	mockTrace.resps = append(mockTrace.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/traces/%s/spans/%s", "[PROJECT]", "[TRACE]", "[SPAN]")
	var spanId string = "spanId-2011840976"
	var displayName *cloudtracepb.TruncatableString = &cloudtracepb.TruncatableString{}
	var startTime *timestamppb.Timestamp = &timestamppb.Timestamp{}
	var endTime *timestamppb.Timestamp = &timestamppb.Timestamp{}
	var request = &cloudtracepb.Span{
		Name:        formattedName,
		SpanId:      spanId,
		DisplayName: displayName,
		StartTime:   startTime,
		EndTime:     endTime,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateSpan(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockTrace.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestTraceServiceCreateSpanError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockTrace.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/traces/%s/spans/%s", "[PROJECT]", "[TRACE]", "[SPAN]")
	var spanId string = "spanId-2011840976"
	var displayName *cloudtracepb.TruncatableString = &cloudtracepb.TruncatableString{}
	var startTime *timestamppb.Timestamp = &timestamppb.Timestamp{}
	var endTime *timestamppb.Timestamp = &timestamppb.Timestamp{}
	var request = &cloudtracepb.Span{
		Name:        formattedName,
		SpanId:      spanId,
		DisplayName: displayName,
		StartTime:   startTime,
		EndTime:     endTime,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateSpan(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
