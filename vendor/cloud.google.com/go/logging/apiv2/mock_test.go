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

package logging

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	loggingpb "google.golang.org/genproto/googleapis/logging/v2"
	field_maskpb "google.golang.org/genproto/protobuf/field_mask"
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

type mockLoggingServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	loggingpb.LoggingServiceV2Server

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockLoggingServer) DeleteLog(ctx context.Context, req *loggingpb.DeleteLogRequest) (*emptypb.Empty, error) {
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

func (s *mockLoggingServer) WriteLogEntries(ctx context.Context, req *loggingpb.WriteLogEntriesRequest) (*loggingpb.WriteLogEntriesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.WriteLogEntriesResponse), nil
}

func (s *mockLoggingServer) ListLogEntries(ctx context.Context, req *loggingpb.ListLogEntriesRequest) (*loggingpb.ListLogEntriesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.ListLogEntriesResponse), nil
}

func (s *mockLoggingServer) ListMonitoredResourceDescriptors(ctx context.Context, req *loggingpb.ListMonitoredResourceDescriptorsRequest) (*loggingpb.ListMonitoredResourceDescriptorsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.ListMonitoredResourceDescriptorsResponse), nil
}

func (s *mockLoggingServer) ListLogs(ctx context.Context, req *loggingpb.ListLogsRequest) (*loggingpb.ListLogsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.ListLogsResponse), nil
}

type mockConfigServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	loggingpb.ConfigServiceV2Server

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockConfigServer) ListSinks(ctx context.Context, req *loggingpb.ListSinksRequest) (*loggingpb.ListSinksResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.ListSinksResponse), nil
}

func (s *mockConfigServer) GetSink(ctx context.Context, req *loggingpb.GetSinkRequest) (*loggingpb.LogSink, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.LogSink), nil
}

func (s *mockConfigServer) CreateSink(ctx context.Context, req *loggingpb.CreateSinkRequest) (*loggingpb.LogSink, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.LogSink), nil
}

func (s *mockConfigServer) UpdateSink(ctx context.Context, req *loggingpb.UpdateSinkRequest) (*loggingpb.LogSink, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.LogSink), nil
}

func (s *mockConfigServer) DeleteSink(ctx context.Context, req *loggingpb.DeleteSinkRequest) (*emptypb.Empty, error) {
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

func (s *mockConfigServer) ListExclusions(ctx context.Context, req *loggingpb.ListExclusionsRequest) (*loggingpb.ListExclusionsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.ListExclusionsResponse), nil
}

func (s *mockConfigServer) GetExclusion(ctx context.Context, req *loggingpb.GetExclusionRequest) (*loggingpb.LogExclusion, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.LogExclusion), nil
}

func (s *mockConfigServer) CreateExclusion(ctx context.Context, req *loggingpb.CreateExclusionRequest) (*loggingpb.LogExclusion, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.LogExclusion), nil
}

func (s *mockConfigServer) UpdateExclusion(ctx context.Context, req *loggingpb.UpdateExclusionRequest) (*loggingpb.LogExclusion, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.LogExclusion), nil
}

func (s *mockConfigServer) DeleteExclusion(ctx context.Context, req *loggingpb.DeleteExclusionRequest) (*emptypb.Empty, error) {
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

type mockMetricsServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	loggingpb.MetricsServiceV2Server

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockMetricsServer) ListLogMetrics(ctx context.Context, req *loggingpb.ListLogMetricsRequest) (*loggingpb.ListLogMetricsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.ListLogMetricsResponse), nil
}

func (s *mockMetricsServer) GetLogMetric(ctx context.Context, req *loggingpb.GetLogMetricRequest) (*loggingpb.LogMetric, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.LogMetric), nil
}

func (s *mockMetricsServer) CreateLogMetric(ctx context.Context, req *loggingpb.CreateLogMetricRequest) (*loggingpb.LogMetric, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.LogMetric), nil
}

func (s *mockMetricsServer) UpdateLogMetric(ctx context.Context, req *loggingpb.UpdateLogMetricRequest) (*loggingpb.LogMetric, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*loggingpb.LogMetric), nil
}

func (s *mockMetricsServer) DeleteLogMetric(ctx context.Context, req *loggingpb.DeleteLogMetricRequest) (*emptypb.Empty, error) {
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

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockLogging mockLoggingServer
	mockConfig  mockConfigServer
	mockMetrics mockMetricsServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	loggingpb.RegisterLoggingServiceV2Server(serv, &mockLogging)
	loggingpb.RegisterConfigServiceV2Server(serv, &mockConfig)
	loggingpb.RegisterMetricsServiceV2Server(serv, &mockMetrics)

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

func TestLoggingServiceV2DeleteLog(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockLogging.err = nil
	mockLogging.reqs = nil

	mockLogging.resps = append(mockLogging.resps[:0], expectedResponse)

	var formattedLogName string = fmt.Sprintf("projects/%s/logs/%s", "[PROJECT]", "[LOG]")
	var request = &loggingpb.DeleteLogRequest{
		LogName: formattedLogName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteLog(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLogging.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestLoggingServiceV2DeleteLogError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLogging.err = gstatus.Error(errCode, "test error")

	var formattedLogName string = fmt.Sprintf("projects/%s/logs/%s", "[PROJECT]", "[LOG]")
	var request = &loggingpb.DeleteLogRequest{
		LogName: formattedLogName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteLog(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestLoggingServiceV2WriteLogEntries(t *testing.T) {
	var expectedResponse *loggingpb.WriteLogEntriesResponse = &loggingpb.WriteLogEntriesResponse{}

	mockLogging.err = nil
	mockLogging.reqs = nil

	mockLogging.resps = append(mockLogging.resps[:0], expectedResponse)

	var entries []*loggingpb.LogEntry = nil
	var request = &loggingpb.WriteLogEntriesRequest{
		Entries: entries,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.WriteLogEntries(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLogging.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestLoggingServiceV2WriteLogEntriesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLogging.err = gstatus.Error(errCode, "test error")

	var entries []*loggingpb.LogEntry = nil
	var request = &loggingpb.WriteLogEntriesRequest{
		Entries: entries,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.WriteLogEntries(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestLoggingServiceV2ListLogEntries(t *testing.T) {
	var nextPageToken string = ""
	var entriesElement *loggingpb.LogEntry = &loggingpb.LogEntry{}
	var entries = []*loggingpb.LogEntry{entriesElement}
	var expectedResponse = &loggingpb.ListLogEntriesResponse{
		NextPageToken: nextPageToken,
		Entries:       entries,
	}

	mockLogging.err = nil
	mockLogging.reqs = nil

	mockLogging.resps = append(mockLogging.resps[:0], expectedResponse)

	var formattedResourceNames []string = nil
	var request = &loggingpb.ListLogEntriesRequest{
		ResourceNames: formattedResourceNames,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListLogEntries(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLogging.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Entries[0])
	got := (interface{})(resp)
	var ok bool

	switch want := (want).(type) {
	case proto.Message:
		ok = proto.Equal(want, got.(proto.Message))
	default:
		ok = want == got
	}
	if !ok {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestLoggingServiceV2ListLogEntriesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLogging.err = gstatus.Error(errCode, "test error")

	var formattedResourceNames []string = nil
	var request = &loggingpb.ListLogEntriesRequest{
		ResourceNames: formattedResourceNames,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListLogEntries(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestLoggingServiceV2ListMonitoredResourceDescriptors(t *testing.T) {
	var nextPageToken string = ""
	var resourceDescriptorsElement *monitoredrespb.MonitoredResourceDescriptor = &monitoredrespb.MonitoredResourceDescriptor{}
	var resourceDescriptors = []*monitoredrespb.MonitoredResourceDescriptor{resourceDescriptorsElement}
	var expectedResponse = &loggingpb.ListMonitoredResourceDescriptorsResponse{
		NextPageToken:       nextPageToken,
		ResourceDescriptors: resourceDescriptors,
	}

	mockLogging.err = nil
	mockLogging.reqs = nil

	mockLogging.resps = append(mockLogging.resps[:0], expectedResponse)

	var request *loggingpb.ListMonitoredResourceDescriptorsRequest = &loggingpb.ListMonitoredResourceDescriptorsRequest{}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListMonitoredResourceDescriptors(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLogging.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.ResourceDescriptors[0])
	got := (interface{})(resp)
	var ok bool

	switch want := (want).(type) {
	case proto.Message:
		ok = proto.Equal(want, got.(proto.Message))
	default:
		ok = want == got
	}
	if !ok {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestLoggingServiceV2ListMonitoredResourceDescriptorsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLogging.err = gstatus.Error(errCode, "test error")

	var request *loggingpb.ListMonitoredResourceDescriptorsRequest = &loggingpb.ListMonitoredResourceDescriptorsRequest{}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListMonitoredResourceDescriptors(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestLoggingServiceV2ListLogs(t *testing.T) {
	var nextPageToken string = ""
	var logNamesElement string = "logNamesElement-1079688374"
	var logNames = []string{logNamesElement}
	var expectedResponse = &loggingpb.ListLogsResponse{
		NextPageToken: nextPageToken,
		LogNames:      logNames,
	}

	mockLogging.err = nil
	mockLogging.reqs = nil

	mockLogging.resps = append(mockLogging.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &loggingpb.ListLogsRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListLogs(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLogging.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.LogNames[0])
	got := (interface{})(resp)
	var ok bool

	switch want := (want).(type) {
	case proto.Message:
		ok = proto.Equal(want, got.(proto.Message))
	default:
		ok = want == got
	}
	if !ok {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestLoggingServiceV2ListLogsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLogging.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &loggingpb.ListLogsRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListLogs(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestConfigServiceV2ListSinks(t *testing.T) {
	var nextPageToken string = ""
	var sinksElement *loggingpb.LogSink = &loggingpb.LogSink{}
	var sinks = []*loggingpb.LogSink{sinksElement}
	var expectedResponse = &loggingpb.ListSinksResponse{
		NextPageToken: nextPageToken,
		Sinks:         sinks,
	}

	mockConfig.err = nil
	mockConfig.reqs = nil

	mockConfig.resps = append(mockConfig.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &loggingpb.ListSinksRequest{
		Parent: formattedParent,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListSinks(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockConfig.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Sinks[0])
	got := (interface{})(resp)
	var ok bool

	switch want := (want).(type) {
	case proto.Message:
		ok = proto.Equal(want, got.(proto.Message))
	default:
		ok = want == got
	}
	if !ok {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestConfigServiceV2ListSinksError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockConfig.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &loggingpb.ListSinksRequest{
		Parent: formattedParent,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListSinks(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestConfigServiceV2GetSink(t *testing.T) {
	var name string = "name3373707"
	var destination string = "destination-1429847026"
	var filter string = "filter-1274492040"
	var writerIdentity string = "writerIdentity775638794"
	var includeChildren bool = true
	var expectedResponse = &loggingpb.LogSink{
		Name:            name,
		Destination:     destination,
		Filter:          filter,
		WriterIdentity:  writerIdentity,
		IncludeChildren: includeChildren,
	}

	mockConfig.err = nil
	mockConfig.reqs = nil

	mockConfig.resps = append(mockConfig.resps[:0], expectedResponse)

	var formattedSinkName string = fmt.Sprintf("projects/%s/sinks/%s", "[PROJECT]", "[SINK]")
	var request = &loggingpb.GetSinkRequest{
		SinkName: formattedSinkName,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetSink(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockConfig.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestConfigServiceV2GetSinkError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockConfig.err = gstatus.Error(errCode, "test error")

	var formattedSinkName string = fmt.Sprintf("projects/%s/sinks/%s", "[PROJECT]", "[SINK]")
	var request = &loggingpb.GetSinkRequest{
		SinkName: formattedSinkName,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetSink(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestConfigServiceV2CreateSink(t *testing.T) {
	var name string = "name3373707"
	var destination string = "destination-1429847026"
	var filter string = "filter-1274492040"
	var writerIdentity string = "writerIdentity775638794"
	var includeChildren bool = true
	var expectedResponse = &loggingpb.LogSink{
		Name:            name,
		Destination:     destination,
		Filter:          filter,
		WriterIdentity:  writerIdentity,
		IncludeChildren: includeChildren,
	}

	mockConfig.err = nil
	mockConfig.reqs = nil

	mockConfig.resps = append(mockConfig.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var sink *loggingpb.LogSink = &loggingpb.LogSink{}
	var request = &loggingpb.CreateSinkRequest{
		Parent: formattedParent,
		Sink:   sink,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateSink(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockConfig.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestConfigServiceV2CreateSinkError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockConfig.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var sink *loggingpb.LogSink = &loggingpb.LogSink{}
	var request = &loggingpb.CreateSinkRequest{
		Parent: formattedParent,
		Sink:   sink,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateSink(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestConfigServiceV2UpdateSink(t *testing.T) {
	var name string = "name3373707"
	var destination string = "destination-1429847026"
	var filter string = "filter-1274492040"
	var writerIdentity string = "writerIdentity775638794"
	var includeChildren bool = true
	var expectedResponse = &loggingpb.LogSink{
		Name:            name,
		Destination:     destination,
		Filter:          filter,
		WriterIdentity:  writerIdentity,
		IncludeChildren: includeChildren,
	}

	mockConfig.err = nil
	mockConfig.reqs = nil

	mockConfig.resps = append(mockConfig.resps[:0], expectedResponse)

	var formattedSinkName string = fmt.Sprintf("projects/%s/sinks/%s", "[PROJECT]", "[SINK]")
	var sink *loggingpb.LogSink = &loggingpb.LogSink{}
	var request = &loggingpb.UpdateSinkRequest{
		SinkName: formattedSinkName,
		Sink:     sink,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateSink(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockConfig.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestConfigServiceV2UpdateSinkError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockConfig.err = gstatus.Error(errCode, "test error")

	var formattedSinkName string = fmt.Sprintf("projects/%s/sinks/%s", "[PROJECT]", "[SINK]")
	var sink *loggingpb.LogSink = &loggingpb.LogSink{}
	var request = &loggingpb.UpdateSinkRequest{
		SinkName: formattedSinkName,
		Sink:     sink,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateSink(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestConfigServiceV2DeleteSink(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockConfig.err = nil
	mockConfig.reqs = nil

	mockConfig.resps = append(mockConfig.resps[:0], expectedResponse)

	var formattedSinkName string = fmt.Sprintf("projects/%s/sinks/%s", "[PROJECT]", "[SINK]")
	var request = &loggingpb.DeleteSinkRequest{
		SinkName: formattedSinkName,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteSink(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockConfig.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestConfigServiceV2DeleteSinkError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockConfig.err = gstatus.Error(errCode, "test error")

	var formattedSinkName string = fmt.Sprintf("projects/%s/sinks/%s", "[PROJECT]", "[SINK]")
	var request = &loggingpb.DeleteSinkRequest{
		SinkName: formattedSinkName,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteSink(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestConfigServiceV2ListExclusions(t *testing.T) {
	var nextPageToken string = ""
	var exclusionsElement *loggingpb.LogExclusion = &loggingpb.LogExclusion{}
	var exclusions = []*loggingpb.LogExclusion{exclusionsElement}
	var expectedResponse = &loggingpb.ListExclusionsResponse{
		NextPageToken: nextPageToken,
		Exclusions:    exclusions,
	}

	mockConfig.err = nil
	mockConfig.reqs = nil

	mockConfig.resps = append(mockConfig.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &loggingpb.ListExclusionsRequest{
		Parent: formattedParent,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListExclusions(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockConfig.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Exclusions[0])
	got := (interface{})(resp)
	var ok bool

	switch want := (want).(type) {
	case proto.Message:
		ok = proto.Equal(want, got.(proto.Message))
	default:
		ok = want == got
	}
	if !ok {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestConfigServiceV2ListExclusionsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockConfig.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &loggingpb.ListExclusionsRequest{
		Parent: formattedParent,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListExclusions(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestConfigServiceV2GetExclusion(t *testing.T) {
	var name2 string = "name2-1052831874"
	var description string = "description-1724546052"
	var filter string = "filter-1274492040"
	var disabled bool = true
	var expectedResponse = &loggingpb.LogExclusion{
		Name:        name2,
		Description: description,
		Filter:      filter,
		Disabled:    disabled,
	}

	mockConfig.err = nil
	mockConfig.reqs = nil

	mockConfig.resps = append(mockConfig.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/exclusions/%s", "[PROJECT]", "[EXCLUSION]")
	var request = &loggingpb.GetExclusionRequest{
		Name: formattedName,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetExclusion(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockConfig.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestConfigServiceV2GetExclusionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockConfig.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/exclusions/%s", "[PROJECT]", "[EXCLUSION]")
	var request = &loggingpb.GetExclusionRequest{
		Name: formattedName,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetExclusion(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestConfigServiceV2CreateExclusion(t *testing.T) {
	var name string = "name3373707"
	var description string = "description-1724546052"
	var filter string = "filter-1274492040"
	var disabled bool = true
	var expectedResponse = &loggingpb.LogExclusion{
		Name:        name,
		Description: description,
		Filter:      filter,
		Disabled:    disabled,
	}

	mockConfig.err = nil
	mockConfig.reqs = nil

	mockConfig.resps = append(mockConfig.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var exclusion *loggingpb.LogExclusion = &loggingpb.LogExclusion{}
	var request = &loggingpb.CreateExclusionRequest{
		Parent:    formattedParent,
		Exclusion: exclusion,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateExclusion(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockConfig.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestConfigServiceV2CreateExclusionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockConfig.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var exclusion *loggingpb.LogExclusion = &loggingpb.LogExclusion{}
	var request = &loggingpb.CreateExclusionRequest{
		Parent:    formattedParent,
		Exclusion: exclusion,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateExclusion(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestConfigServiceV2UpdateExclusion(t *testing.T) {
	var name2 string = "name2-1052831874"
	var description string = "description-1724546052"
	var filter string = "filter-1274492040"
	var disabled bool = true
	var expectedResponse = &loggingpb.LogExclusion{
		Name:        name2,
		Description: description,
		Filter:      filter,
		Disabled:    disabled,
	}

	mockConfig.err = nil
	mockConfig.reqs = nil

	mockConfig.resps = append(mockConfig.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/exclusions/%s", "[PROJECT]", "[EXCLUSION]")
	var exclusion *loggingpb.LogExclusion = &loggingpb.LogExclusion{}
	var updateMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &loggingpb.UpdateExclusionRequest{
		Name:       formattedName,
		Exclusion:  exclusion,
		UpdateMask: updateMask,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateExclusion(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockConfig.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestConfigServiceV2UpdateExclusionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockConfig.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/exclusions/%s", "[PROJECT]", "[EXCLUSION]")
	var exclusion *loggingpb.LogExclusion = &loggingpb.LogExclusion{}
	var updateMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &loggingpb.UpdateExclusionRequest{
		Name:       formattedName,
		Exclusion:  exclusion,
		UpdateMask: updateMask,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateExclusion(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestConfigServiceV2DeleteExclusion(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockConfig.err = nil
	mockConfig.reqs = nil

	mockConfig.resps = append(mockConfig.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/exclusions/%s", "[PROJECT]", "[EXCLUSION]")
	var request = &loggingpb.DeleteExclusionRequest{
		Name: formattedName,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteExclusion(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockConfig.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestConfigServiceV2DeleteExclusionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockConfig.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/exclusions/%s", "[PROJECT]", "[EXCLUSION]")
	var request = &loggingpb.DeleteExclusionRequest{
		Name: formattedName,
	}

	c, err := NewConfigClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteExclusion(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestMetricsServiceV2ListLogMetrics(t *testing.T) {
	var nextPageToken string = ""
	var metricsElement *loggingpb.LogMetric = &loggingpb.LogMetric{}
	var metrics = []*loggingpb.LogMetric{metricsElement}
	var expectedResponse = &loggingpb.ListLogMetricsResponse{
		NextPageToken: nextPageToken,
		Metrics:       metrics,
	}

	mockMetrics.err = nil
	mockMetrics.reqs = nil

	mockMetrics.resps = append(mockMetrics.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &loggingpb.ListLogMetricsRequest{
		Parent: formattedParent,
	}

	c, err := NewMetricsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListLogMetrics(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetrics.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Metrics[0])
	got := (interface{})(resp)
	var ok bool

	switch want := (want).(type) {
	case proto.Message:
		ok = proto.Equal(want, got.(proto.Message))
	default:
		ok = want == got
	}
	if !ok {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestMetricsServiceV2ListLogMetricsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetrics.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &loggingpb.ListLogMetricsRequest{
		Parent: formattedParent,
	}

	c, err := NewMetricsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListLogMetrics(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricsServiceV2GetLogMetric(t *testing.T) {
	var name string = "name3373707"
	var description string = "description-1724546052"
	var filter string = "filter-1274492040"
	var valueExtractor string = "valueExtractor2047672534"
	var expectedResponse = &loggingpb.LogMetric{
		Name:           name,
		Description:    description,
		Filter:         filter,
		ValueExtractor: valueExtractor,
	}

	mockMetrics.err = nil
	mockMetrics.reqs = nil

	mockMetrics.resps = append(mockMetrics.resps[:0], expectedResponse)

	var formattedMetricName string = fmt.Sprintf("projects/%s/metrics/%s", "[PROJECT]", "[METRIC]")
	var request = &loggingpb.GetLogMetricRequest{
		MetricName: formattedMetricName,
	}

	c, err := NewMetricsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetLogMetric(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetrics.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestMetricsServiceV2GetLogMetricError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetrics.err = gstatus.Error(errCode, "test error")

	var formattedMetricName string = fmt.Sprintf("projects/%s/metrics/%s", "[PROJECT]", "[METRIC]")
	var request = &loggingpb.GetLogMetricRequest{
		MetricName: formattedMetricName,
	}

	c, err := NewMetricsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetLogMetric(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricsServiceV2CreateLogMetric(t *testing.T) {
	var name string = "name3373707"
	var description string = "description-1724546052"
	var filter string = "filter-1274492040"
	var valueExtractor string = "valueExtractor2047672534"
	var expectedResponse = &loggingpb.LogMetric{
		Name:           name,
		Description:    description,
		Filter:         filter,
		ValueExtractor: valueExtractor,
	}

	mockMetrics.err = nil
	mockMetrics.reqs = nil

	mockMetrics.resps = append(mockMetrics.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var metric *loggingpb.LogMetric = &loggingpb.LogMetric{}
	var request = &loggingpb.CreateLogMetricRequest{
		Parent: formattedParent,
		Metric: metric,
	}

	c, err := NewMetricsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateLogMetric(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetrics.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestMetricsServiceV2CreateLogMetricError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetrics.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var metric *loggingpb.LogMetric = &loggingpb.LogMetric{}
	var request = &loggingpb.CreateLogMetricRequest{
		Parent: formattedParent,
		Metric: metric,
	}

	c, err := NewMetricsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateLogMetric(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricsServiceV2UpdateLogMetric(t *testing.T) {
	var name string = "name3373707"
	var description string = "description-1724546052"
	var filter string = "filter-1274492040"
	var valueExtractor string = "valueExtractor2047672534"
	var expectedResponse = &loggingpb.LogMetric{
		Name:           name,
		Description:    description,
		Filter:         filter,
		ValueExtractor: valueExtractor,
	}

	mockMetrics.err = nil
	mockMetrics.reqs = nil

	mockMetrics.resps = append(mockMetrics.resps[:0], expectedResponse)

	var formattedMetricName string = fmt.Sprintf("projects/%s/metrics/%s", "[PROJECT]", "[METRIC]")
	var metric *loggingpb.LogMetric = &loggingpb.LogMetric{}
	var request = &loggingpb.UpdateLogMetricRequest{
		MetricName: formattedMetricName,
		Metric:     metric,
	}

	c, err := NewMetricsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateLogMetric(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetrics.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestMetricsServiceV2UpdateLogMetricError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetrics.err = gstatus.Error(errCode, "test error")

	var formattedMetricName string = fmt.Sprintf("projects/%s/metrics/%s", "[PROJECT]", "[METRIC]")
	var metric *loggingpb.LogMetric = &loggingpb.LogMetric{}
	var request = &loggingpb.UpdateLogMetricRequest{
		MetricName: formattedMetricName,
		Metric:     metric,
	}

	c, err := NewMetricsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateLogMetric(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricsServiceV2DeleteLogMetric(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockMetrics.err = nil
	mockMetrics.reqs = nil

	mockMetrics.resps = append(mockMetrics.resps[:0], expectedResponse)

	var formattedMetricName string = fmt.Sprintf("projects/%s/metrics/%s", "[PROJECT]", "[METRIC]")
	var request = &loggingpb.DeleteLogMetricRequest{
		MetricName: formattedMetricName,
	}

	c, err := NewMetricsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteLogMetric(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetrics.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestMetricsServiceV2DeleteLogMetricError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetrics.err = gstatus.Error(errCode, "test error")

	var formattedMetricName string = fmt.Sprintf("projects/%s/metrics/%s", "[PROJECT]", "[METRIC]")
	var request = &loggingpb.DeleteLogMetricRequest{
		MetricName: formattedMetricName,
	}

	c, err := NewMetricsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteLogMetric(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
