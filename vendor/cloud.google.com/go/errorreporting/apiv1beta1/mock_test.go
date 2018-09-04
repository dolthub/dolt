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

package errorreporting

import (
	clouderrorreportingpb "google.golang.org/genproto/googleapis/devtools/clouderrorreporting/v1beta1"
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

type mockErrorGroupServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	clouderrorreportingpb.ErrorGroupServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockErrorGroupServer) GetGroup(ctx context.Context, req *clouderrorreportingpb.GetGroupRequest) (*clouderrorreportingpb.ErrorGroup, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouderrorreportingpb.ErrorGroup), nil
}

func (s *mockErrorGroupServer) UpdateGroup(ctx context.Context, req *clouderrorreportingpb.UpdateGroupRequest) (*clouderrorreportingpb.ErrorGroup, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouderrorreportingpb.ErrorGroup), nil
}

type mockErrorStatsServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	clouderrorreportingpb.ErrorStatsServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockErrorStatsServer) ListGroupStats(ctx context.Context, req *clouderrorreportingpb.ListGroupStatsRequest) (*clouderrorreportingpb.ListGroupStatsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouderrorreportingpb.ListGroupStatsResponse), nil
}

func (s *mockErrorStatsServer) ListEvents(ctx context.Context, req *clouderrorreportingpb.ListEventsRequest) (*clouderrorreportingpb.ListEventsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouderrorreportingpb.ListEventsResponse), nil
}

func (s *mockErrorStatsServer) DeleteEvents(ctx context.Context, req *clouderrorreportingpb.DeleteEventsRequest) (*clouderrorreportingpb.DeleteEventsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouderrorreportingpb.DeleteEventsResponse), nil
}

type mockReportErrorsServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	clouderrorreportingpb.ReportErrorsServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockReportErrorsServer) ReportErrorEvent(ctx context.Context, req *clouderrorreportingpb.ReportErrorEventRequest) (*clouderrorreportingpb.ReportErrorEventResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*clouderrorreportingpb.ReportErrorEventResponse), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockErrorGroup   mockErrorGroupServer
	mockErrorStats   mockErrorStatsServer
	mockReportErrors mockReportErrorsServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	clouderrorreportingpb.RegisterErrorGroupServiceServer(serv, &mockErrorGroup)
	clouderrorreportingpb.RegisterErrorStatsServiceServer(serv, &mockErrorStats)
	clouderrorreportingpb.RegisterReportErrorsServiceServer(serv, &mockReportErrors)

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

func TestErrorGroupServiceGetGroup(t *testing.T) {
	var name string = "name3373707"
	var groupId string = "groupId506361563"
	var expectedResponse = &clouderrorreportingpb.ErrorGroup{
		Name:    name,
		GroupId: groupId,
	}

	mockErrorGroup.err = nil
	mockErrorGroup.reqs = nil

	mockErrorGroup.resps = append(mockErrorGroup.resps[:0], expectedResponse)

	var formattedGroupName string = fmt.Sprintf("projects/%s/groups/%s", "[PROJECT]", "[GROUP]")
	var request = &clouderrorreportingpb.GetGroupRequest{
		GroupName: formattedGroupName,
	}

	c, err := NewErrorGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetGroup(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockErrorGroup.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestErrorGroupServiceGetGroupError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockErrorGroup.err = gstatus.Error(errCode, "test error")

	var formattedGroupName string = fmt.Sprintf("projects/%s/groups/%s", "[PROJECT]", "[GROUP]")
	var request = &clouderrorreportingpb.GetGroupRequest{
		GroupName: formattedGroupName,
	}

	c, err := NewErrorGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetGroup(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestErrorGroupServiceUpdateGroup(t *testing.T) {
	var name string = "name3373707"
	var groupId string = "groupId506361563"
	var expectedResponse = &clouderrorreportingpb.ErrorGroup{
		Name:    name,
		GroupId: groupId,
	}

	mockErrorGroup.err = nil
	mockErrorGroup.reqs = nil

	mockErrorGroup.resps = append(mockErrorGroup.resps[:0], expectedResponse)

	var group *clouderrorreportingpb.ErrorGroup = &clouderrorreportingpb.ErrorGroup{}
	var request = &clouderrorreportingpb.UpdateGroupRequest{
		Group: group,
	}

	c, err := NewErrorGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateGroup(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockErrorGroup.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestErrorGroupServiceUpdateGroupError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockErrorGroup.err = gstatus.Error(errCode, "test error")

	var group *clouderrorreportingpb.ErrorGroup = &clouderrorreportingpb.ErrorGroup{}
	var request = &clouderrorreportingpb.UpdateGroupRequest{
		Group: group,
	}

	c, err := NewErrorGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateGroup(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestErrorStatsServiceListGroupStats(t *testing.T) {
	var nextPageToken string = ""
	var errorGroupStatsElement *clouderrorreportingpb.ErrorGroupStats = &clouderrorreportingpb.ErrorGroupStats{}
	var errorGroupStats = []*clouderrorreportingpb.ErrorGroupStats{errorGroupStatsElement}
	var expectedResponse = &clouderrorreportingpb.ListGroupStatsResponse{
		NextPageToken:   nextPageToken,
		ErrorGroupStats: errorGroupStats,
	}

	mockErrorStats.err = nil
	mockErrorStats.reqs = nil

	mockErrorStats.resps = append(mockErrorStats.resps[:0], expectedResponse)

	var formattedProjectName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var timeRange *clouderrorreportingpb.QueryTimeRange = &clouderrorreportingpb.QueryTimeRange{}
	var request = &clouderrorreportingpb.ListGroupStatsRequest{
		ProjectName: formattedProjectName,
		TimeRange:   timeRange,
	}

	c, err := NewErrorStatsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListGroupStats(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockErrorStats.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.ErrorGroupStats[0])
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

func TestErrorStatsServiceListGroupStatsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockErrorStats.err = gstatus.Error(errCode, "test error")

	var formattedProjectName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var timeRange *clouderrorreportingpb.QueryTimeRange = &clouderrorreportingpb.QueryTimeRange{}
	var request = &clouderrorreportingpb.ListGroupStatsRequest{
		ProjectName: formattedProjectName,
		TimeRange:   timeRange,
	}

	c, err := NewErrorStatsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListGroupStats(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestErrorStatsServiceListEvents(t *testing.T) {
	var nextPageToken string = ""
	var errorEventsElement *clouderrorreportingpb.ErrorEvent = &clouderrorreportingpb.ErrorEvent{}
	var errorEvents = []*clouderrorreportingpb.ErrorEvent{errorEventsElement}
	var expectedResponse = &clouderrorreportingpb.ListEventsResponse{
		NextPageToken: nextPageToken,
		ErrorEvents:   errorEvents,
	}

	mockErrorStats.err = nil
	mockErrorStats.reqs = nil

	mockErrorStats.resps = append(mockErrorStats.resps[:0], expectedResponse)

	var formattedProjectName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var groupId string = "groupId506361563"
	var request = &clouderrorreportingpb.ListEventsRequest{
		ProjectName: formattedProjectName,
		GroupId:     groupId,
	}

	c, err := NewErrorStatsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListEvents(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockErrorStats.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.ErrorEvents[0])
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

func TestErrorStatsServiceListEventsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockErrorStats.err = gstatus.Error(errCode, "test error")

	var formattedProjectName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var groupId string = "groupId506361563"
	var request = &clouderrorreportingpb.ListEventsRequest{
		ProjectName: formattedProjectName,
		GroupId:     groupId,
	}

	c, err := NewErrorStatsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListEvents(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestErrorStatsServiceDeleteEvents(t *testing.T) {
	var expectedResponse *clouderrorreportingpb.DeleteEventsResponse = &clouderrorreportingpb.DeleteEventsResponse{}

	mockErrorStats.err = nil
	mockErrorStats.reqs = nil

	mockErrorStats.resps = append(mockErrorStats.resps[:0], expectedResponse)

	var formattedProjectName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &clouderrorreportingpb.DeleteEventsRequest{
		ProjectName: formattedProjectName,
	}

	c, err := NewErrorStatsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.DeleteEvents(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockErrorStats.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestErrorStatsServiceDeleteEventsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockErrorStats.err = gstatus.Error(errCode, "test error")

	var formattedProjectName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &clouderrorreportingpb.DeleteEventsRequest{
		ProjectName: formattedProjectName,
	}

	c, err := NewErrorStatsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.DeleteEvents(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestReportErrorsServiceReportErrorEvent(t *testing.T) {
	var expectedResponse *clouderrorreportingpb.ReportErrorEventResponse = &clouderrorreportingpb.ReportErrorEventResponse{}

	mockReportErrors.err = nil
	mockReportErrors.reqs = nil

	mockReportErrors.resps = append(mockReportErrors.resps[:0], expectedResponse)

	var formattedProjectName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var event *clouderrorreportingpb.ReportedErrorEvent = &clouderrorreportingpb.ReportedErrorEvent{}
	var request = &clouderrorreportingpb.ReportErrorEventRequest{
		ProjectName: formattedProjectName,
		Event:       event,
	}

	c, err := NewReportErrorsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ReportErrorEvent(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockReportErrors.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestReportErrorsServiceReportErrorEventError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockReportErrors.err = gstatus.Error(errCode, "test error")

	var formattedProjectName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var event *clouderrorreportingpb.ReportedErrorEvent = &clouderrorreportingpb.ReportedErrorEvent{}
	var request = &clouderrorreportingpb.ReportErrorEventRequest{
		ProjectName: formattedProjectName,
		Event:       event,
	}

	c, err := NewReportErrorsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ReportErrorEvent(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
