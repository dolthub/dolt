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

package cloudtasks

import (
	durationpb "github.com/golang/protobuf/ptypes/duration"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2beta2"
	iampb "google.golang.org/genproto/googleapis/iam/v1"
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

type mockCloudTasksServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	taskspb.CloudTasksServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockCloudTasksServer) ListQueues(ctx context.Context, req *taskspb.ListQueuesRequest) (*taskspb.ListQueuesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.ListQueuesResponse), nil
}

func (s *mockCloudTasksServer) GetQueue(ctx context.Context, req *taskspb.GetQueueRequest) (*taskspb.Queue, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Queue), nil
}

func (s *mockCloudTasksServer) CreateQueue(ctx context.Context, req *taskspb.CreateQueueRequest) (*taskspb.Queue, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Queue), nil
}

func (s *mockCloudTasksServer) UpdateQueue(ctx context.Context, req *taskspb.UpdateQueueRequest) (*taskspb.Queue, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Queue), nil
}

func (s *mockCloudTasksServer) DeleteQueue(ctx context.Context, req *taskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
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

func (s *mockCloudTasksServer) PurgeQueue(ctx context.Context, req *taskspb.PurgeQueueRequest) (*taskspb.Queue, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Queue), nil
}

func (s *mockCloudTasksServer) PauseQueue(ctx context.Context, req *taskspb.PauseQueueRequest) (*taskspb.Queue, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Queue), nil
}

func (s *mockCloudTasksServer) ResumeQueue(ctx context.Context, req *taskspb.ResumeQueueRequest) (*taskspb.Queue, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Queue), nil
}

func (s *mockCloudTasksServer) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*iampb.Policy), nil
}

func (s *mockCloudTasksServer) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*iampb.Policy), nil
}

func (s *mockCloudTasksServer) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*iampb.TestIamPermissionsResponse), nil
}

func (s *mockCloudTasksServer) ListTasks(ctx context.Context, req *taskspb.ListTasksRequest) (*taskspb.ListTasksResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.ListTasksResponse), nil
}

func (s *mockCloudTasksServer) GetTask(ctx context.Context, req *taskspb.GetTaskRequest) (*taskspb.Task, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Task), nil
}

func (s *mockCloudTasksServer) CreateTask(ctx context.Context, req *taskspb.CreateTaskRequest) (*taskspb.Task, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Task), nil
}

func (s *mockCloudTasksServer) DeleteTask(ctx context.Context, req *taskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
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

func (s *mockCloudTasksServer) LeaseTasks(ctx context.Context, req *taskspb.LeaseTasksRequest) (*taskspb.LeaseTasksResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.LeaseTasksResponse), nil
}

func (s *mockCloudTasksServer) AcknowledgeTask(ctx context.Context, req *taskspb.AcknowledgeTaskRequest) (*emptypb.Empty, error) {
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

func (s *mockCloudTasksServer) RenewLease(ctx context.Context, req *taskspb.RenewLeaseRequest) (*taskspb.Task, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Task), nil
}

func (s *mockCloudTasksServer) CancelLease(ctx context.Context, req *taskspb.CancelLeaseRequest) (*taskspb.Task, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Task), nil
}

func (s *mockCloudTasksServer) RunTask(ctx context.Context, req *taskspb.RunTaskRequest) (*taskspb.Task, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*taskspb.Task), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockCloudTasks mockCloudTasksServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	taskspb.RegisterCloudTasksServer(serv, &mockCloudTasks)

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

func TestCloudTasksListQueues(t *testing.T) {
	var nextPageToken string = ""
	var queuesElement *taskspb.Queue = &taskspb.Queue{}
	var queues = []*taskspb.Queue{queuesElement}
	var expectedResponse = &taskspb.ListQueuesResponse{
		NextPageToken: nextPageToken,
		Queues:        queues,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var request = &taskspb.ListQueuesRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListQueues(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Queues[0])
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

func TestCloudTasksListQueuesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var request = &taskspb.ListQueuesRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListQueues(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksGetQueue(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &taskspb.Queue{
		Name: name2,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.GetQueueRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetQueue(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksGetQueueError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.GetQueueRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetQueue(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksCreateQueue(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &taskspb.Queue{
		Name: name,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var queue *taskspb.Queue = &taskspb.Queue{}
	var request = &taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateQueue(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksCreateQueueError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var queue *taskspb.Queue = &taskspb.Queue{}
	var request = &taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateQueue(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksUpdateQueue(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &taskspb.Queue{
		Name: name,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var queue *taskspb.Queue = &taskspb.Queue{}
	var request = &taskspb.UpdateQueueRequest{
		Queue: queue,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateQueue(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksUpdateQueueError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var queue *taskspb.Queue = &taskspb.Queue{}
	var request = &taskspb.UpdateQueueRequest{
		Queue: queue,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateQueue(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksDeleteQueue(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.DeleteQueueRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteQueue(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestCloudTasksDeleteQueueError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.DeleteQueueRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteQueue(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestCloudTasksPurgeQueue(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &taskspb.Queue{
		Name: name2,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.PurgeQueueRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.PurgeQueue(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksPurgeQueueError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.PurgeQueueRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.PurgeQueue(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksPauseQueue(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &taskspb.Queue{
		Name: name2,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.PauseQueueRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.PauseQueue(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksPauseQueueError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.PauseQueueRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.PauseQueue(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksResumeQueue(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &taskspb.Queue{
		Name: name2,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.ResumeQueueRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ResumeQueue(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksResumeQueueError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.ResumeQueueRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ResumeQueue(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksGetIamPolicy(t *testing.T) {
	var version int32 = 351608024
	var etag []byte = []byte("21")
	var expectedResponse = &iampb.Policy{
		Version: version,
		Etag:    etag,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedResource string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &iampb.GetIamPolicyRequest{
		Resource: formattedResource,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetIamPolicy(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksGetIamPolicyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedResource string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &iampb.GetIamPolicyRequest{
		Resource: formattedResource,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetIamPolicy(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksSetIamPolicy(t *testing.T) {
	var version int32 = 351608024
	var etag []byte = []byte("21")
	var expectedResponse = &iampb.Policy{
		Version: version,
		Etag:    etag,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedResource string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var policy *iampb.Policy = &iampb.Policy{}
	var request = &iampb.SetIamPolicyRequest{
		Resource: formattedResource,
		Policy:   policy,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SetIamPolicy(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksSetIamPolicyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedResource string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var policy *iampb.Policy = &iampb.Policy{}
	var request = &iampb.SetIamPolicyRequest{
		Resource: formattedResource,
		Policy:   policy,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SetIamPolicy(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksTestIamPermissions(t *testing.T) {
	var expectedResponse *iampb.TestIamPermissionsResponse = &iampb.TestIamPermissionsResponse{}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedResource string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var permissions []string = nil
	var request = &iampb.TestIamPermissionsRequest{
		Resource:    formattedResource,
		Permissions: permissions,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.TestIamPermissions(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksTestIamPermissionsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedResource string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var permissions []string = nil
	var request = &iampb.TestIamPermissionsRequest{
		Resource:    formattedResource,
		Permissions: permissions,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.TestIamPermissions(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksListTasks(t *testing.T) {
	var nextPageToken string = ""
	var tasksElement *taskspb.Task = &taskspb.Task{}
	var tasks = []*taskspb.Task{tasksElement}
	var expectedResponse = &taskspb.ListTasksResponse{
		NextPageToken: nextPageToken,
		Tasks:         tasks,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.ListTasksRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListTasks(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Tasks[0])
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

func TestCloudTasksListTasksError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var request = &taskspb.ListTasksRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListTasks(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksGetTask(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &taskspb.Task{
		Name: name2,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var request = &taskspb.GetTaskRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetTask(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksGetTaskError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var request = &taskspb.GetTaskRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetTask(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksCreateTask(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &taskspb.Task{
		Name: name,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var task *taskspb.Task = &taskspb.Task{}
	var request = &taskspb.CreateTaskRequest{
		Parent: formattedParent,
		Task:   task,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateTask(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksCreateTaskError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var task *taskspb.Task = &taskspb.Task{}
	var request = &taskspb.CreateTaskRequest{
		Parent: formattedParent,
		Task:   task,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateTask(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksDeleteTask(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var request = &taskspb.DeleteTaskRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteTask(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestCloudTasksDeleteTaskError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var request = &taskspb.DeleteTaskRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteTask(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestCloudTasksLeaseTasks(t *testing.T) {
	var expectedResponse *taskspb.LeaseTasksResponse = &taskspb.LeaseTasksResponse{}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var leaseDuration *durationpb.Duration = &durationpb.Duration{}
	var request = &taskspb.LeaseTasksRequest{
		Parent:        formattedParent,
		LeaseDuration: leaseDuration,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.LeaseTasks(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksLeaseTasksError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/queues/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]")
	var leaseDuration *durationpb.Duration = &durationpb.Duration{}
	var request = &taskspb.LeaseTasksRequest{
		Parent:        formattedParent,
		LeaseDuration: leaseDuration,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.LeaseTasks(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksAcknowledgeTask(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var scheduleTime *timestamppb.Timestamp = &timestamppb.Timestamp{}
	var request = &taskspb.AcknowledgeTaskRequest{
		Name:         formattedName,
		ScheduleTime: scheduleTime,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.AcknowledgeTask(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestCloudTasksAcknowledgeTaskError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var scheduleTime *timestamppb.Timestamp = &timestamppb.Timestamp{}
	var request = &taskspb.AcknowledgeTaskRequest{
		Name:         formattedName,
		ScheduleTime: scheduleTime,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.AcknowledgeTask(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestCloudTasksRenewLease(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &taskspb.Task{
		Name: name2,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var scheduleTime *timestamppb.Timestamp = &timestamppb.Timestamp{}
	var leaseDuration *durationpb.Duration = &durationpb.Duration{}
	var request = &taskspb.RenewLeaseRequest{
		Name:          formattedName,
		ScheduleTime:  scheduleTime,
		LeaseDuration: leaseDuration,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.RenewLease(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksRenewLeaseError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var scheduleTime *timestamppb.Timestamp = &timestamppb.Timestamp{}
	var leaseDuration *durationpb.Duration = &durationpb.Duration{}
	var request = &taskspb.RenewLeaseRequest{
		Name:          formattedName,
		ScheduleTime:  scheduleTime,
		LeaseDuration: leaseDuration,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.RenewLease(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksCancelLease(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &taskspb.Task{
		Name: name2,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var scheduleTime *timestamppb.Timestamp = &timestamppb.Timestamp{}
	var request = &taskspb.CancelLeaseRequest{
		Name:         formattedName,
		ScheduleTime: scheduleTime,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CancelLease(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksCancelLeaseError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var scheduleTime *timestamppb.Timestamp = &timestamppb.Timestamp{}
	var request = &taskspb.CancelLeaseRequest{
		Name:         formattedName,
		ScheduleTime: scheduleTime,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CancelLease(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestCloudTasksRunTask(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &taskspb.Task{
		Name: name2,
	}

	mockCloudTasks.err = nil
	mockCloudTasks.reqs = nil

	mockCloudTasks.resps = append(mockCloudTasks.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var request = &taskspb.RunTaskRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.RunTask(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudTasks.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudTasksRunTaskError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudTasks.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", "[PROJECT]", "[LOCATION]", "[QUEUE]", "[TASK]")
	var request = &taskspb.RunTaskRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.RunTask(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
