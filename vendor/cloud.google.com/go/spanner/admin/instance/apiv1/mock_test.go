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

package instance

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	iampb "google.golang.org/genproto/googleapis/iam/v1"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
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

type mockInstanceAdminServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	instancepb.InstanceAdminServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockInstanceAdminServer) ListInstanceConfigs(ctx context.Context, req *instancepb.ListInstanceConfigsRequest) (*instancepb.ListInstanceConfigsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*instancepb.ListInstanceConfigsResponse), nil
}

func (s *mockInstanceAdminServer) GetInstanceConfig(ctx context.Context, req *instancepb.GetInstanceConfigRequest) (*instancepb.InstanceConfig, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*instancepb.InstanceConfig), nil
}

func (s *mockInstanceAdminServer) ListInstances(ctx context.Context, req *instancepb.ListInstancesRequest) (*instancepb.ListInstancesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*instancepb.ListInstancesResponse), nil
}

func (s *mockInstanceAdminServer) GetInstance(ctx context.Context, req *instancepb.GetInstanceRequest) (*instancepb.Instance, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*instancepb.Instance), nil
}

func (s *mockInstanceAdminServer) CreateInstance(ctx context.Context, req *instancepb.CreateInstanceRequest) (*longrunningpb.Operation, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*longrunningpb.Operation), nil
}

func (s *mockInstanceAdminServer) UpdateInstance(ctx context.Context, req *instancepb.UpdateInstanceRequest) (*longrunningpb.Operation, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*longrunningpb.Operation), nil
}

func (s *mockInstanceAdminServer) DeleteInstance(ctx context.Context, req *instancepb.DeleteInstanceRequest) (*emptypb.Empty, error) {
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

func (s *mockInstanceAdminServer) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
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

func (s *mockInstanceAdminServer) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
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

func (s *mockInstanceAdminServer) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
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

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockInstanceAdmin mockInstanceAdminServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	instancepb.RegisterInstanceAdminServer(serv, &mockInstanceAdmin)

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

func TestInstanceAdminListInstanceConfigs(t *testing.T) {
	var nextPageToken string = ""
	var instanceConfigsElement *instancepb.InstanceConfig = &instancepb.InstanceConfig{}
	var instanceConfigs = []*instancepb.InstanceConfig{instanceConfigsElement}
	var expectedResponse = &instancepb.ListInstanceConfigsResponse{
		NextPageToken:   nextPageToken,
		InstanceConfigs: instanceConfigs,
	}

	mockInstanceAdmin.err = nil
	mockInstanceAdmin.reqs = nil

	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &instancepb.ListInstanceConfigsRequest{
		Parent: formattedParent,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListInstanceConfigs(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockInstanceAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.InstanceConfigs[0])
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

func TestInstanceAdminListInstanceConfigsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockInstanceAdmin.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &instancepb.ListInstanceConfigsRequest{
		Parent: formattedParent,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListInstanceConfigs(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestInstanceAdminGetInstanceConfig(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var expectedResponse = &instancepb.InstanceConfig{
		Name:        name2,
		DisplayName: displayName,
	}

	mockInstanceAdmin.err = nil
	mockInstanceAdmin.reqs = nil

	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/instanceConfigs/%s", "[PROJECT]", "[INSTANCE_CONFIG]")
	var request = &instancepb.GetInstanceConfigRequest{
		Name: formattedName,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetInstanceConfig(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockInstanceAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestInstanceAdminGetInstanceConfigError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockInstanceAdmin.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/instanceConfigs/%s", "[PROJECT]", "[INSTANCE_CONFIG]")
	var request = &instancepb.GetInstanceConfigRequest{
		Name: formattedName,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetInstanceConfig(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestInstanceAdminListInstances(t *testing.T) {
	var nextPageToken string = ""
	var instancesElement *instancepb.Instance = &instancepb.Instance{}
	var instances = []*instancepb.Instance{instancesElement}
	var expectedResponse = &instancepb.ListInstancesResponse{
		NextPageToken: nextPageToken,
		Instances:     instances,
	}

	mockInstanceAdmin.err = nil
	mockInstanceAdmin.reqs = nil

	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &instancepb.ListInstancesRequest{
		Parent: formattedParent,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListInstances(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockInstanceAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Instances[0])
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

func TestInstanceAdminListInstancesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockInstanceAdmin.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &instancepb.ListInstancesRequest{
		Parent: formattedParent,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListInstances(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestInstanceAdminGetInstance(t *testing.T) {
	var name2 string = "name2-1052831874"
	var config string = "config-1354792126"
	var displayName string = "displayName1615086568"
	var nodeCount int32 = 1539922066
	var expectedResponse = &instancepb.Instance{
		Name:        name2,
		Config:      config,
		DisplayName: displayName,
		NodeCount:   nodeCount,
	}

	mockInstanceAdmin.err = nil
	mockInstanceAdmin.reqs = nil

	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var request = &instancepb.GetInstanceRequest{
		Name: formattedName,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetInstance(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockInstanceAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestInstanceAdminGetInstanceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockInstanceAdmin.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var request = &instancepb.GetInstanceRequest{
		Name: formattedName,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetInstance(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestInstanceAdminCreateInstance(t *testing.T) {
	var name string = "name3373707"
	var config string = "config-1354792126"
	var displayName string = "displayName1615086568"
	var nodeCount int32 = 1539922066
	var expectedResponse = &instancepb.Instance{
		Name:        name,
		Config:      config,
		DisplayName: displayName,
		NodeCount:   nodeCount,
	}

	mockInstanceAdmin.err = nil
	mockInstanceAdmin.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var instanceId string = "instanceId-2101995259"
	var instance *instancepb.Instance = &instancepb.Instance{}
	var request = &instancepb.CreateInstanceRequest{
		Parent:     formattedParent,
		InstanceId: instanceId,
		Instance:   instance,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.CreateInstance(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockInstanceAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestInstanceAdminCreateInstanceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockInstanceAdmin.err = nil
	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var instanceId string = "instanceId-2101995259"
	var instance *instancepb.Instance = &instancepb.Instance{}
	var request = &instancepb.CreateInstanceRequest{
		Parent:     formattedParent,
		InstanceId: instanceId,
		Instance:   instance,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.CreateInstance(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestInstanceAdminUpdateInstance(t *testing.T) {
	var name string = "name3373707"
	var config string = "config-1354792126"
	var displayName string = "displayName1615086568"
	var nodeCount int32 = 1539922066
	var expectedResponse = &instancepb.Instance{
		Name:        name,
		Config:      config,
		DisplayName: displayName,
		NodeCount:   nodeCount,
	}

	mockInstanceAdmin.err = nil
	mockInstanceAdmin.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var instance *instancepb.Instance = &instancepb.Instance{}
	var fieldMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &instancepb.UpdateInstanceRequest{
		Instance:  instance,
		FieldMask: fieldMask,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.UpdateInstance(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockInstanceAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestInstanceAdminUpdateInstanceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockInstanceAdmin.err = nil
	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var instance *instancepb.Instance = &instancepb.Instance{}
	var fieldMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &instancepb.UpdateInstanceRequest{
		Instance:  instance,
		FieldMask: fieldMask,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.UpdateInstance(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestInstanceAdminDeleteInstance(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockInstanceAdmin.err = nil
	mockInstanceAdmin.reqs = nil

	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var request = &instancepb.DeleteInstanceRequest{
		Name: formattedName,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteInstance(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockInstanceAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestInstanceAdminDeleteInstanceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockInstanceAdmin.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var request = &instancepb.DeleteInstanceRequest{
		Name: formattedName,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteInstance(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestInstanceAdminSetIamPolicy(t *testing.T) {
	var version int32 = 351608024
	var etag []byte = []byte("21")
	var expectedResponse = &iampb.Policy{
		Version: version,
		Etag:    etag,
	}

	mockInstanceAdmin.err = nil
	mockInstanceAdmin.reqs = nil

	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], expectedResponse)

	var formattedResource string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var policy *iampb.Policy = &iampb.Policy{}
	var request = &iampb.SetIamPolicyRequest{
		Resource: formattedResource,
		Policy:   policy,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SetIamPolicy(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockInstanceAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestInstanceAdminSetIamPolicyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockInstanceAdmin.err = gstatus.Error(errCode, "test error")

	var formattedResource string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var policy *iampb.Policy = &iampb.Policy{}
	var request = &iampb.SetIamPolicyRequest{
		Resource: formattedResource,
		Policy:   policy,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
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
func TestInstanceAdminGetIamPolicy(t *testing.T) {
	var version int32 = 351608024
	var etag []byte = []byte("21")
	var expectedResponse = &iampb.Policy{
		Version: version,
		Etag:    etag,
	}

	mockInstanceAdmin.err = nil
	mockInstanceAdmin.reqs = nil

	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], expectedResponse)

	var formattedResource string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var request = &iampb.GetIamPolicyRequest{
		Resource: formattedResource,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetIamPolicy(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockInstanceAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestInstanceAdminGetIamPolicyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockInstanceAdmin.err = gstatus.Error(errCode, "test error")

	var formattedResource string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var request = &iampb.GetIamPolicyRequest{
		Resource: formattedResource,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
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
func TestInstanceAdminTestIamPermissions(t *testing.T) {
	var expectedResponse *iampb.TestIamPermissionsResponse = &iampb.TestIamPermissionsResponse{}

	mockInstanceAdmin.err = nil
	mockInstanceAdmin.reqs = nil

	mockInstanceAdmin.resps = append(mockInstanceAdmin.resps[:0], expectedResponse)

	var formattedResource string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var permissions []string = nil
	var request = &iampb.TestIamPermissionsRequest{
		Resource:    formattedResource,
		Permissions: permissions,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.TestIamPermissions(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockInstanceAdmin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestInstanceAdminTestIamPermissionsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockInstanceAdmin.err = gstatus.Error(errCode, "test error")

	var formattedResource string = fmt.Sprintf("projects/%s/instances/%s", "[PROJECT]", "[INSTANCE]")
	var permissions []string = nil
	var request = &iampb.TestIamPermissionsRequest{
		Resource:    formattedResource,
		Permissions: permissions,
	}

	c, err := NewInstanceAdminClient(context.Background(), clientOpt)
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
