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

package redis

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	redispb "google.golang.org/genproto/googleapis/cloud/redis/v1beta1"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
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

type mockCloudRedisServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	redispb.CloudRedisServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockCloudRedisServer) ListInstances(ctx context.Context, req *redispb.ListInstancesRequest) (*redispb.ListInstancesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*redispb.ListInstancesResponse), nil
}

func (s *mockCloudRedisServer) GetInstance(ctx context.Context, req *redispb.GetInstanceRequest) (*redispb.Instance, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*redispb.Instance), nil
}

func (s *mockCloudRedisServer) CreateInstance(ctx context.Context, req *redispb.CreateInstanceRequest) (*longrunningpb.Operation, error) {
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

func (s *mockCloudRedisServer) UpdateInstance(ctx context.Context, req *redispb.UpdateInstanceRequest) (*longrunningpb.Operation, error) {
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

func (s *mockCloudRedisServer) DeleteInstance(ctx context.Context, req *redispb.DeleteInstanceRequest) (*longrunningpb.Operation, error) {
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

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockCloudRedis mockCloudRedisServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	redispb.RegisterCloudRedisServer(serv, &mockCloudRedis)

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

func TestCloudRedisListInstances(t *testing.T) {
	var nextPageToken string = ""
	var instancesElement *redispb.Instance = &redispb.Instance{}
	var instances = []*redispb.Instance{instancesElement}
	var expectedResponse = &redispb.ListInstancesResponse{
		NextPageToken: nextPageToken,
		Instances:     instances,
	}

	mockCloudRedis.err = nil
	mockCloudRedis.reqs = nil

	mockCloudRedis.resps = append(mockCloudRedis.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var request = &redispb.ListInstancesRequest{
		Parent: formattedParent,
	}

	c, err := NewCloudRedisClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListInstances(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudRedis.reqs[0]; !proto.Equal(want, got) {
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

func TestCloudRedisListInstancesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudRedis.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var request = &redispb.ListInstancesRequest{
		Parent: formattedParent,
	}

	c, err := NewCloudRedisClient(context.Background(), clientOpt)
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
func TestCloudRedisGetInstance(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var locationId string = "locationId552319461"
	var alternativeLocationId string = "alternativeLocationId-718920621"
	var redisVersion string = "redisVersion-685310444"
	var reservedIpRange string = "reservedIpRange-1082940580"
	var host string = "host3208616"
	var port int32 = 3446913
	var currentLocationId string = "currentLocationId1312712735"
	var statusMessage string = "statusMessage-239442758"
	var memorySizeGb int32 = 34199707
	var authorizedNetwork string = "authorizedNetwork-1733809270"
	var expectedResponse = &redispb.Instance{
		Name:                  name2,
		DisplayName:           displayName,
		LocationId:            locationId,
		AlternativeLocationId: alternativeLocationId,
		RedisVersion:          redisVersion,
		ReservedIpRange:       reservedIpRange,
		Host:                  host,
		Port:                  port,
		CurrentLocationId:     currentLocationId,
		StatusMessage:         statusMessage,
		MemorySizeGb:          memorySizeGb,
		AuthorizedNetwork:     authorizedNetwork,
	}

	mockCloudRedis.err = nil
	mockCloudRedis.reqs = nil

	mockCloudRedis.resps = append(mockCloudRedis.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/instances/%s", "[PROJECT]", "[LOCATION]", "[INSTANCE]")
	var request = &redispb.GetInstanceRequest{
		Name: formattedName,
	}

	c, err := NewCloudRedisClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetInstance(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudRedis.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudRedisGetInstanceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudRedis.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/instances/%s", "[PROJECT]", "[LOCATION]", "[INSTANCE]")
	var request = &redispb.GetInstanceRequest{
		Name: formattedName,
	}

	c, err := NewCloudRedisClient(context.Background(), clientOpt)
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
func TestCloudRedisCreateInstance(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var locationId string = "locationId552319461"
	var alternativeLocationId string = "alternativeLocationId-718920621"
	var redisVersion string = "redisVersion-685310444"
	var reservedIpRange string = "reservedIpRange-1082940580"
	var host string = "host3208616"
	var port int32 = 3446913
	var currentLocationId string = "currentLocationId1312712735"
	var statusMessage string = "statusMessage-239442758"
	var memorySizeGb2 int32 = 1493816946
	var authorizedNetwork string = "authorizedNetwork-1733809270"
	var expectedResponse = &redispb.Instance{
		Name:                  name,
		DisplayName:           displayName,
		LocationId:            locationId,
		AlternativeLocationId: alternativeLocationId,
		RedisVersion:          redisVersion,
		ReservedIpRange:       reservedIpRange,
		Host:                  host,
		Port:                  port,
		CurrentLocationId:     currentLocationId,
		StatusMessage:         statusMessage,
		MemorySizeGb:          memorySizeGb2,
		AuthorizedNetwork:     authorizedNetwork,
	}

	mockCloudRedis.err = nil
	mockCloudRedis.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockCloudRedis.resps = append(mockCloudRedis.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var instanceId string = "test_instance"
	var tier redispb.Instance_Tier = redispb.Instance_BASIC
	var memorySizeGb int32 = 1
	var instance = &redispb.Instance{
		Tier:         tier,
		MemorySizeGb: memorySizeGb,
	}
	var request = &redispb.CreateInstanceRequest{
		Parent:     formattedParent,
		InstanceId: instanceId,
		Instance:   instance,
	}

	c, err := NewCloudRedisClient(context.Background(), clientOpt)
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

	if want, got := request, mockCloudRedis.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudRedisCreateInstanceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudRedis.err = nil
	mockCloudRedis.resps = append(mockCloudRedis.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var instanceId string = "test_instance"
	var tier redispb.Instance_Tier = redispb.Instance_BASIC
	var memorySizeGb int32 = 1
	var instance = &redispb.Instance{
		Tier:         tier,
		MemorySizeGb: memorySizeGb,
	}
	var request = &redispb.CreateInstanceRequest{
		Parent:     formattedParent,
		InstanceId: instanceId,
		Instance:   instance,
	}

	c, err := NewCloudRedisClient(context.Background(), clientOpt)
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
func TestCloudRedisUpdateInstance(t *testing.T) {
	var name string = "name3373707"
	var displayName2 string = "displayName21615000987"
	var locationId string = "locationId552319461"
	var alternativeLocationId string = "alternativeLocationId-718920621"
	var redisVersion string = "redisVersion-685310444"
	var reservedIpRange string = "reservedIpRange-1082940580"
	var host string = "host3208616"
	var port int32 = 3446913
	var currentLocationId string = "currentLocationId1312712735"
	var statusMessage string = "statusMessage-239442758"
	var memorySizeGb2 int32 = 1493816946
	var authorizedNetwork string = "authorizedNetwork-1733809270"
	var expectedResponse = &redispb.Instance{
		Name:                  name,
		DisplayName:           displayName2,
		LocationId:            locationId,
		AlternativeLocationId: alternativeLocationId,
		RedisVersion:          redisVersion,
		ReservedIpRange:       reservedIpRange,
		Host:                  host,
		Port:                  port,
		CurrentLocationId:     currentLocationId,
		StatusMessage:         statusMessage,
		MemorySizeGb:          memorySizeGb2,
		AuthorizedNetwork:     authorizedNetwork,
	}

	mockCloudRedis.err = nil
	mockCloudRedis.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockCloudRedis.resps = append(mockCloudRedis.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var pathsElement string = "display_name"
	var pathsElement2 string = "memory_size_gb"
	var paths = []string{pathsElement, pathsElement2}
	var updateMask = &field_maskpb.FieldMask{
		Paths: paths,
	}
	var displayName string = "UpdatedDisplayName"
	var memorySizeGb int32 = 4
	var instance = &redispb.Instance{
		DisplayName:  displayName,
		MemorySizeGb: memorySizeGb,
	}
	var request = &redispb.UpdateInstanceRequest{
		UpdateMask: updateMask,
		Instance:   instance,
	}

	c, err := NewCloudRedisClient(context.Background(), clientOpt)
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

	if want, got := request, mockCloudRedis.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestCloudRedisUpdateInstanceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudRedis.err = nil
	mockCloudRedis.resps = append(mockCloudRedis.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var pathsElement string = "display_name"
	var pathsElement2 string = "memory_size_gb"
	var paths = []string{pathsElement, pathsElement2}
	var updateMask = &field_maskpb.FieldMask{
		Paths: paths,
	}
	var displayName string = "UpdatedDisplayName"
	var memorySizeGb int32 = 4
	var instance = &redispb.Instance{
		DisplayName:  displayName,
		MemorySizeGb: memorySizeGb,
	}
	var request = &redispb.UpdateInstanceRequest{
		UpdateMask: updateMask,
		Instance:   instance,
	}

	c, err := NewCloudRedisClient(context.Background(), clientOpt)
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
func TestCloudRedisDeleteInstance(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockCloudRedis.err = nil
	mockCloudRedis.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockCloudRedis.resps = append(mockCloudRedis.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/instances/%s", "[PROJECT]", "[LOCATION]", "[INSTANCE]")
	var request = &redispb.DeleteInstanceRequest{
		Name: formattedName,
	}

	c, err := NewCloudRedisClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.DeleteInstance(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockCloudRedis.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestCloudRedisDeleteInstanceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockCloudRedis.err = nil
	mockCloudRedis.resps = append(mockCloudRedis.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/instances/%s", "[PROJECT]", "[LOCATION]", "[INSTANCE]")
	var request = &redispb.DeleteInstanceRequest{
		Name: formattedName,
	}

	c, err := NewCloudRedisClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.DeleteInstance(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
