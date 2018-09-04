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

package dataproc

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1beta2"
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

type mockClusterControllerServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	dataprocpb.ClusterControllerServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockClusterControllerServer) CreateCluster(ctx context.Context, req *dataprocpb.CreateClusterRequest) (*longrunningpb.Operation, error) {
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

func (s *mockClusterControllerServer) UpdateCluster(ctx context.Context, req *dataprocpb.UpdateClusterRequest) (*longrunningpb.Operation, error) {
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

func (s *mockClusterControllerServer) DeleteCluster(ctx context.Context, req *dataprocpb.DeleteClusterRequest) (*longrunningpb.Operation, error) {
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

func (s *mockClusterControllerServer) GetCluster(ctx context.Context, req *dataprocpb.GetClusterRequest) (*dataprocpb.Cluster, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.Cluster), nil
}

func (s *mockClusterControllerServer) ListClusters(ctx context.Context, req *dataprocpb.ListClustersRequest) (*dataprocpb.ListClustersResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.ListClustersResponse), nil
}

func (s *mockClusterControllerServer) DiagnoseCluster(ctx context.Context, req *dataprocpb.DiagnoseClusterRequest) (*longrunningpb.Operation, error) {
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

type mockJobControllerServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	dataprocpb.JobControllerServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockJobControllerServer) SubmitJob(ctx context.Context, req *dataprocpb.SubmitJobRequest) (*dataprocpb.Job, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.Job), nil
}

func (s *mockJobControllerServer) GetJob(ctx context.Context, req *dataprocpb.GetJobRequest) (*dataprocpb.Job, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.Job), nil
}

func (s *mockJobControllerServer) ListJobs(ctx context.Context, req *dataprocpb.ListJobsRequest) (*dataprocpb.ListJobsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.ListJobsResponse), nil
}

func (s *mockJobControllerServer) UpdateJob(ctx context.Context, req *dataprocpb.UpdateJobRequest) (*dataprocpb.Job, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.Job), nil
}

func (s *mockJobControllerServer) CancelJob(ctx context.Context, req *dataprocpb.CancelJobRequest) (*dataprocpb.Job, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.Job), nil
}

func (s *mockJobControllerServer) DeleteJob(ctx context.Context, req *dataprocpb.DeleteJobRequest) (*emptypb.Empty, error) {
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

type mockWorkflowTemplateServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	dataprocpb.WorkflowTemplateServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockWorkflowTemplateServer) CreateWorkflowTemplate(ctx context.Context, req *dataprocpb.CreateWorkflowTemplateRequest) (*dataprocpb.WorkflowTemplate, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.WorkflowTemplate), nil
}

func (s *mockWorkflowTemplateServer) GetWorkflowTemplate(ctx context.Context, req *dataprocpb.GetWorkflowTemplateRequest) (*dataprocpb.WorkflowTemplate, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.WorkflowTemplate), nil
}

func (s *mockWorkflowTemplateServer) InstantiateWorkflowTemplate(ctx context.Context, req *dataprocpb.InstantiateWorkflowTemplateRequest) (*longrunningpb.Operation, error) {
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

func (s *mockWorkflowTemplateServer) UpdateWorkflowTemplate(ctx context.Context, req *dataprocpb.UpdateWorkflowTemplateRequest) (*dataprocpb.WorkflowTemplate, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.WorkflowTemplate), nil
}

func (s *mockWorkflowTemplateServer) ListWorkflowTemplates(ctx context.Context, req *dataprocpb.ListWorkflowTemplatesRequest) (*dataprocpb.ListWorkflowTemplatesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dataprocpb.ListWorkflowTemplatesResponse), nil
}

func (s *mockWorkflowTemplateServer) DeleteWorkflowTemplate(ctx context.Context, req *dataprocpb.DeleteWorkflowTemplateRequest) (*emptypb.Empty, error) {
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
	mockClusterController mockClusterControllerServer
	mockJobController     mockJobControllerServer
	mockWorkflowTemplate  mockWorkflowTemplateServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	dataprocpb.RegisterClusterControllerServer(serv, &mockClusterController)
	dataprocpb.RegisterJobControllerServer(serv, &mockJobController)
	dataprocpb.RegisterWorkflowTemplateServiceServer(serv, &mockWorkflowTemplate)

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

func TestClusterControllerCreateCluster(t *testing.T) {
	var projectId2 string = "projectId2939242356"
	var clusterName string = "clusterName-1018081872"
	var clusterUuid string = "clusterUuid-1017854240"
	var expectedResponse = &dataprocpb.Cluster{
		ProjectId:   projectId2,
		ClusterName: clusterName,
		ClusterUuid: clusterUuid,
	}

	mockClusterController.err = nil
	mockClusterController.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockClusterController.resps = append(mockClusterController.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var cluster *dataprocpb.Cluster = &dataprocpb.Cluster{}
	var request = &dataprocpb.CreateClusterRequest{
		ProjectId: projectId,
		Region:    region,
		Cluster:   cluster,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.CreateCluster(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockClusterController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestClusterControllerCreateClusterError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockClusterController.err = nil
	mockClusterController.resps = append(mockClusterController.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var cluster *dataprocpb.Cluster = &dataprocpb.Cluster{}
	var request = &dataprocpb.CreateClusterRequest{
		ProjectId: projectId,
		Region:    region,
		Cluster:   cluster,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.CreateCluster(context.Background(), request)
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
func TestClusterControllerUpdateCluster(t *testing.T) {
	var projectId2 string = "projectId2939242356"
	var clusterName2 string = "clusterName2875867491"
	var clusterUuid string = "clusterUuid-1017854240"
	var expectedResponse = &dataprocpb.Cluster{
		ProjectId:   projectId2,
		ClusterName: clusterName2,
		ClusterUuid: clusterUuid,
	}

	mockClusterController.err = nil
	mockClusterController.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockClusterController.resps = append(mockClusterController.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var clusterName string = "clusterName-1018081872"
	var cluster *dataprocpb.Cluster = &dataprocpb.Cluster{}
	var updateMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &dataprocpb.UpdateClusterRequest{
		ProjectId:   projectId,
		Region:      region,
		ClusterName: clusterName,
		Cluster:     cluster,
		UpdateMask:  updateMask,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.UpdateCluster(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockClusterController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestClusterControllerUpdateClusterError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockClusterController.err = nil
	mockClusterController.resps = append(mockClusterController.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var clusterName string = "clusterName-1018081872"
	var cluster *dataprocpb.Cluster = &dataprocpb.Cluster{}
	var updateMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &dataprocpb.UpdateClusterRequest{
		ProjectId:   projectId,
		Region:      region,
		ClusterName: clusterName,
		Cluster:     cluster,
		UpdateMask:  updateMask,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.UpdateCluster(context.Background(), request)
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
func TestClusterControllerDeleteCluster(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockClusterController.err = nil
	mockClusterController.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockClusterController.resps = append(mockClusterController.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var clusterName string = "clusterName-1018081872"
	var request = &dataprocpb.DeleteClusterRequest{
		ProjectId:   projectId,
		Region:      region,
		ClusterName: clusterName,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.DeleteCluster(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockClusterController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestClusterControllerDeleteClusterError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockClusterController.err = nil
	mockClusterController.resps = append(mockClusterController.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var clusterName string = "clusterName-1018081872"
	var request = &dataprocpb.DeleteClusterRequest{
		ProjectId:   projectId,
		Region:      region,
		ClusterName: clusterName,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.DeleteCluster(context.Background(), request)
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
func TestClusterControllerGetCluster(t *testing.T) {
	var projectId2 string = "projectId2939242356"
	var clusterName2 string = "clusterName2875867491"
	var clusterUuid string = "clusterUuid-1017854240"
	var expectedResponse = &dataprocpb.Cluster{
		ProjectId:   projectId2,
		ClusterName: clusterName2,
		ClusterUuid: clusterUuid,
	}

	mockClusterController.err = nil
	mockClusterController.reqs = nil

	mockClusterController.resps = append(mockClusterController.resps[:0], expectedResponse)

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var clusterName string = "clusterName-1018081872"
	var request = &dataprocpb.GetClusterRequest{
		ProjectId:   projectId,
		Region:      region,
		ClusterName: clusterName,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetCluster(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockClusterController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestClusterControllerGetClusterError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockClusterController.err = gstatus.Error(errCode, "test error")

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var clusterName string = "clusterName-1018081872"
	var request = &dataprocpb.GetClusterRequest{
		ProjectId:   projectId,
		Region:      region,
		ClusterName: clusterName,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetCluster(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestClusterControllerListClusters(t *testing.T) {
	var nextPageToken string = ""
	var clustersElement *dataprocpb.Cluster = &dataprocpb.Cluster{}
	var clusters = []*dataprocpb.Cluster{clustersElement}
	var expectedResponse = &dataprocpb.ListClustersResponse{
		NextPageToken: nextPageToken,
		Clusters:      clusters,
	}

	mockClusterController.err = nil
	mockClusterController.reqs = nil

	mockClusterController.resps = append(mockClusterController.resps[:0], expectedResponse)

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var request = &dataprocpb.ListClustersRequest{
		ProjectId: projectId,
		Region:    region,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListClusters(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockClusterController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Clusters[0])
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

func TestClusterControllerListClustersError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockClusterController.err = gstatus.Error(errCode, "test error")

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var request = &dataprocpb.ListClustersRequest{
		ProjectId: projectId,
		Region:    region,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListClusters(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestClusterControllerDiagnoseCluster(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockClusterController.err = nil
	mockClusterController.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockClusterController.resps = append(mockClusterController.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var clusterName string = "clusterName-1018081872"
	var request = &dataprocpb.DiagnoseClusterRequest{
		ProjectId:   projectId,
		Region:      region,
		ClusterName: clusterName,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.DiagnoseCluster(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockClusterController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestClusterControllerDiagnoseClusterError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockClusterController.err = nil
	mockClusterController.resps = append(mockClusterController.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var clusterName string = "clusterName-1018081872"
	var request = &dataprocpb.DiagnoseClusterRequest{
		ProjectId:   projectId,
		Region:      region,
		ClusterName: clusterName,
	}

	c, err := NewClusterControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.DiagnoseCluster(context.Background(), request)
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
func TestJobControllerSubmitJob(t *testing.T) {
	var driverOutputResourceUri string = "driverOutputResourceUri-542229086"
	var driverControlFilesUri string = "driverControlFilesUri207057643"
	var expectedResponse = &dataprocpb.Job{
		DriverOutputResourceUri: driverOutputResourceUri,
		DriverControlFilesUri:   driverControlFilesUri,
	}

	mockJobController.err = nil
	mockJobController.reqs = nil

	mockJobController.resps = append(mockJobController.resps[:0], expectedResponse)

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var job *dataprocpb.Job = &dataprocpb.Job{}
	var request = &dataprocpb.SubmitJobRequest{
		ProjectId: projectId,
		Region:    region,
		Job:       job,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SubmitJob(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockJobController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestJobControllerSubmitJobError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockJobController.err = gstatus.Error(errCode, "test error")

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var job *dataprocpb.Job = &dataprocpb.Job{}
	var request = &dataprocpb.SubmitJobRequest{
		ProjectId: projectId,
		Region:    region,
		Job:       job,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SubmitJob(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestJobControllerGetJob(t *testing.T) {
	var driverOutputResourceUri string = "driverOutputResourceUri-542229086"
	var driverControlFilesUri string = "driverControlFilesUri207057643"
	var expectedResponse = &dataprocpb.Job{
		DriverOutputResourceUri: driverOutputResourceUri,
		DriverControlFilesUri:   driverControlFilesUri,
	}

	mockJobController.err = nil
	mockJobController.reqs = nil

	mockJobController.resps = append(mockJobController.resps[:0], expectedResponse)

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var jobId string = "jobId-1154752291"
	var request = &dataprocpb.GetJobRequest{
		ProjectId: projectId,
		Region:    region,
		JobId:     jobId,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetJob(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockJobController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestJobControllerGetJobError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockJobController.err = gstatus.Error(errCode, "test error")

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var jobId string = "jobId-1154752291"
	var request = &dataprocpb.GetJobRequest{
		ProjectId: projectId,
		Region:    region,
		JobId:     jobId,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetJob(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestJobControllerListJobs(t *testing.T) {
	var nextPageToken string = ""
	var jobsElement *dataprocpb.Job = &dataprocpb.Job{}
	var jobs = []*dataprocpb.Job{jobsElement}
	var expectedResponse = &dataprocpb.ListJobsResponse{
		NextPageToken: nextPageToken,
		Jobs:          jobs,
	}

	mockJobController.err = nil
	mockJobController.reqs = nil

	mockJobController.resps = append(mockJobController.resps[:0], expectedResponse)

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var request = &dataprocpb.ListJobsRequest{
		ProjectId: projectId,
		Region:    region,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListJobs(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockJobController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Jobs[0])
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

func TestJobControllerListJobsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockJobController.err = gstatus.Error(errCode, "test error")

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var request = &dataprocpb.ListJobsRequest{
		ProjectId: projectId,
		Region:    region,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListJobs(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestJobControllerUpdateJob(t *testing.T) {
	var driverOutputResourceUri string = "driverOutputResourceUri-542229086"
	var driverControlFilesUri string = "driverControlFilesUri207057643"
	var expectedResponse = &dataprocpb.Job{
		DriverOutputResourceUri: driverOutputResourceUri,
		DriverControlFilesUri:   driverControlFilesUri,
	}

	mockJobController.err = nil
	mockJobController.reqs = nil

	mockJobController.resps = append(mockJobController.resps[:0], expectedResponse)

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var jobId string = "jobId-1154752291"
	var job *dataprocpb.Job = &dataprocpb.Job{}
	var updateMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &dataprocpb.UpdateJobRequest{
		ProjectId:  projectId,
		Region:     region,
		JobId:      jobId,
		Job:        job,
		UpdateMask: updateMask,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateJob(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockJobController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestJobControllerUpdateJobError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockJobController.err = gstatus.Error(errCode, "test error")

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var jobId string = "jobId-1154752291"
	var job *dataprocpb.Job = &dataprocpb.Job{}
	var updateMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &dataprocpb.UpdateJobRequest{
		ProjectId:  projectId,
		Region:     region,
		JobId:      jobId,
		Job:        job,
		UpdateMask: updateMask,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateJob(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestJobControllerCancelJob(t *testing.T) {
	var driverOutputResourceUri string = "driverOutputResourceUri-542229086"
	var driverControlFilesUri string = "driverControlFilesUri207057643"
	var expectedResponse = &dataprocpb.Job{
		DriverOutputResourceUri: driverOutputResourceUri,
		DriverControlFilesUri:   driverControlFilesUri,
	}

	mockJobController.err = nil
	mockJobController.reqs = nil

	mockJobController.resps = append(mockJobController.resps[:0], expectedResponse)

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var jobId string = "jobId-1154752291"
	var request = &dataprocpb.CancelJobRequest{
		ProjectId: projectId,
		Region:    region,
		JobId:     jobId,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CancelJob(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockJobController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestJobControllerCancelJobError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockJobController.err = gstatus.Error(errCode, "test error")

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var jobId string = "jobId-1154752291"
	var request = &dataprocpb.CancelJobRequest{
		ProjectId: projectId,
		Region:    region,
		JobId:     jobId,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CancelJob(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestJobControllerDeleteJob(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockJobController.err = nil
	mockJobController.reqs = nil

	mockJobController.resps = append(mockJobController.resps[:0], expectedResponse)

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var jobId string = "jobId-1154752291"
	var request = &dataprocpb.DeleteJobRequest{
		ProjectId: projectId,
		Region:    region,
		JobId:     jobId,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteJob(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockJobController.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestJobControllerDeleteJobError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockJobController.err = gstatus.Error(errCode, "test error")

	var projectId string = "projectId-1969970175"
	var region string = "region-934795532"
	var jobId string = "jobId-1154752291"
	var request = &dataprocpb.DeleteJobRequest{
		ProjectId: projectId,
		Region:    region,
		JobId:     jobId,
	}

	c, err := NewJobControllerClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteJob(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestWorkflowTemplateServiceCreateWorkflowTemplate(t *testing.T) {
	var id string = "id3355"
	var name string = "name3373707"
	var version int32 = 351608024
	var expectedResponse = &dataprocpb.WorkflowTemplate{
		Id:      id,
		Name:    name,
		Version: version,
	}

	mockWorkflowTemplate.err = nil
	mockWorkflowTemplate.reqs = nil

	mockWorkflowTemplate.resps = append(mockWorkflowTemplate.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/regions/%s", "[PROJECT]", "[REGION]")
	var template *dataprocpb.WorkflowTemplate = &dataprocpb.WorkflowTemplate{}
	var request = &dataprocpb.CreateWorkflowTemplateRequest{
		Parent:   formattedParent,
		Template: template,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateWorkflowTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockWorkflowTemplate.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestWorkflowTemplateServiceCreateWorkflowTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockWorkflowTemplate.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/regions/%s", "[PROJECT]", "[REGION]")
	var template *dataprocpb.WorkflowTemplate = &dataprocpb.WorkflowTemplate{}
	var request = &dataprocpb.CreateWorkflowTemplateRequest{
		Parent:   formattedParent,
		Template: template,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateWorkflowTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestWorkflowTemplateServiceGetWorkflowTemplate(t *testing.T) {
	var id string = "id3355"
	var name2 string = "name2-1052831874"
	var version int32 = 351608024
	var expectedResponse = &dataprocpb.WorkflowTemplate{
		Id:      id,
		Name:    name2,
		Version: version,
	}

	mockWorkflowTemplate.err = nil
	mockWorkflowTemplate.reqs = nil

	mockWorkflowTemplate.resps = append(mockWorkflowTemplate.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/regions/%s/workflowTemplates/%s", "[PROJECT]", "[REGION]", "[WORKFLOW_TEMPLATE]")
	var request = &dataprocpb.GetWorkflowTemplateRequest{
		Name: formattedName,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetWorkflowTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockWorkflowTemplate.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestWorkflowTemplateServiceGetWorkflowTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockWorkflowTemplate.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/regions/%s/workflowTemplates/%s", "[PROJECT]", "[REGION]", "[WORKFLOW_TEMPLATE]")
	var request = &dataprocpb.GetWorkflowTemplateRequest{
		Name: formattedName,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetWorkflowTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestWorkflowTemplateServiceInstantiateWorkflowTemplate(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockWorkflowTemplate.err = nil
	mockWorkflowTemplate.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockWorkflowTemplate.resps = append(mockWorkflowTemplate.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedName string = fmt.Sprintf("projects/%s/regions/%s/workflowTemplates/%s", "[PROJECT]", "[REGION]", "[WORKFLOW_TEMPLATE]")
	var request = &dataprocpb.InstantiateWorkflowTemplateRequest{
		Name: formattedName,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.InstantiateWorkflowTemplate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockWorkflowTemplate.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestWorkflowTemplateServiceInstantiateWorkflowTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockWorkflowTemplate.err = nil
	mockWorkflowTemplate.resps = append(mockWorkflowTemplate.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedName string = fmt.Sprintf("projects/%s/regions/%s/workflowTemplates/%s", "[PROJECT]", "[REGION]", "[WORKFLOW_TEMPLATE]")
	var request = &dataprocpb.InstantiateWorkflowTemplateRequest{
		Name: formattedName,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.InstantiateWorkflowTemplate(context.Background(), request)
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
func TestWorkflowTemplateServiceUpdateWorkflowTemplate(t *testing.T) {
	var id string = "id3355"
	var name string = "name3373707"
	var version int32 = 351608024
	var expectedResponse = &dataprocpb.WorkflowTemplate{
		Id:      id,
		Name:    name,
		Version: version,
	}

	mockWorkflowTemplate.err = nil
	mockWorkflowTemplate.reqs = nil

	mockWorkflowTemplate.resps = append(mockWorkflowTemplate.resps[:0], expectedResponse)

	var template *dataprocpb.WorkflowTemplate = &dataprocpb.WorkflowTemplate{}
	var request = &dataprocpb.UpdateWorkflowTemplateRequest{
		Template: template,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateWorkflowTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockWorkflowTemplate.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestWorkflowTemplateServiceUpdateWorkflowTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockWorkflowTemplate.err = gstatus.Error(errCode, "test error")

	var template *dataprocpb.WorkflowTemplate = &dataprocpb.WorkflowTemplate{}
	var request = &dataprocpb.UpdateWorkflowTemplateRequest{
		Template: template,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateWorkflowTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestWorkflowTemplateServiceListWorkflowTemplates(t *testing.T) {
	var nextPageToken string = ""
	var templatesElement *dataprocpb.WorkflowTemplate = &dataprocpb.WorkflowTemplate{}
	var templates = []*dataprocpb.WorkflowTemplate{templatesElement}
	var expectedResponse = &dataprocpb.ListWorkflowTemplatesResponse{
		NextPageToken: nextPageToken,
		Templates:     templates,
	}

	mockWorkflowTemplate.err = nil
	mockWorkflowTemplate.reqs = nil

	mockWorkflowTemplate.resps = append(mockWorkflowTemplate.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/regions/%s", "[PROJECT]", "[REGION]")
	var request = &dataprocpb.ListWorkflowTemplatesRequest{
		Parent: formattedParent,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListWorkflowTemplates(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockWorkflowTemplate.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Templates[0])
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

func TestWorkflowTemplateServiceListWorkflowTemplatesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockWorkflowTemplate.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/regions/%s", "[PROJECT]", "[REGION]")
	var request = &dataprocpb.ListWorkflowTemplatesRequest{
		Parent: formattedParent,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListWorkflowTemplates(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestWorkflowTemplateServiceDeleteWorkflowTemplate(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockWorkflowTemplate.err = nil
	mockWorkflowTemplate.reqs = nil

	mockWorkflowTemplate.resps = append(mockWorkflowTemplate.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/regions/%s/workflowTemplates/%s", "[PROJECT]", "[REGION]", "[WORKFLOW_TEMPLATE]")
	var request = &dataprocpb.DeleteWorkflowTemplateRequest{
		Name: formattedName,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteWorkflowTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockWorkflowTemplate.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestWorkflowTemplateServiceDeleteWorkflowTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockWorkflowTemplate.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/regions/%s/workflowTemplates/%s", "[PROJECT]", "[REGION]", "[WORKFLOW_TEMPLATE]")
	var request = &dataprocpb.DeleteWorkflowTemplateRequest{
		Name: formattedName,
	}

	c, err := NewWorkflowTemplateClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteWorkflowTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
