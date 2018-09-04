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

package dlp

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	dlppb "google.golang.org/genproto/googleapis/privacy/dlp/v2"
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

type mockDlpServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	dlppb.DlpServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockDlpServer) InspectContent(ctx context.Context, req *dlppb.InspectContentRequest) (*dlppb.InspectContentResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.InspectContentResponse), nil
}

func (s *mockDlpServer) RedactImage(ctx context.Context, req *dlppb.RedactImageRequest) (*dlppb.RedactImageResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.RedactImageResponse), nil
}

func (s *mockDlpServer) DeidentifyContent(ctx context.Context, req *dlppb.DeidentifyContentRequest) (*dlppb.DeidentifyContentResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.DeidentifyContentResponse), nil
}

func (s *mockDlpServer) ReidentifyContent(ctx context.Context, req *dlppb.ReidentifyContentRequest) (*dlppb.ReidentifyContentResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.ReidentifyContentResponse), nil
}

func (s *mockDlpServer) ListInfoTypes(ctx context.Context, req *dlppb.ListInfoTypesRequest) (*dlppb.ListInfoTypesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.ListInfoTypesResponse), nil
}

func (s *mockDlpServer) CreateInspectTemplate(ctx context.Context, req *dlppb.CreateInspectTemplateRequest) (*dlppb.InspectTemplate, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.InspectTemplate), nil
}

func (s *mockDlpServer) UpdateInspectTemplate(ctx context.Context, req *dlppb.UpdateInspectTemplateRequest) (*dlppb.InspectTemplate, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.InspectTemplate), nil
}

func (s *mockDlpServer) GetInspectTemplate(ctx context.Context, req *dlppb.GetInspectTemplateRequest) (*dlppb.InspectTemplate, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.InspectTemplate), nil
}

func (s *mockDlpServer) ListInspectTemplates(ctx context.Context, req *dlppb.ListInspectTemplatesRequest) (*dlppb.ListInspectTemplatesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.ListInspectTemplatesResponse), nil
}

func (s *mockDlpServer) DeleteInspectTemplate(ctx context.Context, req *dlppb.DeleteInspectTemplateRequest) (*emptypb.Empty, error) {
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

func (s *mockDlpServer) CreateDeidentifyTemplate(ctx context.Context, req *dlppb.CreateDeidentifyTemplateRequest) (*dlppb.DeidentifyTemplate, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.DeidentifyTemplate), nil
}

func (s *mockDlpServer) UpdateDeidentifyTemplate(ctx context.Context, req *dlppb.UpdateDeidentifyTemplateRequest) (*dlppb.DeidentifyTemplate, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.DeidentifyTemplate), nil
}

func (s *mockDlpServer) GetDeidentifyTemplate(ctx context.Context, req *dlppb.GetDeidentifyTemplateRequest) (*dlppb.DeidentifyTemplate, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.DeidentifyTemplate), nil
}

func (s *mockDlpServer) ListDeidentifyTemplates(ctx context.Context, req *dlppb.ListDeidentifyTemplatesRequest) (*dlppb.ListDeidentifyTemplatesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.ListDeidentifyTemplatesResponse), nil
}

func (s *mockDlpServer) DeleteDeidentifyTemplate(ctx context.Context, req *dlppb.DeleteDeidentifyTemplateRequest) (*emptypb.Empty, error) {
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

func (s *mockDlpServer) CreateJobTrigger(ctx context.Context, req *dlppb.CreateJobTriggerRequest) (*dlppb.JobTrigger, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.JobTrigger), nil
}

func (s *mockDlpServer) UpdateJobTrigger(ctx context.Context, req *dlppb.UpdateJobTriggerRequest) (*dlppb.JobTrigger, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.JobTrigger), nil
}

func (s *mockDlpServer) GetJobTrigger(ctx context.Context, req *dlppb.GetJobTriggerRequest) (*dlppb.JobTrigger, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.JobTrigger), nil
}

func (s *mockDlpServer) ListJobTriggers(ctx context.Context, req *dlppb.ListJobTriggersRequest) (*dlppb.ListJobTriggersResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.ListJobTriggersResponse), nil
}

func (s *mockDlpServer) DeleteJobTrigger(ctx context.Context, req *dlppb.DeleteJobTriggerRequest) (*emptypb.Empty, error) {
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

func (s *mockDlpServer) CreateDlpJob(ctx context.Context, req *dlppb.CreateDlpJobRequest) (*dlppb.DlpJob, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.DlpJob), nil
}

func (s *mockDlpServer) ListDlpJobs(ctx context.Context, req *dlppb.ListDlpJobsRequest) (*dlppb.ListDlpJobsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.ListDlpJobsResponse), nil
}

func (s *mockDlpServer) GetDlpJob(ctx context.Context, req *dlppb.GetDlpJobRequest) (*dlppb.DlpJob, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dlppb.DlpJob), nil
}

func (s *mockDlpServer) DeleteDlpJob(ctx context.Context, req *dlppb.DeleteDlpJobRequest) (*emptypb.Empty, error) {
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

func (s *mockDlpServer) CancelDlpJob(ctx context.Context, req *dlppb.CancelDlpJobRequest) (*emptypb.Empty, error) {
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
	mockDlp mockDlpServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	dlppb.RegisterDlpServiceServer(serv, &mockDlp)

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

func TestDlpServiceInspectContent(t *testing.T) {
	var expectedResponse *dlppb.InspectContentResponse = &dlppb.InspectContentResponse{}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.InspectContentRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.InspectContent(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceInspectContentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.InspectContentRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.InspectContent(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceRedactImage(t *testing.T) {
	var redactedImage []byte = []byte("28")
	var extractedText string = "extractedText998260012"
	var expectedResponse = &dlppb.RedactImageResponse{
		RedactedImage: redactedImage,
		ExtractedText: extractedText,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.RedactImageRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.RedactImage(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceRedactImageError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.RedactImageRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.RedactImage(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceDeidentifyContent(t *testing.T) {
	var expectedResponse *dlppb.DeidentifyContentResponse = &dlppb.DeidentifyContentResponse{}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.DeidentifyContentRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.DeidentifyContent(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceDeidentifyContentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.DeidentifyContentRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.DeidentifyContent(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceReidentifyContent(t *testing.T) {
	var expectedResponse *dlppb.ReidentifyContentResponse = &dlppb.ReidentifyContentResponse{}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.ReidentifyContentRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ReidentifyContent(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceReidentifyContentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.ReidentifyContentRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ReidentifyContent(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceListInfoTypes(t *testing.T) {
	var expectedResponse *dlppb.ListInfoTypesResponse = &dlppb.ListInfoTypesResponse{}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var request *dlppb.ListInfoTypesRequest = &dlppb.ListInfoTypesRequest{}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListInfoTypes(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceListInfoTypesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var request *dlppb.ListInfoTypesRequest = &dlppb.ListInfoTypesRequest{}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListInfoTypes(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceCreateInspectTemplate(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &dlppb.InspectTemplate{
		Name:        name,
		DisplayName: displayName,
		Description: description,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("organizations/%s", "[ORGANIZATION]")
	var request = &dlppb.CreateInspectTemplateRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateInspectTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceCreateInspectTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("organizations/%s", "[ORGANIZATION]")
	var request = &dlppb.CreateInspectTemplateRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateInspectTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceUpdateInspectTemplate(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &dlppb.InspectTemplate{
		Name:        name2,
		DisplayName: displayName,
		Description: description,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("organizations/%s/inspectTemplates/%s", "[ORGANIZATION]", "[INSPECT_TEMPLATE]")
	var request = &dlppb.UpdateInspectTemplateRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateInspectTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceUpdateInspectTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("organizations/%s/inspectTemplates/%s", "[ORGANIZATION]", "[INSPECT_TEMPLATE]")
	var request = &dlppb.UpdateInspectTemplateRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateInspectTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceGetInspectTemplate(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &dlppb.InspectTemplate{
		Name:        name,
		DisplayName: displayName,
		Description: description,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var request *dlppb.GetInspectTemplateRequest = &dlppb.GetInspectTemplateRequest{}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetInspectTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceGetInspectTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var request *dlppb.GetInspectTemplateRequest = &dlppb.GetInspectTemplateRequest{}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetInspectTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceListInspectTemplates(t *testing.T) {
	var nextPageToken string = ""
	var inspectTemplatesElement *dlppb.InspectTemplate = &dlppb.InspectTemplate{}
	var inspectTemplates = []*dlppb.InspectTemplate{inspectTemplatesElement}
	var expectedResponse = &dlppb.ListInspectTemplatesResponse{
		NextPageToken:    nextPageToken,
		InspectTemplates: inspectTemplates,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("organizations/%s", "[ORGANIZATION]")
	var request = &dlppb.ListInspectTemplatesRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListInspectTemplates(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.InspectTemplates[0])
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

func TestDlpServiceListInspectTemplatesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("organizations/%s", "[ORGANIZATION]")
	var request = &dlppb.ListInspectTemplatesRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListInspectTemplates(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceDeleteInspectTemplate(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("organizations/%s/inspectTemplates/%s", "[ORGANIZATION]", "[INSPECT_TEMPLATE]")
	var request = &dlppb.DeleteInspectTemplateRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteInspectTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestDlpServiceDeleteInspectTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("organizations/%s/inspectTemplates/%s", "[ORGANIZATION]", "[INSPECT_TEMPLATE]")
	var request = &dlppb.DeleteInspectTemplateRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteInspectTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestDlpServiceCreateDeidentifyTemplate(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &dlppb.DeidentifyTemplate{
		Name:        name,
		DisplayName: displayName,
		Description: description,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("organizations/%s", "[ORGANIZATION]")
	var request = &dlppb.CreateDeidentifyTemplateRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateDeidentifyTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceCreateDeidentifyTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("organizations/%s", "[ORGANIZATION]")
	var request = &dlppb.CreateDeidentifyTemplateRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateDeidentifyTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceUpdateDeidentifyTemplate(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &dlppb.DeidentifyTemplate{
		Name:        name2,
		DisplayName: displayName,
		Description: description,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("organizations/%s/deidentifyTemplates/%s", "[ORGANIZATION]", "[DEIDENTIFY_TEMPLATE]")
	var request = &dlppb.UpdateDeidentifyTemplateRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateDeidentifyTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceUpdateDeidentifyTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("organizations/%s/deidentifyTemplates/%s", "[ORGANIZATION]", "[DEIDENTIFY_TEMPLATE]")
	var request = &dlppb.UpdateDeidentifyTemplateRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateDeidentifyTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceGetDeidentifyTemplate(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &dlppb.DeidentifyTemplate{
		Name:        name2,
		DisplayName: displayName,
		Description: description,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("organizations/%s/deidentifyTemplates/%s", "[ORGANIZATION]", "[DEIDENTIFY_TEMPLATE]")
	var request = &dlppb.GetDeidentifyTemplateRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetDeidentifyTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceGetDeidentifyTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("organizations/%s/deidentifyTemplates/%s", "[ORGANIZATION]", "[DEIDENTIFY_TEMPLATE]")
	var request = &dlppb.GetDeidentifyTemplateRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetDeidentifyTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceListDeidentifyTemplates(t *testing.T) {
	var nextPageToken string = ""
	var deidentifyTemplatesElement *dlppb.DeidentifyTemplate = &dlppb.DeidentifyTemplate{}
	var deidentifyTemplates = []*dlppb.DeidentifyTemplate{deidentifyTemplatesElement}
	var expectedResponse = &dlppb.ListDeidentifyTemplatesResponse{
		NextPageToken:       nextPageToken,
		DeidentifyTemplates: deidentifyTemplates,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("organizations/%s", "[ORGANIZATION]")
	var request = &dlppb.ListDeidentifyTemplatesRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListDeidentifyTemplates(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.DeidentifyTemplates[0])
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

func TestDlpServiceListDeidentifyTemplatesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("organizations/%s", "[ORGANIZATION]")
	var request = &dlppb.ListDeidentifyTemplatesRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListDeidentifyTemplates(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceDeleteDeidentifyTemplate(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("organizations/%s/deidentifyTemplates/%s", "[ORGANIZATION]", "[DEIDENTIFY_TEMPLATE]")
	var request = &dlppb.DeleteDeidentifyTemplateRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteDeidentifyTemplate(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestDlpServiceDeleteDeidentifyTemplateError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("organizations/%s/deidentifyTemplates/%s", "[ORGANIZATION]", "[DEIDENTIFY_TEMPLATE]")
	var request = &dlppb.DeleteDeidentifyTemplateRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteDeidentifyTemplate(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestDlpServiceCreateDlpJob(t *testing.T) {
	var name string = "name3373707"
	var jobTriggerName string = "jobTriggerName1819490804"
	var expectedResponse = &dlppb.DlpJob{
		Name:           name,
		JobTriggerName: jobTriggerName,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.CreateDlpJobRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateDlpJob(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceCreateDlpJobError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.CreateDlpJobRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateDlpJob(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceListDlpJobs(t *testing.T) {
	var nextPageToken string = ""
	var jobsElement *dlppb.DlpJob = &dlppb.DlpJob{}
	var jobs = []*dlppb.DlpJob{jobsElement}
	var expectedResponse = &dlppb.ListDlpJobsResponse{
		NextPageToken: nextPageToken,
		Jobs:          jobs,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.ListDlpJobsRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListDlpJobs(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
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

func TestDlpServiceListDlpJobsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.ListDlpJobsRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListDlpJobs(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceGetDlpJob(t *testing.T) {
	var name2 string = "name2-1052831874"
	var jobTriggerName string = "jobTriggerName1819490804"
	var expectedResponse = &dlppb.DlpJob{
		Name:           name2,
		JobTriggerName: jobTriggerName,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/dlpJobs/%s", "[PROJECT]", "[DLP_JOB]")
	var request = &dlppb.GetDlpJobRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetDlpJob(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceGetDlpJobError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/dlpJobs/%s", "[PROJECT]", "[DLP_JOB]")
	var request = &dlppb.GetDlpJobRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetDlpJob(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceDeleteDlpJob(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/dlpJobs/%s", "[PROJECT]", "[DLP_JOB]")
	var request = &dlppb.DeleteDlpJobRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteDlpJob(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestDlpServiceDeleteDlpJobError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/dlpJobs/%s", "[PROJECT]", "[DLP_JOB]")
	var request = &dlppb.DeleteDlpJobRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteDlpJob(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestDlpServiceCancelDlpJob(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/dlpJobs/%s", "[PROJECT]", "[DLP_JOB]")
	var request = &dlppb.CancelDlpJobRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.CancelDlpJob(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestDlpServiceCancelDlpJobError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/dlpJobs/%s", "[PROJECT]", "[DLP_JOB]")
	var request = &dlppb.CancelDlpJobRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.CancelDlpJob(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestDlpServiceListJobTriggers(t *testing.T) {
	var nextPageToken string = ""
	var jobTriggersElement *dlppb.JobTrigger = &dlppb.JobTrigger{}
	var jobTriggers = []*dlppb.JobTrigger{jobTriggersElement}
	var expectedResponse = &dlppb.ListJobTriggersResponse{
		NextPageToken: nextPageToken,
		JobTriggers:   jobTriggers,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.ListJobTriggersRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListJobTriggers(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.JobTriggers[0])
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

func TestDlpServiceListJobTriggersError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.ListJobTriggersRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListJobTriggers(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceGetJobTrigger(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &dlppb.JobTrigger{
		Name:        name2,
		DisplayName: displayName,
		Description: description,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/jobTriggers/%s", "[PROJECT]", "[JOB_TRIGGER]")
	var request = &dlppb.GetJobTriggerRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetJobTrigger(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceGetJobTriggerError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/jobTriggers/%s", "[PROJECT]", "[JOB_TRIGGER]")
	var request = &dlppb.GetJobTriggerRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetJobTrigger(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceDeleteJobTrigger(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var name string = "name3373707"
	var request = &dlppb.DeleteJobTriggerRequest{
		Name: name,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteJobTrigger(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestDlpServiceDeleteJobTriggerError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var name string = "name3373707"
	var request = &dlppb.DeleteJobTriggerRequest{
		Name: name,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteJobTrigger(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestDlpServiceUpdateJobTrigger(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &dlppb.JobTrigger{
		Name:        name2,
		DisplayName: displayName,
		Description: description,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/jobTriggers/%s", "[PROJECT]", "[JOB_TRIGGER]")
	var request = &dlppb.UpdateJobTriggerRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateJobTrigger(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceUpdateJobTriggerError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/jobTriggers/%s", "[PROJECT]", "[JOB_TRIGGER]")
	var request = &dlppb.UpdateJobTriggerRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateJobTrigger(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestDlpServiceCreateJobTrigger(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &dlppb.JobTrigger{
		Name:        name,
		DisplayName: displayName,
		Description: description,
	}

	mockDlp.err = nil
	mockDlp.reqs = nil

	mockDlp.resps = append(mockDlp.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.CreateJobTriggerRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateJobTrigger(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockDlp.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestDlpServiceCreateJobTriggerError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockDlp.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dlppb.CreateJobTriggerRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateJobTrigger(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
