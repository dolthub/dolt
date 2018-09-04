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

package containeranalysis

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	containeranalysispb "google.golang.org/genproto/googleapis/devtools/containeranalysis/v1beta1"
	grafeaspb "google.golang.org/genproto/googleapis/devtools/containeranalysis/v1beta1/grafeas"
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

type mockContainerAnalysisV1Beta1Server struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	containeranalysispb.ContainerAnalysisV1Beta1Server

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockContainerAnalysisV1Beta1Server) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
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

func (s *mockContainerAnalysisV1Beta1Server) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
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

func (s *mockContainerAnalysisV1Beta1Server) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
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

func (s *mockContainerAnalysisV1Beta1Server) GetScanConfig(ctx context.Context, req *containeranalysispb.GetScanConfigRequest) (*containeranalysispb.ScanConfig, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*containeranalysispb.ScanConfig), nil
}

func (s *mockContainerAnalysisV1Beta1Server) ListScanConfigs(ctx context.Context, req *containeranalysispb.ListScanConfigsRequest) (*containeranalysispb.ListScanConfigsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*containeranalysispb.ListScanConfigsResponse), nil
}

func (s *mockContainerAnalysisV1Beta1Server) UpdateScanConfig(ctx context.Context, req *containeranalysispb.UpdateScanConfigRequest) (*containeranalysispb.ScanConfig, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*containeranalysispb.ScanConfig), nil
}

type mockGrafeasV1Beta1Server struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	grafeaspb.GrafeasV1Beta1Server

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockGrafeasV1Beta1Server) GetOccurrence(ctx context.Context, req *grafeaspb.GetOccurrenceRequest) (*grafeaspb.Occurrence, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.Occurrence), nil
}

func (s *mockGrafeasV1Beta1Server) ListOccurrences(ctx context.Context, req *grafeaspb.ListOccurrencesRequest) (*grafeaspb.ListOccurrencesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.ListOccurrencesResponse), nil
}

func (s *mockGrafeasV1Beta1Server) DeleteOccurrence(ctx context.Context, req *grafeaspb.DeleteOccurrenceRequest) (*emptypb.Empty, error) {
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

func (s *mockGrafeasV1Beta1Server) CreateOccurrence(ctx context.Context, req *grafeaspb.CreateOccurrenceRequest) (*grafeaspb.Occurrence, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.Occurrence), nil
}

func (s *mockGrafeasV1Beta1Server) BatchCreateOccurrences(ctx context.Context, req *grafeaspb.BatchCreateOccurrencesRequest) (*grafeaspb.BatchCreateOccurrencesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.BatchCreateOccurrencesResponse), nil
}

func (s *mockGrafeasV1Beta1Server) UpdateOccurrence(ctx context.Context, req *grafeaspb.UpdateOccurrenceRequest) (*grafeaspb.Occurrence, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.Occurrence), nil
}

func (s *mockGrafeasV1Beta1Server) GetOccurrenceNote(ctx context.Context, req *grafeaspb.GetOccurrenceNoteRequest) (*grafeaspb.Note, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.Note), nil
}

func (s *mockGrafeasV1Beta1Server) GetNote(ctx context.Context, req *grafeaspb.GetNoteRequest) (*grafeaspb.Note, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.Note), nil
}

func (s *mockGrafeasV1Beta1Server) ListNotes(ctx context.Context, req *grafeaspb.ListNotesRequest) (*grafeaspb.ListNotesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.ListNotesResponse), nil
}

func (s *mockGrafeasV1Beta1Server) DeleteNote(ctx context.Context, req *grafeaspb.DeleteNoteRequest) (*emptypb.Empty, error) {
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

func (s *mockGrafeasV1Beta1Server) CreateNote(ctx context.Context, req *grafeaspb.CreateNoteRequest) (*grafeaspb.Note, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.Note), nil
}

func (s *mockGrafeasV1Beta1Server) BatchCreateNotes(ctx context.Context, req *grafeaspb.BatchCreateNotesRequest) (*grafeaspb.BatchCreateNotesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.BatchCreateNotesResponse), nil
}

func (s *mockGrafeasV1Beta1Server) UpdateNote(ctx context.Context, req *grafeaspb.UpdateNoteRequest) (*grafeaspb.Note, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.Note), nil
}

func (s *mockGrafeasV1Beta1Server) ListNoteOccurrences(ctx context.Context, req *grafeaspb.ListNoteOccurrencesRequest) (*grafeaspb.ListNoteOccurrencesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.ListNoteOccurrencesResponse), nil
}

func (s *mockGrafeasV1Beta1Server) GetVulnerabilityOccurrencesSummary(ctx context.Context, req *grafeaspb.GetVulnerabilityOccurrencesSummaryRequest) (*grafeaspb.VulnerabilityOccurrencesSummary, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*grafeaspb.VulnerabilityOccurrencesSummary), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockContainerAnalysisV1Beta1 mockContainerAnalysisV1Beta1Server
	mockGrafeasV1Beta1           mockGrafeasV1Beta1Server
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	containeranalysispb.RegisterContainerAnalysisV1Beta1Server(serv, &mockContainerAnalysisV1Beta1)
	grafeaspb.RegisterGrafeasV1Beta1Server(serv, &mockGrafeasV1Beta1)

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

func TestContainerAnalysisV1Beta1SetIamPolicy(t *testing.T) {
	var version int32 = 351608024
	var etag []byte = []byte("21")
	var expectedResponse = &iampb.Policy{
		Version: version,
		Etag:    etag,
	}

	mockContainerAnalysisV1Beta1.err = nil
	mockContainerAnalysisV1Beta1.reqs = nil

	mockContainerAnalysisV1Beta1.resps = append(mockContainerAnalysisV1Beta1.resps[:0], expectedResponse)

	var formattedResource string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var policy *iampb.Policy = &iampb.Policy{}
	var request = &iampb.SetIamPolicyRequest{
		Resource: formattedResource,
		Policy:   policy,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SetIamPolicy(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContainerAnalysisV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestContainerAnalysisV1Beta1SetIamPolicyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContainerAnalysisV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedResource string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var policy *iampb.Policy = &iampb.Policy{}
	var request = &iampb.SetIamPolicyRequest{
		Resource: formattedResource,
		Policy:   policy,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
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
func TestContainerAnalysisV1Beta1GetIamPolicy(t *testing.T) {
	var version int32 = 351608024
	var etag []byte = []byte("21")
	var expectedResponse = &iampb.Policy{
		Version: version,
		Etag:    etag,
	}

	mockContainerAnalysisV1Beta1.err = nil
	mockContainerAnalysisV1Beta1.reqs = nil

	mockContainerAnalysisV1Beta1.resps = append(mockContainerAnalysisV1Beta1.resps[:0], expectedResponse)

	var formattedResource string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var request = &iampb.GetIamPolicyRequest{
		Resource: formattedResource,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetIamPolicy(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContainerAnalysisV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestContainerAnalysisV1Beta1GetIamPolicyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContainerAnalysisV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedResource string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var request = &iampb.GetIamPolicyRequest{
		Resource: formattedResource,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
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
func TestContainerAnalysisV1Beta1TestIamPermissions(t *testing.T) {
	var expectedResponse *iampb.TestIamPermissionsResponse = &iampb.TestIamPermissionsResponse{}

	mockContainerAnalysisV1Beta1.err = nil
	mockContainerAnalysisV1Beta1.reqs = nil

	mockContainerAnalysisV1Beta1.resps = append(mockContainerAnalysisV1Beta1.resps[:0], expectedResponse)

	var formattedResource string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var permissions []string = nil
	var request = &iampb.TestIamPermissionsRequest{
		Resource:    formattedResource,
		Permissions: permissions,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.TestIamPermissions(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContainerAnalysisV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestContainerAnalysisV1Beta1TestIamPermissionsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContainerAnalysisV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedResource string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var permissions []string = nil
	var request = &iampb.TestIamPermissionsRequest{
		Resource:    formattedResource,
		Permissions: permissions,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
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
func TestContainerAnalysisV1Beta1GetScanConfig(t *testing.T) {
	var name2 string = "name2-1052831874"
	var description string = "description-1724546052"
	var enabled bool = false
	var expectedResponse = &containeranalysispb.ScanConfig{
		Name:        name2,
		Description: description,
		Enabled:     enabled,
	}

	mockContainerAnalysisV1Beta1.err = nil
	mockContainerAnalysisV1Beta1.reqs = nil

	mockContainerAnalysisV1Beta1.resps = append(mockContainerAnalysisV1Beta1.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/scanConfigs/%s", "[PROJECT]", "[SCAN_CONFIG]")
	var request = &containeranalysispb.GetScanConfigRequest{
		Name: formattedName,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetScanConfig(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContainerAnalysisV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestContainerAnalysisV1Beta1GetScanConfigError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContainerAnalysisV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/scanConfigs/%s", "[PROJECT]", "[SCAN_CONFIG]")
	var request = &containeranalysispb.GetScanConfigRequest{
		Name: formattedName,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetScanConfig(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestContainerAnalysisV1Beta1ListScanConfigs(t *testing.T) {
	var nextPageToken string = ""
	var scanConfigsElement *containeranalysispb.ScanConfig = &containeranalysispb.ScanConfig{}
	var scanConfigs = []*containeranalysispb.ScanConfig{scanConfigsElement}
	var expectedResponse = &containeranalysispb.ListScanConfigsResponse{
		NextPageToken: nextPageToken,
		ScanConfigs:   scanConfigs,
	}

	mockContainerAnalysisV1Beta1.err = nil
	mockContainerAnalysisV1Beta1.reqs = nil

	mockContainerAnalysisV1Beta1.resps = append(mockContainerAnalysisV1Beta1.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &containeranalysispb.ListScanConfigsRequest{
		Parent: formattedParent,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListScanConfigs(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContainerAnalysisV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.ScanConfigs[0])
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

func TestContainerAnalysisV1Beta1ListScanConfigsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContainerAnalysisV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &containeranalysispb.ListScanConfigsRequest{
		Parent: formattedParent,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListScanConfigs(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestContainerAnalysisV1Beta1UpdateScanConfig(t *testing.T) {
	var name2 string = "name2-1052831874"
	var description string = "description-1724546052"
	var enabled bool = false
	var expectedResponse = &containeranalysispb.ScanConfig{
		Name:        name2,
		Description: description,
		Enabled:     enabled,
	}

	mockContainerAnalysisV1Beta1.err = nil
	mockContainerAnalysisV1Beta1.reqs = nil

	mockContainerAnalysisV1Beta1.resps = append(mockContainerAnalysisV1Beta1.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/scanConfigs/%s", "[PROJECT]", "[SCAN_CONFIG]")
	var scanConfig *containeranalysispb.ScanConfig = &containeranalysispb.ScanConfig{}
	var request = &containeranalysispb.UpdateScanConfigRequest{
		Name:       formattedName,
		ScanConfig: scanConfig,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateScanConfig(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContainerAnalysisV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestContainerAnalysisV1Beta1UpdateScanConfigError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContainerAnalysisV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/scanConfigs/%s", "[PROJECT]", "[SCAN_CONFIG]")
	var scanConfig *containeranalysispb.ScanConfig = &containeranalysispb.ScanConfig{}
	var request = &containeranalysispb.UpdateScanConfigRequest{
		Name:       formattedName,
		ScanConfig: scanConfig,
	}

	c, err := NewContainerAnalysisV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateScanConfig(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1GetOccurrence(t *testing.T) {
	var name2 string = "name2-1052831874"
	var noteName string = "noteName1780787896"
	var remediation string = "remediation779381797"
	var expectedResponse = &grafeaspb.Occurrence{
		Name:        name2,
		NoteName:    noteName,
		Remediation: remediation,
	}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/occurrences/%s", "[PROJECT]", "[OCCURRENCE]")
	var request = &grafeaspb.GetOccurrenceRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetOccurrence(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGrafeasV1Beta1GetOccurrenceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/occurrences/%s", "[PROJECT]", "[OCCURRENCE]")
	var request = &grafeaspb.GetOccurrenceRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetOccurrence(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1ListOccurrences(t *testing.T) {
	var nextPageToken string = ""
	var occurrencesElement *grafeaspb.Occurrence = &grafeaspb.Occurrence{}
	var occurrences = []*grafeaspb.Occurrence{occurrencesElement}
	var expectedResponse = &grafeaspb.ListOccurrencesResponse{
		NextPageToken: nextPageToken,
		Occurrences:   occurrences,
	}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &grafeaspb.ListOccurrencesRequest{
		Parent: formattedParent,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListOccurrences(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Occurrences[0])
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

func TestGrafeasV1Beta1ListOccurrencesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &grafeaspb.ListOccurrencesRequest{
		Parent: formattedParent,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListOccurrences(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1DeleteOccurrence(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/occurrences/%s", "[PROJECT]", "[OCCURRENCE]")
	var request = &grafeaspb.DeleteOccurrenceRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteOccurrence(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestGrafeasV1Beta1DeleteOccurrenceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/occurrences/%s", "[PROJECT]", "[OCCURRENCE]")
	var request = &grafeaspb.DeleteOccurrenceRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteOccurrence(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestGrafeasV1Beta1CreateOccurrence(t *testing.T) {
	var name string = "name3373707"
	var noteName string = "noteName1780787896"
	var remediation string = "remediation779381797"
	var expectedResponse = &grafeaspb.Occurrence{
		Name:        name,
		NoteName:    noteName,
		Remediation: remediation,
	}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var occurrence *grafeaspb.Occurrence = &grafeaspb.Occurrence{}
	var request = &grafeaspb.CreateOccurrenceRequest{
		Parent:     formattedParent,
		Occurrence: occurrence,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateOccurrence(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGrafeasV1Beta1CreateOccurrenceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var occurrence *grafeaspb.Occurrence = &grafeaspb.Occurrence{}
	var request = &grafeaspb.CreateOccurrenceRequest{
		Parent:     formattedParent,
		Occurrence: occurrence,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateOccurrence(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1BatchCreateOccurrences(t *testing.T) {
	var expectedResponse *grafeaspb.BatchCreateOccurrencesResponse = &grafeaspb.BatchCreateOccurrencesResponse{}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var occurrences []*grafeaspb.Occurrence = nil
	var request = &grafeaspb.BatchCreateOccurrencesRequest{
		Parent:      formattedParent,
		Occurrences: occurrences,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.BatchCreateOccurrences(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGrafeasV1Beta1BatchCreateOccurrencesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var occurrences []*grafeaspb.Occurrence = nil
	var request = &grafeaspb.BatchCreateOccurrencesRequest{
		Parent:      formattedParent,
		Occurrences: occurrences,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.BatchCreateOccurrences(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1UpdateOccurrence(t *testing.T) {
	var name2 string = "name2-1052831874"
	var noteName string = "noteName1780787896"
	var remediation string = "remediation779381797"
	var expectedResponse = &grafeaspb.Occurrence{
		Name:        name2,
		NoteName:    noteName,
		Remediation: remediation,
	}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/occurrences/%s", "[PROJECT]", "[OCCURRENCE]")
	var occurrence *grafeaspb.Occurrence = &grafeaspb.Occurrence{}
	var request = &grafeaspb.UpdateOccurrenceRequest{
		Name:       formattedName,
		Occurrence: occurrence,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateOccurrence(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGrafeasV1Beta1UpdateOccurrenceError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/occurrences/%s", "[PROJECT]", "[OCCURRENCE]")
	var occurrence *grafeaspb.Occurrence = &grafeaspb.Occurrence{}
	var request = &grafeaspb.UpdateOccurrenceRequest{
		Name:       formattedName,
		Occurrence: occurrence,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateOccurrence(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1GetOccurrenceNote(t *testing.T) {
	var name2 string = "name2-1052831874"
	var shortDescription string = "shortDescription-235369287"
	var longDescription string = "longDescription-1747792199"
	var expectedResponse = &grafeaspb.Note{
		Name:             name2,
		ShortDescription: shortDescription,
		LongDescription:  longDescription,
	}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/occurrences/%s", "[PROJECT]", "[OCCURRENCE]")
	var request = &grafeaspb.GetOccurrenceNoteRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetOccurrenceNote(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGrafeasV1Beta1GetOccurrenceNoteError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/occurrences/%s", "[PROJECT]", "[OCCURRENCE]")
	var request = &grafeaspb.GetOccurrenceNoteRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetOccurrenceNote(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1GetNote(t *testing.T) {
	var name2 string = "name2-1052831874"
	var shortDescription string = "shortDescription-235369287"
	var longDescription string = "longDescription-1747792199"
	var expectedResponse = &grafeaspb.Note{
		Name:             name2,
		ShortDescription: shortDescription,
		LongDescription:  longDescription,
	}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var request = &grafeaspb.GetNoteRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetNote(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGrafeasV1Beta1GetNoteError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var request = &grafeaspb.GetNoteRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetNote(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1ListNotes(t *testing.T) {
	var nextPageToken string = ""
	var notesElement *grafeaspb.Note = &grafeaspb.Note{}
	var notes = []*grafeaspb.Note{notesElement}
	var expectedResponse = &grafeaspb.ListNotesResponse{
		NextPageToken: nextPageToken,
		Notes:         notes,
	}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &grafeaspb.ListNotesRequest{
		Parent: formattedParent,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListNotes(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Notes[0])
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

func TestGrafeasV1Beta1ListNotesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &grafeaspb.ListNotesRequest{
		Parent: formattedParent,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListNotes(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1DeleteNote(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var request = &grafeaspb.DeleteNoteRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteNote(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestGrafeasV1Beta1DeleteNoteError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var request = &grafeaspb.DeleteNoteRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteNote(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestGrafeasV1Beta1CreateNote(t *testing.T) {
	var name string = "name3373707"
	var shortDescription string = "shortDescription-235369287"
	var longDescription string = "longDescription-1747792199"
	var expectedResponse = &grafeaspb.Note{
		Name:             name,
		ShortDescription: shortDescription,
		LongDescription:  longDescription,
	}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var noteId string = "noteId2129224840"
	var note *grafeaspb.Note = &grafeaspb.Note{}
	var request = &grafeaspb.CreateNoteRequest{
		Parent: formattedParent,
		NoteId: noteId,
		Note:   note,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateNote(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGrafeasV1Beta1CreateNoteError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var noteId string = "noteId2129224840"
	var note *grafeaspb.Note = &grafeaspb.Note{}
	var request = &grafeaspb.CreateNoteRequest{
		Parent: formattedParent,
		NoteId: noteId,
		Note:   note,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateNote(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1BatchCreateNotes(t *testing.T) {
	var expectedResponse *grafeaspb.BatchCreateNotesResponse = &grafeaspb.BatchCreateNotesResponse{}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var notes map[string]*grafeaspb.Note = nil
	var request = &grafeaspb.BatchCreateNotesRequest{
		Parent: formattedParent,
		Notes:  notes,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.BatchCreateNotes(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGrafeasV1Beta1BatchCreateNotesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var notes map[string]*grafeaspb.Note = nil
	var request = &grafeaspb.BatchCreateNotesRequest{
		Parent: formattedParent,
		Notes:  notes,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.BatchCreateNotes(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1UpdateNote(t *testing.T) {
	var name2 string = "name2-1052831874"
	var shortDescription string = "shortDescription-235369287"
	var longDescription string = "longDescription-1747792199"
	var expectedResponse = &grafeaspb.Note{
		Name:             name2,
		ShortDescription: shortDescription,
		LongDescription:  longDescription,
	}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var note *grafeaspb.Note = &grafeaspb.Note{}
	var request = &grafeaspb.UpdateNoteRequest{
		Name: formattedName,
		Note: note,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateNote(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGrafeasV1Beta1UpdateNoteError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var note *grafeaspb.Note = &grafeaspb.Note{}
	var request = &grafeaspb.UpdateNoteRequest{
		Name: formattedName,
		Note: note,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateNote(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1ListNoteOccurrences(t *testing.T) {
	var nextPageToken string = ""
	var occurrencesElement *grafeaspb.Occurrence = &grafeaspb.Occurrence{}
	var occurrences = []*grafeaspb.Occurrence{occurrencesElement}
	var expectedResponse = &grafeaspb.ListNoteOccurrencesResponse{
		NextPageToken: nextPageToken,
		Occurrences:   occurrences,
	}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var request = &grafeaspb.ListNoteOccurrencesRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListNoteOccurrences(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Occurrences[0])
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

func TestGrafeasV1Beta1ListNoteOccurrencesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/notes/%s", "[PROJECT]", "[NOTE]")
	var request = &grafeaspb.ListNoteOccurrencesRequest{
		Name: formattedName,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListNoteOccurrences(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGrafeasV1Beta1GetVulnerabilityOccurrencesSummary(t *testing.T) {
	var expectedResponse *grafeaspb.VulnerabilityOccurrencesSummary = &grafeaspb.VulnerabilityOccurrencesSummary{}

	mockGrafeasV1Beta1.err = nil
	mockGrafeasV1Beta1.reqs = nil

	mockGrafeasV1Beta1.resps = append(mockGrafeasV1Beta1.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &grafeaspb.GetVulnerabilityOccurrencesSummaryRequest{
		Parent: formattedParent,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetVulnerabilityOccurrencesSummary(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGrafeasV1Beta1.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGrafeasV1Beta1GetVulnerabilityOccurrencesSummaryError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGrafeasV1Beta1.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &grafeaspb.GetVulnerabilityOccurrencesSummaryRequest{
		Parent: formattedParent,
	}

	c, err := NewGrafeasV1Beta1Client(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetVulnerabilityOccurrencesSummary(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
