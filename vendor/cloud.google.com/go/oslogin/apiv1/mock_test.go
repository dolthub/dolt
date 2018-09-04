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

package oslogin

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	commonpb "google.golang.org/genproto/googleapis/cloud/oslogin/common"
	osloginpb "google.golang.org/genproto/googleapis/cloud/oslogin/v1"
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

type mockOsLoginServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	osloginpb.OsLoginServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockOsLoginServer) DeletePosixAccount(ctx context.Context, req *osloginpb.DeletePosixAccountRequest) (*emptypb.Empty, error) {
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

func (s *mockOsLoginServer) DeleteSshPublicKey(ctx context.Context, req *osloginpb.DeleteSshPublicKeyRequest) (*emptypb.Empty, error) {
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

func (s *mockOsLoginServer) GetLoginProfile(ctx context.Context, req *osloginpb.GetLoginProfileRequest) (*osloginpb.LoginProfile, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*osloginpb.LoginProfile), nil
}

func (s *mockOsLoginServer) GetSshPublicKey(ctx context.Context, req *osloginpb.GetSshPublicKeyRequest) (*commonpb.SshPublicKey, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*commonpb.SshPublicKey), nil
}

func (s *mockOsLoginServer) ImportSshPublicKey(ctx context.Context, req *osloginpb.ImportSshPublicKeyRequest) (*osloginpb.ImportSshPublicKeyResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*osloginpb.ImportSshPublicKeyResponse), nil
}

func (s *mockOsLoginServer) UpdateSshPublicKey(ctx context.Context, req *osloginpb.UpdateSshPublicKeyRequest) (*commonpb.SshPublicKey, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*commonpb.SshPublicKey), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockOsLogin mockOsLoginServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	osloginpb.RegisterOsLoginServiceServer(serv, &mockOsLogin)

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

func TestOsLoginServiceDeletePosixAccount(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockOsLogin.err = nil
	mockOsLogin.reqs = nil

	mockOsLogin.resps = append(mockOsLogin.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("users/%s/projects/%s", "[USER]", "[PROJECT]")
	var request = &osloginpb.DeletePosixAccountRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeletePosixAccount(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockOsLogin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestOsLoginServiceDeletePosixAccountError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockOsLogin.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("users/%s/projects/%s", "[USER]", "[PROJECT]")
	var request = &osloginpb.DeletePosixAccountRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeletePosixAccount(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestOsLoginServiceDeleteSshPublicKey(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockOsLogin.err = nil
	mockOsLogin.reqs = nil

	mockOsLogin.resps = append(mockOsLogin.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("users/%s/sshPublicKeys/%s", "[USER]", "[FINGERPRINT]")
	var request = &osloginpb.DeleteSshPublicKeyRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteSshPublicKey(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockOsLogin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestOsLoginServiceDeleteSshPublicKeyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockOsLogin.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("users/%s/sshPublicKeys/%s", "[USER]", "[FINGERPRINT]")
	var request = &osloginpb.DeleteSshPublicKeyRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteSshPublicKey(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestOsLoginServiceGetLoginProfile(t *testing.T) {
	var name2 string = "name2-1052831874"
	var suspended bool = false
	var expectedResponse = &osloginpb.LoginProfile{
		Name:      name2,
		Suspended: suspended,
	}

	mockOsLogin.err = nil
	mockOsLogin.reqs = nil

	mockOsLogin.resps = append(mockOsLogin.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("users/%s", "[USER]")
	var request = &osloginpb.GetLoginProfileRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetLoginProfile(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockOsLogin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestOsLoginServiceGetLoginProfileError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockOsLogin.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("users/%s", "[USER]")
	var request = &osloginpb.GetLoginProfileRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetLoginProfile(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestOsLoginServiceGetSshPublicKey(t *testing.T) {
	var key string = "key106079"
	var expirationTimeUsec int64 = 2058878882
	var fingerprint string = "fingerprint-1375934236"
	var expectedResponse = &commonpb.SshPublicKey{
		Key:                key,
		ExpirationTimeUsec: expirationTimeUsec,
		Fingerprint:        fingerprint,
	}

	mockOsLogin.err = nil
	mockOsLogin.reqs = nil

	mockOsLogin.resps = append(mockOsLogin.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("users/%s/sshPublicKeys/%s", "[USER]", "[FINGERPRINT]")
	var request = &osloginpb.GetSshPublicKeyRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetSshPublicKey(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockOsLogin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestOsLoginServiceGetSshPublicKeyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockOsLogin.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("users/%s/sshPublicKeys/%s", "[USER]", "[FINGERPRINT]")
	var request = &osloginpb.GetSshPublicKeyRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetSshPublicKey(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestOsLoginServiceImportSshPublicKey(t *testing.T) {
	var expectedResponse *osloginpb.ImportSshPublicKeyResponse = &osloginpb.ImportSshPublicKeyResponse{}

	mockOsLogin.err = nil
	mockOsLogin.reqs = nil

	mockOsLogin.resps = append(mockOsLogin.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("users/%s", "[USER]")
	var sshPublicKey *commonpb.SshPublicKey = &commonpb.SshPublicKey{}
	var request = &osloginpb.ImportSshPublicKeyRequest{
		Parent:       formattedParent,
		SshPublicKey: sshPublicKey,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ImportSshPublicKey(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockOsLogin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestOsLoginServiceImportSshPublicKeyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockOsLogin.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("users/%s", "[USER]")
	var sshPublicKey *commonpb.SshPublicKey = &commonpb.SshPublicKey{}
	var request = &osloginpb.ImportSshPublicKeyRequest{
		Parent:       formattedParent,
		SshPublicKey: sshPublicKey,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ImportSshPublicKey(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestOsLoginServiceUpdateSshPublicKey(t *testing.T) {
	var key string = "key106079"
	var expirationTimeUsec int64 = 2058878882
	var fingerprint string = "fingerprint-1375934236"
	var expectedResponse = &commonpb.SshPublicKey{
		Key:                key,
		ExpirationTimeUsec: expirationTimeUsec,
		Fingerprint:        fingerprint,
	}

	mockOsLogin.err = nil
	mockOsLogin.reqs = nil

	mockOsLogin.resps = append(mockOsLogin.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("users/%s/sshPublicKeys/%s", "[USER]", "[FINGERPRINT]")
	var sshPublicKey *commonpb.SshPublicKey = &commonpb.SshPublicKey{}
	var request = &osloginpb.UpdateSshPublicKeyRequest{
		Name:         formattedName,
		SshPublicKey: sshPublicKey,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateSshPublicKey(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockOsLogin.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestOsLoginServiceUpdateSshPublicKeyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockOsLogin.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("users/%s/sshPublicKeys/%s", "[USER]", "[FINGERPRINT]")
	var sshPublicKey *commonpb.SshPublicKey = &commonpb.SshPublicKey{}
	var request = &osloginpb.UpdateSshPublicKeyRequest{
		Name:         formattedName,
		SshPublicKey: sshPublicKey,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateSshPublicKey(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
