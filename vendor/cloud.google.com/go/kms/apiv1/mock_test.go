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

package kms

import (
	durationpb "github.com/golang/protobuf/ptypes/duration"
	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	kmspb "google.golang.org/genproto/googleapis/cloud/kms/v1"
	iampb "google.golang.org/genproto/googleapis/iam/v1"
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

type mockKeyManagementServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	kmspb.KeyManagementServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockKeyManagementServer) ListKeyRings(ctx context.Context, req *kmspb.ListKeyRingsRequest) (*kmspb.ListKeyRingsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.ListKeyRingsResponse), nil
}

func (s *mockKeyManagementServer) ListCryptoKeys(ctx context.Context, req *kmspb.ListCryptoKeysRequest) (*kmspb.ListCryptoKeysResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.ListCryptoKeysResponse), nil
}

func (s *mockKeyManagementServer) ListCryptoKeyVersions(ctx context.Context, req *kmspb.ListCryptoKeyVersionsRequest) (*kmspb.ListCryptoKeyVersionsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.ListCryptoKeyVersionsResponse), nil
}

func (s *mockKeyManagementServer) GetKeyRing(ctx context.Context, req *kmspb.GetKeyRingRequest) (*kmspb.KeyRing, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.KeyRing), nil
}

func (s *mockKeyManagementServer) GetCryptoKey(ctx context.Context, req *kmspb.GetCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.CryptoKey), nil
}

func (s *mockKeyManagementServer) GetCryptoKeyVersion(ctx context.Context, req *kmspb.GetCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.CryptoKeyVersion), nil
}

func (s *mockKeyManagementServer) CreateKeyRing(ctx context.Context, req *kmspb.CreateKeyRingRequest) (*kmspb.KeyRing, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.KeyRing), nil
}

func (s *mockKeyManagementServer) CreateCryptoKey(ctx context.Context, req *kmspb.CreateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.CryptoKey), nil
}

func (s *mockKeyManagementServer) CreateCryptoKeyVersion(ctx context.Context, req *kmspb.CreateCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.CryptoKeyVersion), nil
}

func (s *mockKeyManagementServer) UpdateCryptoKey(ctx context.Context, req *kmspb.UpdateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.CryptoKey), nil
}

func (s *mockKeyManagementServer) UpdateCryptoKeyVersion(ctx context.Context, req *kmspb.UpdateCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.CryptoKeyVersion), nil
}

func (s *mockKeyManagementServer) Encrypt(ctx context.Context, req *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.EncryptResponse), nil
}

func (s *mockKeyManagementServer) Decrypt(ctx context.Context, req *kmspb.DecryptRequest) (*kmspb.DecryptResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.DecryptResponse), nil
}

func (s *mockKeyManagementServer) UpdateCryptoKeyPrimaryVersion(ctx context.Context, req *kmspb.UpdateCryptoKeyPrimaryVersionRequest) (*kmspb.CryptoKey, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.CryptoKey), nil
}

func (s *mockKeyManagementServer) DestroyCryptoKeyVersion(ctx context.Context, req *kmspb.DestroyCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.CryptoKeyVersion), nil
}

func (s *mockKeyManagementServer) RestoreCryptoKeyVersion(ctx context.Context, req *kmspb.RestoreCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*kmspb.CryptoKeyVersion), nil
}

type mockIamPolicyServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	iampb.IAMPolicyServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockIamPolicyServer) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
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

func (s *mockIamPolicyServer) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
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

func (s *mockIamPolicyServer) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
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
	mockKeyManagement mockKeyManagementServer
	mockIamPolicy     mockIamPolicyServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	kmspb.RegisterKeyManagementServiceServer(serv, &mockKeyManagement)
	iampb.RegisterIAMPolicyServer(serv, &mockIamPolicy)

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

func TestKeyManagementServiceListKeyRings(t *testing.T) {
	var nextPageToken string = ""
	var totalSize int32 = 705419236
	var keyRingsElement *kmspb.KeyRing = &kmspb.KeyRing{}
	var keyRings = []*kmspb.KeyRing{keyRingsElement}
	var expectedResponse = &kmspb.ListKeyRingsResponse{
		NextPageToken: nextPageToken,
		TotalSize:     totalSize,
		KeyRings:      keyRings,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var request = &kmspb.ListKeyRingsRequest{
		Parent: formattedParent,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListKeyRings(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.KeyRings[0])
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

func TestKeyManagementServiceListKeyRingsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var request = &kmspb.ListKeyRingsRequest{
		Parent: formattedParent,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListKeyRings(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceListCryptoKeys(t *testing.T) {
	var nextPageToken string = ""
	var totalSize int32 = 705419236
	var cryptoKeysElement *kmspb.CryptoKey = &kmspb.CryptoKey{}
	var cryptoKeys = []*kmspb.CryptoKey{cryptoKeysElement}
	var expectedResponse = &kmspb.ListCryptoKeysResponse{
		NextPageToken: nextPageToken,
		TotalSize:     totalSize,
		CryptoKeys:    cryptoKeys,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]")
	var request = &kmspb.ListCryptoKeysRequest{
		Parent: formattedParent,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListCryptoKeys(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.CryptoKeys[0])
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

func TestKeyManagementServiceListCryptoKeysError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]")
	var request = &kmspb.ListCryptoKeysRequest{
		Parent: formattedParent,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListCryptoKeys(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceListCryptoKeyVersions(t *testing.T) {
	var nextPageToken string = ""
	var totalSize int32 = 705419236
	var cryptoKeyVersionsElement *kmspb.CryptoKeyVersion = &kmspb.CryptoKeyVersion{}
	var cryptoKeyVersions = []*kmspb.CryptoKeyVersion{cryptoKeyVersionsElement}
	var expectedResponse = &kmspb.ListCryptoKeyVersionsResponse{
		NextPageToken:     nextPageToken,
		TotalSize:         totalSize,
		CryptoKeyVersions: cryptoKeyVersions,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]")
	var request = &kmspb.ListCryptoKeyVersionsRequest{
		Parent: formattedParent,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListCryptoKeyVersions(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.CryptoKeyVersions[0])
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

func TestKeyManagementServiceListCryptoKeyVersionsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]")
	var request = &kmspb.ListCryptoKeyVersionsRequest{
		Parent: formattedParent,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListCryptoKeyVersions(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceGetKeyRing(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &kmspb.KeyRing{
		Name: name2,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]")
	var request = &kmspb.GetKeyRingRequest{
		Name: formattedName,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetKeyRing(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceGetKeyRingError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]")
	var request = &kmspb.GetKeyRingRequest{
		Name: formattedName,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetKeyRing(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceGetCryptoKey(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &kmspb.CryptoKey{
		Name: name2,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]")
	var request = &kmspb.GetCryptoKeyRequest{
		Name: formattedName,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetCryptoKey(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceGetCryptoKeyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]")
	var request = &kmspb.GetCryptoKeyRequest{
		Name: formattedName,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetCryptoKey(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceGetCryptoKeyVersion(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &kmspb.CryptoKeyVersion{
		Name: name2,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s/cryptoKeyVersions/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]", "[CRYPTO_KEY_VERSION]")
	var request = &kmspb.GetCryptoKeyVersionRequest{
		Name: formattedName,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetCryptoKeyVersion(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceGetCryptoKeyVersionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s/cryptoKeyVersions/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]", "[CRYPTO_KEY_VERSION]")
	var request = &kmspb.GetCryptoKeyVersionRequest{
		Name: formattedName,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetCryptoKeyVersion(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceCreateKeyRing(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &kmspb.KeyRing{
		Name: name,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var keyRingId string = "keyRingId-2056646742"
	var keyRing *kmspb.KeyRing = &kmspb.KeyRing{}
	var request = &kmspb.CreateKeyRingRequest{
		Parent:    formattedParent,
		KeyRingId: keyRingId,
		KeyRing:   keyRing,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateKeyRing(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceCreateKeyRingError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s", "[PROJECT]", "[LOCATION]")
	var keyRingId string = "keyRingId-2056646742"
	var keyRing *kmspb.KeyRing = &kmspb.KeyRing{}
	var request = &kmspb.CreateKeyRingRequest{
		Parent:    formattedParent,
		KeyRingId: keyRingId,
		KeyRing:   keyRing,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateKeyRing(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceCreateCryptoKey(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &kmspb.CryptoKey{
		Name: name,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]")
	var cryptoKeyId string = "my-app-key"
	var purpose kmspb.CryptoKey_CryptoKeyPurpose = kmspb.CryptoKey_ENCRYPT_DECRYPT
	var seconds int64 = 2147483647
	var nextRotationTime = &timestamppb.Timestamp{
		Seconds: seconds,
	}
	var seconds2 int64 = 604800
	var rotationPeriod = &durationpb.Duration{
		Seconds: seconds2,
	}
	var cryptoKey = &kmspb.CryptoKey{
		Purpose:          purpose,
		NextRotationTime: nextRotationTime,
		RotationSchedule: &kmspb.CryptoKey_RotationPeriod{
			RotationPeriod: rotationPeriod,
		},
	}
	var request = &kmspb.CreateCryptoKeyRequest{
		Parent:      formattedParent,
		CryptoKeyId: cryptoKeyId,
		CryptoKey:   cryptoKey,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateCryptoKey(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceCreateCryptoKeyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]")
	var cryptoKeyId string = "my-app-key"
	var purpose kmspb.CryptoKey_CryptoKeyPurpose = kmspb.CryptoKey_ENCRYPT_DECRYPT
	var seconds int64 = 2147483647
	var nextRotationTime = &timestamppb.Timestamp{
		Seconds: seconds,
	}
	var seconds2 int64 = 604800
	var rotationPeriod = &durationpb.Duration{
		Seconds: seconds2,
	}
	var cryptoKey = &kmspb.CryptoKey{
		Purpose:          purpose,
		NextRotationTime: nextRotationTime,
		RotationSchedule: &kmspb.CryptoKey_RotationPeriod{
			RotationPeriod: rotationPeriod,
		},
	}
	var request = &kmspb.CreateCryptoKeyRequest{
		Parent:      formattedParent,
		CryptoKeyId: cryptoKeyId,
		CryptoKey:   cryptoKey,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateCryptoKey(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceCreateCryptoKeyVersion(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &kmspb.CryptoKeyVersion{
		Name: name,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]")
	var cryptoKeyVersion *kmspb.CryptoKeyVersion = &kmspb.CryptoKeyVersion{}
	var request = &kmspb.CreateCryptoKeyVersionRequest{
		Parent:           formattedParent,
		CryptoKeyVersion: cryptoKeyVersion,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateCryptoKeyVersion(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceCreateCryptoKeyVersionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]")
	var cryptoKeyVersion *kmspb.CryptoKeyVersion = &kmspb.CryptoKeyVersion{}
	var request = &kmspb.CreateCryptoKeyVersionRequest{
		Parent:           formattedParent,
		CryptoKeyVersion: cryptoKeyVersion,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateCryptoKeyVersion(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceUpdateCryptoKey(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &kmspb.CryptoKey{
		Name: name,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var cryptoKey *kmspb.CryptoKey = &kmspb.CryptoKey{}
	var updateMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &kmspb.UpdateCryptoKeyRequest{
		CryptoKey:  cryptoKey,
		UpdateMask: updateMask,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateCryptoKey(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceUpdateCryptoKeyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var cryptoKey *kmspb.CryptoKey = &kmspb.CryptoKey{}
	var updateMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &kmspb.UpdateCryptoKeyRequest{
		CryptoKey:  cryptoKey,
		UpdateMask: updateMask,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateCryptoKey(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceUpdateCryptoKeyVersion(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &kmspb.CryptoKeyVersion{
		Name: name,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var cryptoKeyVersion *kmspb.CryptoKeyVersion = &kmspb.CryptoKeyVersion{}
	var updateMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: cryptoKeyVersion,
		UpdateMask:       updateMask,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateCryptoKeyVersion(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceUpdateCryptoKeyVersionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var cryptoKeyVersion *kmspb.CryptoKeyVersion = &kmspb.CryptoKeyVersion{}
	var updateMask *field_maskpb.FieldMask = &field_maskpb.FieldMask{}
	var request = &kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: cryptoKeyVersion,
		UpdateMask:       updateMask,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateCryptoKeyVersion(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceEncrypt(t *testing.T) {
	var name2 string = "name2-1052831874"
	var ciphertext []byte = []byte("-72")
	var expectedResponse = &kmspb.EncryptResponse{
		Name:       name2,
		Ciphertext: ciphertext,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY_PATH]")
	var plaintext []byte = []byte("-9")
	var request = &kmspb.EncryptRequest{
		Name:      formattedName,
		Plaintext: plaintext,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Encrypt(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceEncryptError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY_PATH]")
	var plaintext []byte = []byte("-9")
	var request = &kmspb.EncryptRequest{
		Name:      formattedName,
		Plaintext: plaintext,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Encrypt(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceDecrypt(t *testing.T) {
	var plaintext []byte = []byte("-9")
	var expectedResponse = &kmspb.DecryptResponse{
		Plaintext: plaintext,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]")
	var ciphertext []byte = []byte("-72")
	var request = &kmspb.DecryptRequest{
		Name:       formattedName,
		Ciphertext: ciphertext,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Decrypt(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceDecryptError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]")
	var ciphertext []byte = []byte("-72")
	var request = &kmspb.DecryptRequest{
		Name:       formattedName,
		Ciphertext: ciphertext,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Decrypt(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceUpdateCryptoKeyPrimaryVersion(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &kmspb.CryptoKey{
		Name: name2,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]")
	var cryptoKeyVersionId string = "cryptoKeyVersionId729489152"
	var request = &kmspb.UpdateCryptoKeyPrimaryVersionRequest{
		Name:               formattedName,
		CryptoKeyVersionId: cryptoKeyVersionId,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateCryptoKeyPrimaryVersion(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceUpdateCryptoKeyPrimaryVersionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]")
	var cryptoKeyVersionId string = "cryptoKeyVersionId729489152"
	var request = &kmspb.UpdateCryptoKeyPrimaryVersionRequest{
		Name:               formattedName,
		CryptoKeyVersionId: cryptoKeyVersionId,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateCryptoKeyPrimaryVersion(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceDestroyCryptoKeyVersion(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &kmspb.CryptoKeyVersion{
		Name: name2,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s/cryptoKeyVersions/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]", "[CRYPTO_KEY_VERSION]")
	var request = &kmspb.DestroyCryptoKeyVersionRequest{
		Name: formattedName,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.DestroyCryptoKeyVersion(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceDestroyCryptoKeyVersionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s/cryptoKeyVersions/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]", "[CRYPTO_KEY_VERSION]")
	var request = &kmspb.DestroyCryptoKeyVersionRequest{
		Name: formattedName,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.DestroyCryptoKeyVersion(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestKeyManagementServiceRestoreCryptoKeyVersion(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &kmspb.CryptoKeyVersion{
		Name: name2,
	}

	mockKeyManagement.err = nil
	mockKeyManagement.reqs = nil

	mockKeyManagement.resps = append(mockKeyManagement.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s/cryptoKeyVersions/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]", "[CRYPTO_KEY_VERSION]")
	var request = &kmspb.RestoreCryptoKeyVersionRequest{
		Name: formattedName,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.RestoreCryptoKeyVersion(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockKeyManagement.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestKeyManagementServiceRestoreCryptoKeyVersionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockKeyManagement.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s/cryptoKeyVersions/%s", "[PROJECT]", "[LOCATION]", "[KEY_RING]", "[CRYPTO_KEY]", "[CRYPTO_KEY_VERSION]")
	var request = &kmspb.RestoreCryptoKeyVersionRequest{
		Name: formattedName,
	}

	c, err := NewKeyManagementClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.RestoreCryptoKeyVersion(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
