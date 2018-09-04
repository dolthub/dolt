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

package spanner

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
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

type mockSpannerServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	spannerpb.SpannerServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockSpannerServer) CreateSession(ctx context.Context, req *spannerpb.CreateSessionRequest) (*spannerpb.Session, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*spannerpb.Session), nil
}

func (s *mockSpannerServer) GetSession(ctx context.Context, req *spannerpb.GetSessionRequest) (*spannerpb.Session, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*spannerpb.Session), nil
}

func (s *mockSpannerServer) ListSessions(ctx context.Context, req *spannerpb.ListSessionsRequest) (*spannerpb.ListSessionsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*spannerpb.ListSessionsResponse), nil
}

func (s *mockSpannerServer) DeleteSession(ctx context.Context, req *spannerpb.DeleteSessionRequest) (*emptypb.Empty, error) {
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

func (s *mockSpannerServer) ExecuteSql(ctx context.Context, req *spannerpb.ExecuteSqlRequest) (*spannerpb.ResultSet, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*spannerpb.ResultSet), nil
}

func (s *mockSpannerServer) ExecuteStreamingSql(req *spannerpb.ExecuteSqlRequest, stream spannerpb.Spanner_ExecuteStreamingSqlServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return s.err
	}
	for _, v := range s.resps {
		if err := stream.Send(v.(*spannerpb.PartialResultSet)); err != nil {
			return err
		}
	}
	return nil
}

func (s *mockSpannerServer) Read(ctx context.Context, req *spannerpb.ReadRequest) (*spannerpb.ResultSet, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*spannerpb.ResultSet), nil
}

func (s *mockSpannerServer) StreamingRead(req *spannerpb.ReadRequest, stream spannerpb.Spanner_StreamingReadServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return s.err
	}
	for _, v := range s.resps {
		if err := stream.Send(v.(*spannerpb.PartialResultSet)); err != nil {
			return err
		}
	}
	return nil
}

func (s *mockSpannerServer) BeginTransaction(ctx context.Context, req *spannerpb.BeginTransactionRequest) (*spannerpb.Transaction, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*spannerpb.Transaction), nil
}

func (s *mockSpannerServer) Commit(ctx context.Context, req *spannerpb.CommitRequest) (*spannerpb.CommitResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*spannerpb.CommitResponse), nil
}

func (s *mockSpannerServer) Rollback(ctx context.Context, req *spannerpb.RollbackRequest) (*emptypb.Empty, error) {
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

func (s *mockSpannerServer) PartitionQuery(ctx context.Context, req *spannerpb.PartitionQueryRequest) (*spannerpb.PartitionResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*spannerpb.PartitionResponse), nil
}

func (s *mockSpannerServer) PartitionRead(ctx context.Context, req *spannerpb.PartitionReadRequest) (*spannerpb.PartitionResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*spannerpb.PartitionResponse), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockSpanner mockSpannerServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	spannerpb.RegisterSpannerServer(serv, &mockSpanner)

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

func TestSpannerCreateSession(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &spannerpb.Session{
		Name: name,
	}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedDatabase string = fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]")
	var request = &spannerpb.CreateSessionRequest{
		Database: formattedDatabase,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateSession(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpannerCreateSessionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedDatabase string = fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]")
	var request = &spannerpb.CreateSessionRequest{
		Database: formattedDatabase,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateSession(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpannerGetSession(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &spannerpb.Session{
		Name: name2,
	}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var request = &spannerpb.GetSessionRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetSession(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpannerGetSessionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var request = &spannerpb.GetSessionRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetSession(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpannerListSessions(t *testing.T) {
	var nextPageToken string = ""
	var sessionsElement *spannerpb.Session = &spannerpb.Session{}
	var sessions = []*spannerpb.Session{sessionsElement}
	var expectedResponse = &spannerpb.ListSessionsResponse{
		NextPageToken: nextPageToken,
		Sessions:      sessions,
	}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedDatabase string = fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]")
	var request = &spannerpb.ListSessionsRequest{
		Database: formattedDatabase,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListSessions(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Sessions[0])
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

func TestSpannerListSessionsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedDatabase string = fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]")
	var request = &spannerpb.ListSessionsRequest{
		Database: formattedDatabase,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListSessions(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpannerDeleteSession(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var request = &spannerpb.DeleteSessionRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteSession(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestSpannerDeleteSessionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var request = &spannerpb.DeleteSessionRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteSession(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestSpannerExecuteSql(t *testing.T) {
	var expectedResponse *spannerpb.ResultSet = &spannerpb.ResultSet{}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var sql string = "sql114126"
	var request = &spannerpb.ExecuteSqlRequest{
		Session: formattedSession,
		Sql:     sql,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ExecuteSql(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpannerExecuteSqlError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var sql string = "sql114126"
	var request = &spannerpb.ExecuteSqlRequest{
		Session: formattedSession,
		Sql:     sql,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ExecuteSql(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpannerExecuteStreamingSql(t *testing.T) {
	var chunkedValue bool = true
	var resumeToken []byte = []byte("103")
	var expectedResponse = &spannerpb.PartialResultSet{
		ChunkedValue: chunkedValue,
		ResumeToken:  resumeToken,
	}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var sql string = "sql114126"
	var request = &spannerpb.ExecuteSqlRequest{
		Session: formattedSession,
		Sql:     sql,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.ExecuteStreamingSql(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpannerExecuteStreamingSqlError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var sql string = "sql114126"
	var request = &spannerpb.ExecuteSqlRequest{
		Session: formattedSession,
		Sql:     sql,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.ExecuteStreamingSql(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpannerRead(t *testing.T) {
	var expectedResponse *spannerpb.ResultSet = &spannerpb.ResultSet{}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var table string = "table110115790"
	var columns []string = nil
	var keySet *spannerpb.KeySet = &spannerpb.KeySet{}
	var request = &spannerpb.ReadRequest{
		Session: formattedSession,
		Table:   table,
		Columns: columns,
		KeySet:  keySet,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Read(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpannerReadError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var table string = "table110115790"
	var columns []string = nil
	var keySet *spannerpb.KeySet = &spannerpb.KeySet{}
	var request = &spannerpb.ReadRequest{
		Session: formattedSession,
		Table:   table,
		Columns: columns,
		KeySet:  keySet,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Read(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpannerStreamingRead(t *testing.T) {
	var chunkedValue bool = true
	var resumeToken []byte = []byte("103")
	var expectedResponse = &spannerpb.PartialResultSet{
		ChunkedValue: chunkedValue,
		ResumeToken:  resumeToken,
	}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var table string = "table110115790"
	var columns []string = nil
	var keySet *spannerpb.KeySet = &spannerpb.KeySet{}
	var request = &spannerpb.ReadRequest{
		Session: formattedSession,
		Table:   table,
		Columns: columns,
		KeySet:  keySet,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.StreamingRead(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpannerStreamingReadError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var table string = "table110115790"
	var columns []string = nil
	var keySet *spannerpb.KeySet = &spannerpb.KeySet{}
	var request = &spannerpb.ReadRequest{
		Session: formattedSession,
		Table:   table,
		Columns: columns,
		KeySet:  keySet,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.StreamingRead(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpannerBeginTransaction(t *testing.T) {
	var id []byte = []byte("27")
	var expectedResponse = &spannerpb.Transaction{
		Id: id,
	}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var options *spannerpb.TransactionOptions = &spannerpb.TransactionOptions{}
	var request = &spannerpb.BeginTransactionRequest{
		Session: formattedSession,
		Options: options,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.BeginTransaction(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpannerBeginTransactionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var options *spannerpb.TransactionOptions = &spannerpb.TransactionOptions{}
	var request = &spannerpb.BeginTransactionRequest{
		Session: formattedSession,
		Options: options,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.BeginTransaction(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpannerCommit(t *testing.T) {
	var expectedResponse *spannerpb.CommitResponse = &spannerpb.CommitResponse{}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var mutations []*spannerpb.Mutation = nil
	var request = &spannerpb.CommitRequest{
		Session:   formattedSession,
		Mutations: mutations,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Commit(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpannerCommitError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var mutations []*spannerpb.Mutation = nil
	var request = &spannerpb.CommitRequest{
		Session:   formattedSession,
		Mutations: mutations,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Commit(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpannerRollback(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var transactionId []byte = []byte("28")
	var request = &spannerpb.RollbackRequest{
		Session:       formattedSession,
		TransactionId: transactionId,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.Rollback(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestSpannerRollbackError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var transactionId []byte = []byte("28")
	var request = &spannerpb.RollbackRequest{
		Session:       formattedSession,
		TransactionId: transactionId,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.Rollback(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestSpannerPartitionQuery(t *testing.T) {
	var expectedResponse *spannerpb.PartitionResponse = &spannerpb.PartitionResponse{}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var sql string = "sql114126"
	var request = &spannerpb.PartitionQueryRequest{
		Session: formattedSession,
		Sql:     sql,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.PartitionQuery(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpannerPartitionQueryError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var sql string = "sql114126"
	var request = &spannerpb.PartitionQueryRequest{
		Session: formattedSession,
		Sql:     sql,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.PartitionQuery(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpannerPartitionRead(t *testing.T) {
	var expectedResponse *spannerpb.PartitionResponse = &spannerpb.PartitionResponse{}

	mockSpanner.err = nil
	mockSpanner.reqs = nil

	mockSpanner.resps = append(mockSpanner.resps[:0], expectedResponse)

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var table string = "table110115790"
	var keySet *spannerpb.KeySet = &spannerpb.KeySet{}
	var request = &spannerpb.PartitionReadRequest{
		Session: formattedSession,
		Table:   table,
		KeySet:  keySet,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.PartitionRead(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpanner.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpannerPartitionReadError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpanner.err = gstatus.Error(errCode, "test error")

	var formattedSession string = fmt.Sprintf("projects/%s/instances/%s/databases/%s/sessions/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]", "[SESSION]")
	var table string = "table110115790"
	var keySet *spannerpb.KeySet = &spannerpb.KeySet{}
	var request = &spannerpb.PartitionReadRequest{
		Session: formattedSession,
		Table:   table,
		KeySet:  keySet,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.PartitionRead(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
