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

package firestore

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	firestorepb "google.golang.org/genproto/googleapis/firestore/v1beta1"
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

type mockFirestoreServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	firestorepb.FirestoreServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockFirestoreServer) GetDocument(ctx context.Context, req *firestorepb.GetDocumentRequest) (*firestorepb.Document, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*firestorepb.Document), nil
}

func (s *mockFirestoreServer) ListDocuments(ctx context.Context, req *firestorepb.ListDocumentsRequest) (*firestorepb.ListDocumentsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*firestorepb.ListDocumentsResponse), nil
}

func (s *mockFirestoreServer) CreateDocument(ctx context.Context, req *firestorepb.CreateDocumentRequest) (*firestorepb.Document, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*firestorepb.Document), nil
}

func (s *mockFirestoreServer) UpdateDocument(ctx context.Context, req *firestorepb.UpdateDocumentRequest) (*firestorepb.Document, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*firestorepb.Document), nil
}

func (s *mockFirestoreServer) DeleteDocument(ctx context.Context, req *firestorepb.DeleteDocumentRequest) (*emptypb.Empty, error) {
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

func (s *mockFirestoreServer) BatchGetDocuments(req *firestorepb.BatchGetDocumentsRequest, stream firestorepb.Firestore_BatchGetDocumentsServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return s.err
	}
	for _, v := range s.resps {
		if err := stream.Send(v.(*firestorepb.BatchGetDocumentsResponse)); err != nil {
			return err
		}
	}
	return nil
}

func (s *mockFirestoreServer) BeginTransaction(ctx context.Context, req *firestorepb.BeginTransactionRequest) (*firestorepb.BeginTransactionResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*firestorepb.BeginTransactionResponse), nil
}

func (s *mockFirestoreServer) Commit(ctx context.Context, req *firestorepb.CommitRequest) (*firestorepb.CommitResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*firestorepb.CommitResponse), nil
}

func (s *mockFirestoreServer) Rollback(ctx context.Context, req *firestorepb.RollbackRequest) (*emptypb.Empty, error) {
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

func (s *mockFirestoreServer) RunQuery(req *firestorepb.RunQueryRequest, stream firestorepb.Firestore_RunQueryServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return s.err
	}
	for _, v := range s.resps {
		if err := stream.Send(v.(*firestorepb.RunQueryResponse)); err != nil {
			return err
		}
	}
	return nil
}

func (s *mockFirestoreServer) Write(stream firestorepb.Firestore_WriteServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	for {
		if req, err := stream.Recv(); err == io.EOF {
			break
		} else if err != nil {
			return err
		} else {
			s.reqs = append(s.reqs, req)
		}
	}
	if s.err != nil {
		return s.err
	}
	for _, v := range s.resps {
		if err := stream.Send(v.(*firestorepb.WriteResponse)); err != nil {
			return err
		}
	}
	return nil
}

func (s *mockFirestoreServer) Listen(stream firestorepb.Firestore_ListenServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	for {
		if req, err := stream.Recv(); err == io.EOF {
			break
		} else if err != nil {
			return err
		} else {
			s.reqs = append(s.reqs, req)
		}
	}
	if s.err != nil {
		return s.err
	}
	for _, v := range s.resps {
		if err := stream.Send(v.(*firestorepb.ListenResponse)); err != nil {
			return err
		}
	}
	return nil
}

func (s *mockFirestoreServer) ListCollectionIds(ctx context.Context, req *firestorepb.ListCollectionIdsRequest) (*firestorepb.ListCollectionIdsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*firestorepb.ListCollectionIdsResponse), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockFirestore mockFirestoreServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	firestorepb.RegisterFirestoreServer(serv, &mockFirestore)

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

func TestFirestoreGetDocument(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &firestorepb.Document{
		Name: name2,
	}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var request = &firestorepb.GetDocumentRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetDocument(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestFirestoreGetDocumentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var request = &firestorepb.GetDocumentRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetDocument(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestFirestoreListDocuments(t *testing.T) {
	var nextPageToken string = ""
	var documentsElement *firestorepb.Document = &firestorepb.Document{}
	var documents = []*firestorepb.Document{documentsElement}
	var expectedResponse = &firestorepb.ListDocumentsResponse{
		NextPageToken: nextPageToken,
		Documents:     documents,
	}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var collectionId string = "collectionId-821242276"
	var request = &firestorepb.ListDocumentsRequest{
		Parent:       formattedParent,
		CollectionId: collectionId,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListDocuments(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Documents[0])
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

func TestFirestoreListDocumentsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var collectionId string = "collectionId-821242276"
	var request = &firestorepb.ListDocumentsRequest{
		Parent:       formattedParent,
		CollectionId: collectionId,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListDocuments(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestFirestoreCreateDocument(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &firestorepb.Document{
		Name: name,
	}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var collectionId string = "collectionId-821242276"
	var documentId string = "documentId506676927"
	var document *firestorepb.Document = &firestorepb.Document{}
	var request = &firestorepb.CreateDocumentRequest{
		Parent:       formattedParent,
		CollectionId: collectionId,
		DocumentId:   documentId,
		Document:     document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateDocument(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestFirestoreCreateDocumentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var collectionId string = "collectionId-821242276"
	var documentId string = "documentId506676927"
	var document *firestorepb.Document = &firestorepb.Document{}
	var request = &firestorepb.CreateDocumentRequest{
		Parent:       formattedParent,
		CollectionId: collectionId,
		DocumentId:   documentId,
		Document:     document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateDocument(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestFirestoreUpdateDocument(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &firestorepb.Document{
		Name: name,
	}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var document *firestorepb.Document = &firestorepb.Document{}
	var updateMask *firestorepb.DocumentMask = &firestorepb.DocumentMask{}
	var request = &firestorepb.UpdateDocumentRequest{
		Document:   document,
		UpdateMask: updateMask,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateDocument(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestFirestoreUpdateDocumentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var document *firestorepb.Document = &firestorepb.Document{}
	var updateMask *firestorepb.DocumentMask = &firestorepb.DocumentMask{}
	var request = &firestorepb.UpdateDocumentRequest{
		Document:   document,
		UpdateMask: updateMask,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateDocument(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestFirestoreDeleteDocument(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var request = &firestorepb.DeleteDocumentRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteDocument(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestFirestoreDeleteDocumentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var request = &firestorepb.DeleteDocumentRequest{
		Name: formattedName,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteDocument(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestFirestoreBatchGetDocuments(t *testing.T) {
	var missing string = "missing1069449574"
	var transaction []byte = []byte("-34")
	var expectedResponse = &firestorepb.BatchGetDocumentsResponse{
		Result: &firestorepb.BatchGetDocumentsResponse_Missing{
			Missing: missing,
		},
		Transaction: transaction,
	}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var documents []string = nil
	var request = &firestorepb.BatchGetDocumentsRequest{
		Database:  formattedDatabase,
		Documents: documents,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.BatchGetDocuments(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestFirestoreBatchGetDocumentsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var documents []string = nil
	var request = &firestorepb.BatchGetDocumentsRequest{
		Database:  formattedDatabase,
		Documents: documents,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.BatchGetDocuments(context.Background(), request)
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
func TestFirestoreBeginTransaction(t *testing.T) {
	var transaction []byte = []byte("-34")
	var expectedResponse = &firestorepb.BeginTransactionResponse{
		Transaction: transaction,
	}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var request = &firestorepb.BeginTransactionRequest{
		Database: formattedDatabase,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.BeginTransaction(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestFirestoreBeginTransactionError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var request = &firestorepb.BeginTransactionRequest{
		Database: formattedDatabase,
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
func TestFirestoreCommit(t *testing.T) {
	var expectedResponse *firestorepb.CommitResponse = &firestorepb.CommitResponse{}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var writes []*firestorepb.Write = nil
	var request = &firestorepb.CommitRequest{
		Database: formattedDatabase,
		Writes:   writes,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Commit(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestFirestoreCommitError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var writes []*firestorepb.Write = nil
	var request = &firestorepb.CommitRequest{
		Database: formattedDatabase,
		Writes:   writes,
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
func TestFirestoreRollback(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var transaction []byte = []byte("-34")
	var request = &firestorepb.RollbackRequest{
		Database:    formattedDatabase,
		Transaction: transaction,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.Rollback(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestFirestoreRollbackError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var transaction []byte = []byte("-34")
	var request = &firestorepb.RollbackRequest{
		Database:    formattedDatabase,
		Transaction: transaction,
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
func TestFirestoreRunQuery(t *testing.T) {
	var transaction []byte = []byte("-34")
	var skippedResults int32 = 880286183
	var expectedResponse = &firestorepb.RunQueryResponse{
		Transaction:    transaction,
		SkippedResults: skippedResults,
	}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var request = &firestorepb.RunQueryRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.RunQuery(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestFirestoreRunQueryError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var request = &firestorepb.RunQueryRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.RunQuery(context.Background(), request)
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
func TestFirestoreWrite(t *testing.T) {
	var streamId string = "streamId-315624902"
	var streamToken []byte = []byte("122")
	var expectedResponse = &firestorepb.WriteResponse{
		StreamId:    streamId,
		StreamToken: streamToken,
	}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var request = &firestorepb.WriteRequest{
		Database: formattedDatabase,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.Write(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestFirestoreWriteError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var request = &firestorepb.WriteRequest{
		Database: formattedDatabase,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.Write(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
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
func TestFirestoreListen(t *testing.T) {
	var expectedResponse *firestorepb.ListenResponse = &firestorepb.ListenResponse{}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var request = &firestorepb.ListenRequest{
		Database: formattedDatabase,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.Listen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestFirestoreListenError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedDatabase string = fmt.Sprintf("projects/%s/databases/%s", "[PROJECT]", "[DATABASE]")
	var request = &firestorepb.ListenRequest{
		Database: formattedDatabase,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.Listen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
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
func TestFirestoreListCollectionIds(t *testing.T) {
	var nextPageToken string = ""
	var collectionIdsElement string = "collectionIdsElement1368994900"
	var collectionIds = []string{collectionIdsElement}
	var expectedResponse = &firestorepb.ListCollectionIdsResponse{
		NextPageToken: nextPageToken,
		CollectionIds: collectionIds,
	}

	mockFirestore.err = nil
	mockFirestore.reqs = nil

	mockFirestore.resps = append(mockFirestore.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var request = &firestorepb.ListCollectionIdsRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListCollectionIds(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockFirestore.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.CollectionIds[0])
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

func TestFirestoreListCollectionIdsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockFirestore.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", "[PROJECT]", "[DATABASE]", "[DOCUMENT]", "[ANY_PATH]")
	var request = &firestorepb.ListCollectionIdsRequest{
		Parent: formattedParent,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListCollectionIds(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
