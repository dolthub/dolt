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

package language

import (
	languagepb "google.golang.org/genproto/googleapis/cloud/language/v1beta2"
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

type mockLanguageServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	languagepb.LanguageServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockLanguageServer) AnalyzeSentiment(ctx context.Context, req *languagepb.AnalyzeSentimentRequest) (*languagepb.AnalyzeSentimentResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*languagepb.AnalyzeSentimentResponse), nil
}

func (s *mockLanguageServer) AnalyzeEntities(ctx context.Context, req *languagepb.AnalyzeEntitiesRequest) (*languagepb.AnalyzeEntitiesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*languagepb.AnalyzeEntitiesResponse), nil
}

func (s *mockLanguageServer) AnalyzeEntitySentiment(ctx context.Context, req *languagepb.AnalyzeEntitySentimentRequest) (*languagepb.AnalyzeEntitySentimentResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*languagepb.AnalyzeEntitySentimentResponse), nil
}

func (s *mockLanguageServer) AnalyzeSyntax(ctx context.Context, req *languagepb.AnalyzeSyntaxRequest) (*languagepb.AnalyzeSyntaxResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*languagepb.AnalyzeSyntaxResponse), nil
}

func (s *mockLanguageServer) ClassifyText(ctx context.Context, req *languagepb.ClassifyTextRequest) (*languagepb.ClassifyTextResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*languagepb.ClassifyTextResponse), nil
}

func (s *mockLanguageServer) AnnotateText(ctx context.Context, req *languagepb.AnnotateTextRequest) (*languagepb.AnnotateTextResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*languagepb.AnnotateTextResponse), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockLanguage mockLanguageServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	languagepb.RegisterLanguageServiceServer(serv, &mockLanguage)

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

func TestLanguageServiceAnalyzeSentiment(t *testing.T) {
	var language string = "language-1613589672"
	var expectedResponse = &languagepb.AnalyzeSentimentResponse{
		Language: language,
	}

	mockLanguage.err = nil
	mockLanguage.reqs = nil

	mockLanguage.resps = append(mockLanguage.resps[:0], expectedResponse)

	var document *languagepb.Document = &languagepb.Document{}
	var request = &languagepb.AnalyzeSentimentRequest{
		Document: document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.AnalyzeSentiment(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLanguage.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestLanguageServiceAnalyzeSentimentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLanguage.err = gstatus.Error(errCode, "test error")

	var document *languagepb.Document = &languagepb.Document{}
	var request = &languagepb.AnalyzeSentimentRequest{
		Document: document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.AnalyzeSentiment(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestLanguageServiceAnalyzeEntities(t *testing.T) {
	var language string = "language-1613589672"
	var expectedResponse = &languagepb.AnalyzeEntitiesResponse{
		Language: language,
	}

	mockLanguage.err = nil
	mockLanguage.reqs = nil

	mockLanguage.resps = append(mockLanguage.resps[:0], expectedResponse)

	var document *languagepb.Document = &languagepb.Document{}
	var request = &languagepb.AnalyzeEntitiesRequest{
		Document: document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.AnalyzeEntities(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLanguage.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestLanguageServiceAnalyzeEntitiesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLanguage.err = gstatus.Error(errCode, "test error")

	var document *languagepb.Document = &languagepb.Document{}
	var request = &languagepb.AnalyzeEntitiesRequest{
		Document: document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.AnalyzeEntities(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestLanguageServiceAnalyzeEntitySentiment(t *testing.T) {
	var language string = "language-1613589672"
	var expectedResponse = &languagepb.AnalyzeEntitySentimentResponse{
		Language: language,
	}

	mockLanguage.err = nil
	mockLanguage.reqs = nil

	mockLanguage.resps = append(mockLanguage.resps[:0], expectedResponse)

	var document *languagepb.Document = &languagepb.Document{}
	var request = &languagepb.AnalyzeEntitySentimentRequest{
		Document: document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.AnalyzeEntitySentiment(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLanguage.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestLanguageServiceAnalyzeEntitySentimentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLanguage.err = gstatus.Error(errCode, "test error")

	var document *languagepb.Document = &languagepb.Document{}
	var request = &languagepb.AnalyzeEntitySentimentRequest{
		Document: document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.AnalyzeEntitySentiment(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestLanguageServiceAnalyzeSyntax(t *testing.T) {
	var language string = "language-1613589672"
	var expectedResponse = &languagepb.AnalyzeSyntaxResponse{
		Language: language,
	}

	mockLanguage.err = nil
	mockLanguage.reqs = nil

	mockLanguage.resps = append(mockLanguage.resps[:0], expectedResponse)

	var document *languagepb.Document = &languagepb.Document{}
	var request = &languagepb.AnalyzeSyntaxRequest{
		Document: document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.AnalyzeSyntax(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLanguage.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestLanguageServiceAnalyzeSyntaxError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLanguage.err = gstatus.Error(errCode, "test error")

	var document *languagepb.Document = &languagepb.Document{}
	var request = &languagepb.AnalyzeSyntaxRequest{
		Document: document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.AnalyzeSyntax(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestLanguageServiceClassifyText(t *testing.T) {
	var expectedResponse *languagepb.ClassifyTextResponse = &languagepb.ClassifyTextResponse{}

	mockLanguage.err = nil
	mockLanguage.reqs = nil

	mockLanguage.resps = append(mockLanguage.resps[:0], expectedResponse)

	var document *languagepb.Document = &languagepb.Document{}
	var request = &languagepb.ClassifyTextRequest{
		Document: document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ClassifyText(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLanguage.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestLanguageServiceClassifyTextError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLanguage.err = gstatus.Error(errCode, "test error")

	var document *languagepb.Document = &languagepb.Document{}
	var request = &languagepb.ClassifyTextRequest{
		Document: document,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ClassifyText(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestLanguageServiceAnnotateText(t *testing.T) {
	var language string = "language-1613589672"
	var expectedResponse = &languagepb.AnnotateTextResponse{
		Language: language,
	}

	mockLanguage.err = nil
	mockLanguage.reqs = nil

	mockLanguage.resps = append(mockLanguage.resps[:0], expectedResponse)

	var document *languagepb.Document = &languagepb.Document{}
	var features *languagepb.AnnotateTextRequest_Features = &languagepb.AnnotateTextRequest_Features{}
	var request = &languagepb.AnnotateTextRequest{
		Document: document,
		Features: features,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.AnnotateText(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockLanguage.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestLanguageServiceAnnotateTextError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockLanguage.err = gstatus.Error(errCode, "test error")

	var document *languagepb.Document = &languagepb.Document{}
	var features *languagepb.AnnotateTextRequest_Features = &languagepb.AnnotateTextRequest_Features{}
	var request = &languagepb.AnnotateTextRequest{
		Document: document,
		Features: features,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.AnnotateText(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
