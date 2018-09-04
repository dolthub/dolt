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

package texttospeech

import (
	texttospeechpb "google.golang.org/genproto/googleapis/cloud/texttospeech/v1"
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

type mockTextToSpeechServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	texttospeechpb.TextToSpeechServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockTextToSpeechServer) ListVoices(ctx context.Context, req *texttospeechpb.ListVoicesRequest) (*texttospeechpb.ListVoicesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*texttospeechpb.ListVoicesResponse), nil
}

func (s *mockTextToSpeechServer) SynthesizeSpeech(ctx context.Context, req *texttospeechpb.SynthesizeSpeechRequest) (*texttospeechpb.SynthesizeSpeechResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*texttospeechpb.SynthesizeSpeechResponse), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockTextToSpeech mockTextToSpeechServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	texttospeechpb.RegisterTextToSpeechServer(serv, &mockTextToSpeech)

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

func TestTextToSpeechListVoices(t *testing.T) {
	var expectedResponse *texttospeechpb.ListVoicesResponse = &texttospeechpb.ListVoicesResponse{}

	mockTextToSpeech.err = nil
	mockTextToSpeech.reqs = nil

	mockTextToSpeech.resps = append(mockTextToSpeech.resps[:0], expectedResponse)

	var request *texttospeechpb.ListVoicesRequest = &texttospeechpb.ListVoicesRequest{}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListVoices(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockTextToSpeech.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestTextToSpeechListVoicesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockTextToSpeech.err = gstatus.Error(errCode, "test error")

	var request *texttospeechpb.ListVoicesRequest = &texttospeechpb.ListVoicesRequest{}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListVoices(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestTextToSpeechSynthesizeSpeech(t *testing.T) {
	var audioContent []byte = []byte("16")
	var expectedResponse = &texttospeechpb.SynthesizeSpeechResponse{
		AudioContent: audioContent,
	}

	mockTextToSpeech.err = nil
	mockTextToSpeech.reqs = nil

	mockTextToSpeech.resps = append(mockTextToSpeech.resps[:0], expectedResponse)

	var input *texttospeechpb.SynthesisInput = &texttospeechpb.SynthesisInput{}
	var voice *texttospeechpb.VoiceSelectionParams = &texttospeechpb.VoiceSelectionParams{}
	var audioConfig *texttospeechpb.AudioConfig = &texttospeechpb.AudioConfig{}
	var request = &texttospeechpb.SynthesizeSpeechRequest{
		Input:       input,
		Voice:       voice,
		AudioConfig: audioConfig,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SynthesizeSpeech(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockTextToSpeech.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestTextToSpeechSynthesizeSpeechError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockTextToSpeech.err = gstatus.Error(errCode, "test error")

	var input *texttospeechpb.SynthesisInput = &texttospeechpb.SynthesisInput{}
	var voice *texttospeechpb.VoiceSelectionParams = &texttospeechpb.VoiceSelectionParams{}
	var audioConfig *texttospeechpb.AudioConfig = &texttospeechpb.AudioConfig{}
	var request = &texttospeechpb.SynthesizeSpeechRequest{
		Input:       input,
		Voice:       voice,
		AudioConfig: audioConfig,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SynthesizeSpeech(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
