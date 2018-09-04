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

package speech

import (
	speechpb "google.golang.org/genproto/googleapis/cloud/speech/v1"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
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

type mockSpeechServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	speechpb.SpeechServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockSpeechServer) Recognize(ctx context.Context, req *speechpb.RecognizeRequest) (*speechpb.RecognizeResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*speechpb.RecognizeResponse), nil
}

func (s *mockSpeechServer) LongRunningRecognize(ctx context.Context, req *speechpb.LongRunningRecognizeRequest) (*longrunningpb.Operation, error) {
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

func (s *mockSpeechServer) StreamingRecognize(stream speechpb.Speech_StreamingRecognizeServer) error {
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
		if err := stream.Send(v.(*speechpb.StreamingRecognizeResponse)); err != nil {
			return err
		}
	}
	return nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockSpeech mockSpeechServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	speechpb.RegisterSpeechServer(serv, &mockSpeech)

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

func TestSpeechRecognize(t *testing.T) {
	var expectedResponse *speechpb.RecognizeResponse = &speechpb.RecognizeResponse{}

	mockSpeech.err = nil
	mockSpeech.reqs = nil

	mockSpeech.resps = append(mockSpeech.resps[:0], expectedResponse)

	var encoding speechpb.RecognitionConfig_AudioEncoding = speechpb.RecognitionConfig_FLAC
	var sampleRateHertz int32 = 44100
	var languageCode string = "en-US"
	var config = &speechpb.RecognitionConfig{
		Encoding:        encoding,
		SampleRateHertz: sampleRateHertz,
		LanguageCode:    languageCode,
	}
	var uri string = "gs://bucket_name/file_name.flac"
	var audio = &speechpb.RecognitionAudio{
		AudioSource: &speechpb.RecognitionAudio_Uri{
			Uri: uri,
		},
	}
	var request = &speechpb.RecognizeRequest{
		Config: config,
		Audio:  audio,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Recognize(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpeech.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpeechRecognizeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpeech.err = gstatus.Error(errCode, "test error")

	var encoding speechpb.RecognitionConfig_AudioEncoding = speechpb.RecognitionConfig_FLAC
	var sampleRateHertz int32 = 44100
	var languageCode string = "en-US"
	var config = &speechpb.RecognitionConfig{
		Encoding:        encoding,
		SampleRateHertz: sampleRateHertz,
		LanguageCode:    languageCode,
	}
	var uri string = "gs://bucket_name/file_name.flac"
	var audio = &speechpb.RecognitionAudio{
		AudioSource: &speechpb.RecognitionAudio_Uri{
			Uri: uri,
		},
	}
	var request = &speechpb.RecognizeRequest{
		Config: config,
		Audio:  audio,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.Recognize(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSpeechLongRunningRecognize(t *testing.T) {
	var expectedResponse *speechpb.LongRunningRecognizeResponse = &speechpb.LongRunningRecognizeResponse{}

	mockSpeech.err = nil
	mockSpeech.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockSpeech.resps = append(mockSpeech.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var encoding speechpb.RecognitionConfig_AudioEncoding = speechpb.RecognitionConfig_FLAC
	var sampleRateHertz int32 = 44100
	var languageCode string = "en-US"
	var config = &speechpb.RecognitionConfig{
		Encoding:        encoding,
		SampleRateHertz: sampleRateHertz,
		LanguageCode:    languageCode,
	}
	var uri string = "gs://bucket_name/file_name.flac"
	var audio = &speechpb.RecognitionAudio{
		AudioSource: &speechpb.RecognitionAudio_Uri{
			Uri: uri,
		},
	}
	var request = &speechpb.LongRunningRecognizeRequest{
		Config: config,
		Audio:  audio,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.LongRunningRecognize(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSpeech.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpeechLongRunningRecognizeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpeech.err = nil
	mockSpeech.resps = append(mockSpeech.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var encoding speechpb.RecognitionConfig_AudioEncoding = speechpb.RecognitionConfig_FLAC
	var sampleRateHertz int32 = 44100
	var languageCode string = "en-US"
	var config = &speechpb.RecognitionConfig{
		Encoding:        encoding,
		SampleRateHertz: sampleRateHertz,
		LanguageCode:    languageCode,
	}
	var uri string = "gs://bucket_name/file_name.flac"
	var audio = &speechpb.RecognitionAudio{
		AudioSource: &speechpb.RecognitionAudio_Uri{
			Uri: uri,
		},
	}
	var request = &speechpb.LongRunningRecognizeRequest{
		Config: config,
		Audio:  audio,
	}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.LongRunningRecognize(context.Background(), request)
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
func TestSpeechStreamingRecognize(t *testing.T) {
	var expectedResponse *speechpb.StreamingRecognizeResponse = &speechpb.StreamingRecognizeResponse{}

	mockSpeech.err = nil
	mockSpeech.reqs = nil

	mockSpeech.resps = append(mockSpeech.resps[:0], expectedResponse)

	var request *speechpb.StreamingRecognizeRequest = &speechpb.StreamingRecognizeRequest{}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.StreamingRecognize(context.Background())
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

	if want, got := request, mockSpeech.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSpeechStreamingRecognizeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSpeech.err = gstatus.Error(errCode, "test error")

	var request *speechpb.StreamingRecognizeRequest = &speechpb.StreamingRecognizeRequest{}

	c, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.StreamingRecognize(context.Background())
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
