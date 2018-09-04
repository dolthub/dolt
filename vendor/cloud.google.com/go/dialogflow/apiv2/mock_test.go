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

package dialogflow

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	dialogflowpb "google.golang.org/genproto/googleapis/cloud/dialogflow/v2"
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

type mockAgentsServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	dialogflowpb.AgentsServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockAgentsServer) GetAgent(ctx context.Context, req *dialogflowpb.GetAgentRequest) (*dialogflowpb.Agent, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.Agent), nil
}

func (s *mockAgentsServer) SearchAgents(ctx context.Context, req *dialogflowpb.SearchAgentsRequest) (*dialogflowpb.SearchAgentsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.SearchAgentsResponse), nil
}

func (s *mockAgentsServer) TrainAgent(ctx context.Context, req *dialogflowpb.TrainAgentRequest) (*longrunningpb.Operation, error) {
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

func (s *mockAgentsServer) ExportAgent(ctx context.Context, req *dialogflowpb.ExportAgentRequest) (*longrunningpb.Operation, error) {
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

func (s *mockAgentsServer) ImportAgent(ctx context.Context, req *dialogflowpb.ImportAgentRequest) (*longrunningpb.Operation, error) {
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

func (s *mockAgentsServer) RestoreAgent(ctx context.Context, req *dialogflowpb.RestoreAgentRequest) (*longrunningpb.Operation, error) {
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

type mockContextsServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	dialogflowpb.ContextsServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockContextsServer) ListContexts(ctx context.Context, req *dialogflowpb.ListContextsRequest) (*dialogflowpb.ListContextsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.ListContextsResponse), nil
}

func (s *mockContextsServer) GetContext(ctx context.Context, req *dialogflowpb.GetContextRequest) (*dialogflowpb.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.Context), nil
}

func (s *mockContextsServer) CreateContext(ctx context.Context, req *dialogflowpb.CreateContextRequest) (*dialogflowpb.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.Context), nil
}

func (s *mockContextsServer) UpdateContext(ctx context.Context, req *dialogflowpb.UpdateContextRequest) (*dialogflowpb.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.Context), nil
}

func (s *mockContextsServer) DeleteContext(ctx context.Context, req *dialogflowpb.DeleteContextRequest) (*emptypb.Empty, error) {
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

func (s *mockContextsServer) DeleteAllContexts(ctx context.Context, req *dialogflowpb.DeleteAllContextsRequest) (*emptypb.Empty, error) {
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

type mockEntityTypesServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	dialogflowpb.EntityTypesServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockEntityTypesServer) ListEntityTypes(ctx context.Context, req *dialogflowpb.ListEntityTypesRequest) (*dialogflowpb.ListEntityTypesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.ListEntityTypesResponse), nil
}

func (s *mockEntityTypesServer) GetEntityType(ctx context.Context, req *dialogflowpb.GetEntityTypeRequest) (*dialogflowpb.EntityType, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.EntityType), nil
}

func (s *mockEntityTypesServer) CreateEntityType(ctx context.Context, req *dialogflowpb.CreateEntityTypeRequest) (*dialogflowpb.EntityType, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.EntityType), nil
}

func (s *mockEntityTypesServer) UpdateEntityType(ctx context.Context, req *dialogflowpb.UpdateEntityTypeRequest) (*dialogflowpb.EntityType, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.EntityType), nil
}

func (s *mockEntityTypesServer) DeleteEntityType(ctx context.Context, req *dialogflowpb.DeleteEntityTypeRequest) (*emptypb.Empty, error) {
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

func (s *mockEntityTypesServer) BatchUpdateEntityTypes(ctx context.Context, req *dialogflowpb.BatchUpdateEntityTypesRequest) (*longrunningpb.Operation, error) {
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

func (s *mockEntityTypesServer) BatchDeleteEntityTypes(ctx context.Context, req *dialogflowpb.BatchDeleteEntityTypesRequest) (*longrunningpb.Operation, error) {
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

func (s *mockEntityTypesServer) BatchCreateEntities(ctx context.Context, req *dialogflowpb.BatchCreateEntitiesRequest) (*longrunningpb.Operation, error) {
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

func (s *mockEntityTypesServer) BatchUpdateEntities(ctx context.Context, req *dialogflowpb.BatchUpdateEntitiesRequest) (*longrunningpb.Operation, error) {
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

func (s *mockEntityTypesServer) BatchDeleteEntities(ctx context.Context, req *dialogflowpb.BatchDeleteEntitiesRequest) (*longrunningpb.Operation, error) {
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

type mockIntentsServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	dialogflowpb.IntentsServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockIntentsServer) ListIntents(ctx context.Context, req *dialogflowpb.ListIntentsRequest) (*dialogflowpb.ListIntentsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.ListIntentsResponse), nil
}

func (s *mockIntentsServer) GetIntent(ctx context.Context, req *dialogflowpb.GetIntentRequest) (*dialogflowpb.Intent, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.Intent), nil
}

func (s *mockIntentsServer) CreateIntent(ctx context.Context, req *dialogflowpb.CreateIntentRequest) (*dialogflowpb.Intent, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.Intent), nil
}

func (s *mockIntentsServer) UpdateIntent(ctx context.Context, req *dialogflowpb.UpdateIntentRequest) (*dialogflowpb.Intent, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.Intent), nil
}

func (s *mockIntentsServer) DeleteIntent(ctx context.Context, req *dialogflowpb.DeleteIntentRequest) (*emptypb.Empty, error) {
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

func (s *mockIntentsServer) BatchUpdateIntents(ctx context.Context, req *dialogflowpb.BatchUpdateIntentsRequest) (*longrunningpb.Operation, error) {
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

func (s *mockIntentsServer) BatchDeleteIntents(ctx context.Context, req *dialogflowpb.BatchDeleteIntentsRequest) (*longrunningpb.Operation, error) {
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

type mockSessionEntityTypesServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	dialogflowpb.SessionEntityTypesServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockSessionEntityTypesServer) ListSessionEntityTypes(ctx context.Context, req *dialogflowpb.ListSessionEntityTypesRequest) (*dialogflowpb.ListSessionEntityTypesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.ListSessionEntityTypesResponse), nil
}

func (s *mockSessionEntityTypesServer) GetSessionEntityType(ctx context.Context, req *dialogflowpb.GetSessionEntityTypeRequest) (*dialogflowpb.SessionEntityType, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.SessionEntityType), nil
}

func (s *mockSessionEntityTypesServer) CreateSessionEntityType(ctx context.Context, req *dialogflowpb.CreateSessionEntityTypeRequest) (*dialogflowpb.SessionEntityType, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.SessionEntityType), nil
}

func (s *mockSessionEntityTypesServer) UpdateSessionEntityType(ctx context.Context, req *dialogflowpb.UpdateSessionEntityTypeRequest) (*dialogflowpb.SessionEntityType, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.SessionEntityType), nil
}

func (s *mockSessionEntityTypesServer) DeleteSessionEntityType(ctx context.Context, req *dialogflowpb.DeleteSessionEntityTypeRequest) (*emptypb.Empty, error) {
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

type mockSessionsServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	dialogflowpb.SessionsServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockSessionsServer) DetectIntent(ctx context.Context, req *dialogflowpb.DetectIntentRequest) (*dialogflowpb.DetectIntentResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*dialogflowpb.DetectIntentResponse), nil
}

func (s *mockSessionsServer) StreamingDetectIntent(stream dialogflowpb.Sessions_StreamingDetectIntentServer) error {
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
		if err := stream.Send(v.(*dialogflowpb.StreamingDetectIntentResponse)); err != nil {
			return err
		}
	}
	return nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockAgents             mockAgentsServer
	mockContexts           mockContextsServer
	mockEntityTypes        mockEntityTypesServer
	mockIntents            mockIntentsServer
	mockSessionEntityTypes mockSessionEntityTypesServer
	mockSessions           mockSessionsServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	dialogflowpb.RegisterAgentsServer(serv, &mockAgents)
	dialogflowpb.RegisterContextsServer(serv, &mockContexts)
	dialogflowpb.RegisterEntityTypesServer(serv, &mockEntityTypes)
	dialogflowpb.RegisterIntentsServer(serv, &mockIntents)
	dialogflowpb.RegisterSessionEntityTypesServer(serv, &mockSessionEntityTypes)
	dialogflowpb.RegisterSessionsServer(serv, &mockSessions)

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

func TestAgentsGetAgent(t *testing.T) {
	var parent2 string = "parent21175163357"
	var displayName string = "displayName1615086568"
	var defaultLanguageCode string = "defaultLanguageCode856575222"
	var timeZone string = "timeZone36848094"
	var description string = "description-1724546052"
	var avatarUri string = "avatarUri-402824826"
	var enableLogging bool = false
	var classificationThreshold float32 = 1.11581064E8
	var expectedResponse = &dialogflowpb.Agent{
		Parent:                  parent2,
		DisplayName:             displayName,
		DefaultLanguageCode:     defaultLanguageCode,
		TimeZone:                timeZone,
		Description:             description,
		AvatarUri:               avatarUri,
		EnableLogging:           enableLogging,
		ClassificationThreshold: classificationThreshold,
	}

	mockAgents.err = nil
	mockAgents.reqs = nil

	mockAgents.resps = append(mockAgents.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.GetAgentRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetAgent(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAgents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestAgentsGetAgentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAgents.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.GetAgentRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetAgent(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestAgentsSearchAgents(t *testing.T) {
	var nextPageToken string = ""
	var agentsElement *dialogflowpb.Agent = &dialogflowpb.Agent{}
	var agents = []*dialogflowpb.Agent{agentsElement}
	var expectedResponse = &dialogflowpb.SearchAgentsResponse{
		NextPageToken: nextPageToken,
		Agents:        agents,
	}

	mockAgents.err = nil
	mockAgents.reqs = nil

	mockAgents.resps = append(mockAgents.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.SearchAgentsRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SearchAgents(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAgents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Agents[0])
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

func TestAgentsSearchAgentsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAgents.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.SearchAgentsRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.SearchAgents(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestAgentsTrainAgent(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockAgents.err = nil
	mockAgents.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockAgents.resps = append(mockAgents.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.TrainAgentRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.TrainAgent(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAgents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestAgentsTrainAgentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAgents.err = nil
	mockAgents.resps = append(mockAgents.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.TrainAgentRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.TrainAgent(context.Background(), request)
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
func TestAgentsExportAgent(t *testing.T) {
	var agentUri string = "agentUri-1700713166"
	var expectedResponse = &dialogflowpb.ExportAgentResponse{
		Agent: &dialogflowpb.ExportAgentResponse_AgentUri{
			AgentUri: agentUri,
		},
	}

	mockAgents.err = nil
	mockAgents.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockAgents.resps = append(mockAgents.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.ExportAgentRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.ExportAgent(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAgents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestAgentsExportAgentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAgents.err = nil
	mockAgents.resps = append(mockAgents.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.ExportAgentRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.ExportAgent(context.Background(), request)
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
func TestAgentsImportAgent(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockAgents.err = nil
	mockAgents.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockAgents.resps = append(mockAgents.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.ImportAgentRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.ImportAgent(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAgents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestAgentsImportAgentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAgents.err = nil
	mockAgents.resps = append(mockAgents.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.ImportAgentRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.ImportAgent(context.Background(), request)
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
func TestAgentsRestoreAgent(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockAgents.err = nil
	mockAgents.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockAgents.resps = append(mockAgents.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.RestoreAgentRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.RestoreAgent(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAgents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestAgentsRestoreAgentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAgents.err = nil
	mockAgents.resps = append(mockAgents.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &dialogflowpb.RestoreAgentRequest{
		Parent: formattedParent,
	}

	c, err := NewAgentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.RestoreAgent(context.Background(), request)
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
func TestContextsListContexts(t *testing.T) {
	var nextPageToken string = ""
	var contextsElement *dialogflowpb.Context = &dialogflowpb.Context{}
	var contexts = []*dialogflowpb.Context{contextsElement}
	var expectedResponse = &dialogflowpb.ListContextsResponse{
		NextPageToken: nextPageToken,
		Contexts:      contexts,
	}

	mockContexts.err = nil
	mockContexts.reqs = nil

	mockContexts.resps = append(mockContexts.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var request = &dialogflowpb.ListContextsRequest{
		Parent: formattedParent,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListContexts(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContexts.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Contexts[0])
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

func TestContextsListContextsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContexts.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var request = &dialogflowpb.ListContextsRequest{
		Parent: formattedParent,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListContexts(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestContextsGetContext(t *testing.T) {
	var name2 string = "name2-1052831874"
	var lifespanCount int32 = 1178775510
	var expectedResponse = &dialogflowpb.Context{
		Name:          name2,
		LifespanCount: lifespanCount,
	}

	mockContexts.err = nil
	mockContexts.reqs = nil

	mockContexts.resps = append(mockContexts.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/agent/sessions/%s/contexts/%s", "[PROJECT]", "[SESSION]", "[CONTEXT]")
	var request = &dialogflowpb.GetContextRequest{
		Name: formattedName,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetContext(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContexts.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestContextsGetContextError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContexts.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/agent/sessions/%s/contexts/%s", "[PROJECT]", "[SESSION]", "[CONTEXT]")
	var request = &dialogflowpb.GetContextRequest{
		Name: formattedName,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetContext(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestContextsCreateContext(t *testing.T) {
	var name string = "name3373707"
	var lifespanCount int32 = 1178775510
	var expectedResponse = &dialogflowpb.Context{
		Name:          name,
		LifespanCount: lifespanCount,
	}

	mockContexts.err = nil
	mockContexts.reqs = nil

	mockContexts.resps = append(mockContexts.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var context_ *dialogflowpb.Context = &dialogflowpb.Context{}
	var request = &dialogflowpb.CreateContextRequest{
		Parent:  formattedParent,
		Context: context_,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateContext(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContexts.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestContextsCreateContextError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContexts.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var context_ *dialogflowpb.Context = &dialogflowpb.Context{}
	var request = &dialogflowpb.CreateContextRequest{
		Parent:  formattedParent,
		Context: context_,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateContext(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestContextsUpdateContext(t *testing.T) {
	var name string = "name3373707"
	var lifespanCount int32 = 1178775510
	var expectedResponse = &dialogflowpb.Context{
		Name:          name,
		LifespanCount: lifespanCount,
	}

	mockContexts.err = nil
	mockContexts.reqs = nil

	mockContexts.resps = append(mockContexts.resps[:0], expectedResponse)

	var context_ *dialogflowpb.Context = &dialogflowpb.Context{}
	var request = &dialogflowpb.UpdateContextRequest{
		Context: context_,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateContext(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContexts.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestContextsUpdateContextError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContexts.err = gstatus.Error(errCode, "test error")

	var context_ *dialogflowpb.Context = &dialogflowpb.Context{}
	var request = &dialogflowpb.UpdateContextRequest{
		Context: context_,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateContext(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestContextsDeleteContext(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockContexts.err = nil
	mockContexts.reqs = nil

	mockContexts.resps = append(mockContexts.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/agent/sessions/%s/contexts/%s", "[PROJECT]", "[SESSION]", "[CONTEXT]")
	var request = &dialogflowpb.DeleteContextRequest{
		Name: formattedName,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteContext(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContexts.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestContextsDeleteContextError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContexts.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/agent/sessions/%s/contexts/%s", "[PROJECT]", "[SESSION]", "[CONTEXT]")
	var request = &dialogflowpb.DeleteContextRequest{
		Name: formattedName,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteContext(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestContextsDeleteAllContexts(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockContexts.err = nil
	mockContexts.reqs = nil

	mockContexts.resps = append(mockContexts.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var request = &dialogflowpb.DeleteAllContextsRequest{
		Parent: formattedParent,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteAllContexts(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockContexts.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestContextsDeleteAllContextsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockContexts.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var request = &dialogflowpb.DeleteAllContextsRequest{
		Parent: formattedParent,
	}

	c, err := NewContextsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteAllContexts(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestEntityTypesListEntityTypes(t *testing.T) {
	var nextPageToken string = ""
	var entityTypesElement *dialogflowpb.EntityType = &dialogflowpb.EntityType{}
	var entityTypes = []*dialogflowpb.EntityType{entityTypesElement}
	var expectedResponse = &dialogflowpb.ListEntityTypesResponse{
		NextPageToken: nextPageToken,
		EntityTypes:   entityTypes,
	}

	mockEntityTypes.err = nil
	mockEntityTypes.reqs = nil

	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var request = &dialogflowpb.ListEntityTypesRequest{
		Parent: formattedParent,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListEntityTypes(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.EntityTypes[0])
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

func TestEntityTypesListEntityTypesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockEntityTypes.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var request = &dialogflowpb.ListEntityTypesRequest{
		Parent: formattedParent,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListEntityTypes(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestEntityTypesGetEntityType(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var expectedResponse = &dialogflowpb.EntityType{
		Name:        name2,
		DisplayName: displayName,
	}

	mockEntityTypes.err = nil
	mockEntityTypes.reqs = nil

	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/agent/entityTypes/%s", "[PROJECT]", "[ENTITY_TYPE]")
	var request = &dialogflowpb.GetEntityTypeRequest{
		Name: formattedName,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetEntityType(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestEntityTypesGetEntityTypeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockEntityTypes.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/agent/entityTypes/%s", "[PROJECT]", "[ENTITY_TYPE]")
	var request = &dialogflowpb.GetEntityTypeRequest{
		Name: formattedName,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetEntityType(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestEntityTypesCreateEntityType(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var expectedResponse = &dialogflowpb.EntityType{
		Name:        name,
		DisplayName: displayName,
	}

	mockEntityTypes.err = nil
	mockEntityTypes.reqs = nil

	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var entityType *dialogflowpb.EntityType = &dialogflowpb.EntityType{}
	var request = &dialogflowpb.CreateEntityTypeRequest{
		Parent:     formattedParent,
		EntityType: entityType,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateEntityType(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestEntityTypesCreateEntityTypeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockEntityTypes.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var entityType *dialogflowpb.EntityType = &dialogflowpb.EntityType{}
	var request = &dialogflowpb.CreateEntityTypeRequest{
		Parent:     formattedParent,
		EntityType: entityType,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateEntityType(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestEntityTypesUpdateEntityType(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var expectedResponse = &dialogflowpb.EntityType{
		Name:        name,
		DisplayName: displayName,
	}

	mockEntityTypes.err = nil
	mockEntityTypes.reqs = nil

	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], expectedResponse)

	var entityType *dialogflowpb.EntityType = &dialogflowpb.EntityType{}
	var request = &dialogflowpb.UpdateEntityTypeRequest{
		EntityType: entityType,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateEntityType(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestEntityTypesUpdateEntityTypeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockEntityTypes.err = gstatus.Error(errCode, "test error")

	var entityType *dialogflowpb.EntityType = &dialogflowpb.EntityType{}
	var request = &dialogflowpb.UpdateEntityTypeRequest{
		EntityType: entityType,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateEntityType(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestEntityTypesDeleteEntityType(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockEntityTypes.err = nil
	mockEntityTypes.reqs = nil

	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/agent/entityTypes/%s", "[PROJECT]", "[ENTITY_TYPE]")
	var request = &dialogflowpb.DeleteEntityTypeRequest{
		Name: formattedName,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteEntityType(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestEntityTypesDeleteEntityTypeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockEntityTypes.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/agent/entityTypes/%s", "[PROJECT]", "[ENTITY_TYPE]")
	var request = &dialogflowpb.DeleteEntityTypeRequest{
		Name: formattedName,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteEntityType(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestEntityTypesBatchUpdateEntityTypes(t *testing.T) {
	var expectedResponse *dialogflowpb.BatchUpdateEntityTypesResponse = &dialogflowpb.BatchUpdateEntityTypesResponse{}

	mockEntityTypes.err = nil
	mockEntityTypes.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var request = &dialogflowpb.BatchUpdateEntityTypesRequest{
		Parent: formattedParent,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchUpdateEntityTypes(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestEntityTypesBatchUpdateEntityTypesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockEntityTypes.err = nil
	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var request = &dialogflowpb.BatchUpdateEntityTypesRequest{
		Parent: formattedParent,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchUpdateEntityTypes(context.Background(), request)
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
func TestEntityTypesBatchDeleteEntityTypes(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockEntityTypes.err = nil
	mockEntityTypes.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var entityTypeNames []string = nil
	var request = &dialogflowpb.BatchDeleteEntityTypesRequest{
		Parent:          formattedParent,
		EntityTypeNames: entityTypeNames,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchDeleteEntityTypes(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestEntityTypesBatchDeleteEntityTypesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockEntityTypes.err = nil
	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var entityTypeNames []string = nil
	var request = &dialogflowpb.BatchDeleteEntityTypesRequest{
		Parent:          formattedParent,
		EntityTypeNames: entityTypeNames,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchDeleteEntityTypes(context.Background(), request)
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
func TestEntityTypesBatchCreateEntities(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockEntityTypes.err = nil
	mockEntityTypes.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent/entityTypes/%s", "[PROJECT]", "[ENTITY_TYPE]")
	var entities []*dialogflowpb.EntityType_Entity = nil
	var request = &dialogflowpb.BatchCreateEntitiesRequest{
		Parent:   formattedParent,
		Entities: entities,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchCreateEntities(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestEntityTypesBatchCreateEntitiesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockEntityTypes.err = nil
	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent/entityTypes/%s", "[PROJECT]", "[ENTITY_TYPE]")
	var entities []*dialogflowpb.EntityType_Entity = nil
	var request = &dialogflowpb.BatchCreateEntitiesRequest{
		Parent:   formattedParent,
		Entities: entities,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchCreateEntities(context.Background(), request)
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
func TestEntityTypesBatchUpdateEntities(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockEntityTypes.err = nil
	mockEntityTypes.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent/entityTypes/%s", "[PROJECT]", "[ENTITY_TYPE]")
	var entities []*dialogflowpb.EntityType_Entity = nil
	var request = &dialogflowpb.BatchUpdateEntitiesRequest{
		Parent:   formattedParent,
		Entities: entities,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchUpdateEntities(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestEntityTypesBatchUpdateEntitiesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockEntityTypes.err = nil
	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent/entityTypes/%s", "[PROJECT]", "[ENTITY_TYPE]")
	var entities []*dialogflowpb.EntityType_Entity = nil
	var request = &dialogflowpb.BatchUpdateEntitiesRequest{
		Parent:   formattedParent,
		Entities: entities,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchUpdateEntities(context.Background(), request)
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
func TestEntityTypesBatchDeleteEntities(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockEntityTypes.err = nil
	mockEntityTypes.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent/entityTypes/%s", "[PROJECT]", "[ENTITY_TYPE]")
	var entityValues []string = nil
	var request = &dialogflowpb.BatchDeleteEntitiesRequest{
		Parent:       formattedParent,
		EntityValues: entityValues,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchDeleteEntities(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestEntityTypesBatchDeleteEntitiesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockEntityTypes.err = nil
	mockEntityTypes.resps = append(mockEntityTypes.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent/entityTypes/%s", "[PROJECT]", "[ENTITY_TYPE]")
	var entityValues []string = nil
	var request = &dialogflowpb.BatchDeleteEntitiesRequest{
		Parent:       formattedParent,
		EntityValues: entityValues,
	}

	c, err := NewEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchDeleteEntities(context.Background(), request)
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
func TestIntentsListIntents(t *testing.T) {
	var nextPageToken string = ""
	var intentsElement *dialogflowpb.Intent = &dialogflowpb.Intent{}
	var intents = []*dialogflowpb.Intent{intentsElement}
	var expectedResponse = &dialogflowpb.ListIntentsResponse{
		NextPageToken: nextPageToken,
		Intents:       intents,
	}

	mockIntents.err = nil
	mockIntents.reqs = nil

	mockIntents.resps = append(mockIntents.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var request = &dialogflowpb.ListIntentsRequest{
		Parent: formattedParent,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListIntents(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockIntents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Intents[0])
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

func TestIntentsListIntentsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockIntents.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var request = &dialogflowpb.ListIntentsRequest{
		Parent: formattedParent,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListIntents(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestIntentsGetIntent(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var priority int32 = 1165461084
	var isFallback bool = false
	var mlDisabled bool = true
	var action string = "action-1422950858"
	var resetContexts bool = true
	var rootFollowupIntentName string = "rootFollowupIntentName402253784"
	var parentFollowupIntentName string = "parentFollowupIntentName-1131901680"
	var expectedResponse = &dialogflowpb.Intent{
		Name:                     name2,
		DisplayName:              displayName,
		Priority:                 priority,
		IsFallback:               isFallback,
		MlDisabled:               mlDisabled,
		Action:                   action,
		ResetContexts:            resetContexts,
		RootFollowupIntentName:   rootFollowupIntentName,
		ParentFollowupIntentName: parentFollowupIntentName,
	}

	mockIntents.err = nil
	mockIntents.reqs = nil

	mockIntents.resps = append(mockIntents.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/agent/intents/%s", "[PROJECT]", "[INTENT]")
	var request = &dialogflowpb.GetIntentRequest{
		Name: formattedName,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetIntent(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockIntents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestIntentsGetIntentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockIntents.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/agent/intents/%s", "[PROJECT]", "[INTENT]")
	var request = &dialogflowpb.GetIntentRequest{
		Name: formattedName,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetIntent(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestIntentsCreateIntent(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var priority int32 = 1165461084
	var isFallback bool = false
	var mlDisabled bool = true
	var action string = "action-1422950858"
	var resetContexts bool = true
	var rootFollowupIntentName string = "rootFollowupIntentName402253784"
	var parentFollowupIntentName string = "parentFollowupIntentName-1131901680"
	var expectedResponse = &dialogflowpb.Intent{
		Name:                     name,
		DisplayName:              displayName,
		Priority:                 priority,
		IsFallback:               isFallback,
		MlDisabled:               mlDisabled,
		Action:                   action,
		ResetContexts:            resetContexts,
		RootFollowupIntentName:   rootFollowupIntentName,
		ParentFollowupIntentName: parentFollowupIntentName,
	}

	mockIntents.err = nil
	mockIntents.reqs = nil

	mockIntents.resps = append(mockIntents.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var intent *dialogflowpb.Intent = &dialogflowpb.Intent{}
	var request = &dialogflowpb.CreateIntentRequest{
		Parent: formattedParent,
		Intent: intent,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateIntent(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockIntents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestIntentsCreateIntentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockIntents.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var intent *dialogflowpb.Intent = &dialogflowpb.Intent{}
	var request = &dialogflowpb.CreateIntentRequest{
		Parent: formattedParent,
		Intent: intent,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateIntent(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestIntentsUpdateIntent(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var priority int32 = 1165461084
	var isFallback bool = false
	var mlDisabled bool = true
	var action string = "action-1422950858"
	var resetContexts bool = true
	var rootFollowupIntentName string = "rootFollowupIntentName402253784"
	var parentFollowupIntentName string = "parentFollowupIntentName-1131901680"
	var expectedResponse = &dialogflowpb.Intent{
		Name:                     name,
		DisplayName:              displayName,
		Priority:                 priority,
		IsFallback:               isFallback,
		MlDisabled:               mlDisabled,
		Action:                   action,
		ResetContexts:            resetContexts,
		RootFollowupIntentName:   rootFollowupIntentName,
		ParentFollowupIntentName: parentFollowupIntentName,
	}

	mockIntents.err = nil
	mockIntents.reqs = nil

	mockIntents.resps = append(mockIntents.resps[:0], expectedResponse)

	var intent *dialogflowpb.Intent = &dialogflowpb.Intent{}
	var languageCode string = "languageCode-412800396"
	var request = &dialogflowpb.UpdateIntentRequest{
		Intent:       intent,
		LanguageCode: languageCode,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateIntent(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockIntents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestIntentsUpdateIntentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockIntents.err = gstatus.Error(errCode, "test error")

	var intent *dialogflowpb.Intent = &dialogflowpb.Intent{}
	var languageCode string = "languageCode-412800396"
	var request = &dialogflowpb.UpdateIntentRequest{
		Intent:       intent,
		LanguageCode: languageCode,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateIntent(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestIntentsDeleteIntent(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockIntents.err = nil
	mockIntents.reqs = nil

	mockIntents.resps = append(mockIntents.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/agent/intents/%s", "[PROJECT]", "[INTENT]")
	var request = &dialogflowpb.DeleteIntentRequest{
		Name: formattedName,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteIntent(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockIntents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestIntentsDeleteIntentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockIntents.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/agent/intents/%s", "[PROJECT]", "[INTENT]")
	var request = &dialogflowpb.DeleteIntentRequest{
		Name: formattedName,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteIntent(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestIntentsBatchUpdateIntents(t *testing.T) {
	var expectedResponse *dialogflowpb.BatchUpdateIntentsResponse = &dialogflowpb.BatchUpdateIntentsResponse{}

	mockIntents.err = nil
	mockIntents.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockIntents.resps = append(mockIntents.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var languageCode string = "languageCode-412800396"
	var request = &dialogflowpb.BatchUpdateIntentsRequest{
		Parent:       formattedParent,
		LanguageCode: languageCode,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchUpdateIntents(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockIntents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestIntentsBatchUpdateIntentsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockIntents.err = nil
	mockIntents.resps = append(mockIntents.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var languageCode string = "languageCode-412800396"
	var request = &dialogflowpb.BatchUpdateIntentsRequest{
		Parent:       formattedParent,
		LanguageCode: languageCode,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchUpdateIntents(context.Background(), request)
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
func TestIntentsBatchDeleteIntents(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockIntents.err = nil
	mockIntents.reqs = nil

	any, err := ptypes.MarshalAny(expectedResponse)
	if err != nil {
		t.Fatal(err)
	}
	mockIntents.resps = append(mockIntents.resps[:0], &longrunningpb.Operation{
		Name:   "longrunning-test",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var intents []*dialogflowpb.Intent = nil
	var request = &dialogflowpb.BatchDeleteIntentsRequest{
		Parent:  formattedParent,
		Intents: intents,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchDeleteIntents(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	err = respLRO.Wait(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockIntents.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestIntentsBatchDeleteIntentsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockIntents.err = nil
	mockIntents.resps = append(mockIntents.resps[:0], &longrunningpb.Operation{
		Name: "longrunning-test",
		Done: true,
		Result: &longrunningpb.Operation_Error{
			Error: &status.Status{
				Code:    int32(errCode),
				Message: "test error",
			},
		},
	})

	var formattedParent string = fmt.Sprintf("projects/%s/agent", "[PROJECT]")
	var intents []*dialogflowpb.Intent = nil
	var request = &dialogflowpb.BatchDeleteIntentsRequest{
		Parent:  formattedParent,
		Intents: intents,
	}

	c, err := NewIntentsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	respLRO, err := c.BatchDeleteIntents(context.Background(), request)
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
func TestSessionEntityTypesListSessionEntityTypes(t *testing.T) {
	var nextPageToken string = ""
	var sessionEntityTypesElement *dialogflowpb.SessionEntityType = &dialogflowpb.SessionEntityType{}
	var sessionEntityTypes = []*dialogflowpb.SessionEntityType{sessionEntityTypesElement}
	var expectedResponse = &dialogflowpb.ListSessionEntityTypesResponse{
		NextPageToken:      nextPageToken,
		SessionEntityTypes: sessionEntityTypes,
	}

	mockSessionEntityTypes.err = nil
	mockSessionEntityTypes.reqs = nil

	mockSessionEntityTypes.resps = append(mockSessionEntityTypes.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var request = &dialogflowpb.ListSessionEntityTypesRequest{
		Parent: formattedParent,
	}

	c, err := NewSessionEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListSessionEntityTypes(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSessionEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.SessionEntityTypes[0])
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

func TestSessionEntityTypesListSessionEntityTypesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSessionEntityTypes.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var request = &dialogflowpb.ListSessionEntityTypesRequest{
		Parent: formattedParent,
	}

	c, err := NewSessionEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListSessionEntityTypes(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSessionEntityTypesGetSessionEntityType(t *testing.T) {
	var name2 string = "name2-1052831874"
	var expectedResponse = &dialogflowpb.SessionEntityType{
		Name: name2,
	}

	mockSessionEntityTypes.err = nil
	mockSessionEntityTypes.reqs = nil

	mockSessionEntityTypes.resps = append(mockSessionEntityTypes.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/agent/sessions/%s/entityTypes/%s", "[PROJECT]", "[SESSION]", "[ENTITY_TYPE]")
	var request = &dialogflowpb.GetSessionEntityTypeRequest{
		Name: formattedName,
	}

	c, err := NewSessionEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetSessionEntityType(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSessionEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSessionEntityTypesGetSessionEntityTypeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSessionEntityTypes.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/agent/sessions/%s/entityTypes/%s", "[PROJECT]", "[SESSION]", "[ENTITY_TYPE]")
	var request = &dialogflowpb.GetSessionEntityTypeRequest{
		Name: formattedName,
	}

	c, err := NewSessionEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetSessionEntityType(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSessionEntityTypesCreateSessionEntityType(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &dialogflowpb.SessionEntityType{
		Name: name,
	}

	mockSessionEntityTypes.err = nil
	mockSessionEntityTypes.reqs = nil

	mockSessionEntityTypes.resps = append(mockSessionEntityTypes.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var sessionEntityType *dialogflowpb.SessionEntityType = &dialogflowpb.SessionEntityType{}
	var request = &dialogflowpb.CreateSessionEntityTypeRequest{
		Parent:            formattedParent,
		SessionEntityType: sessionEntityType,
	}

	c, err := NewSessionEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateSessionEntityType(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSessionEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSessionEntityTypesCreateSessionEntityTypeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSessionEntityTypes.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var sessionEntityType *dialogflowpb.SessionEntityType = &dialogflowpb.SessionEntityType{}
	var request = &dialogflowpb.CreateSessionEntityTypeRequest{
		Parent:            formattedParent,
		SessionEntityType: sessionEntityType,
	}

	c, err := NewSessionEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateSessionEntityType(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSessionEntityTypesUpdateSessionEntityType(t *testing.T) {
	var name string = "name3373707"
	var expectedResponse = &dialogflowpb.SessionEntityType{
		Name: name,
	}

	mockSessionEntityTypes.err = nil
	mockSessionEntityTypes.reqs = nil

	mockSessionEntityTypes.resps = append(mockSessionEntityTypes.resps[:0], expectedResponse)

	var sessionEntityType *dialogflowpb.SessionEntityType = &dialogflowpb.SessionEntityType{}
	var request = &dialogflowpb.UpdateSessionEntityTypeRequest{
		SessionEntityType: sessionEntityType,
	}

	c, err := NewSessionEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateSessionEntityType(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSessionEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSessionEntityTypesUpdateSessionEntityTypeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSessionEntityTypes.err = gstatus.Error(errCode, "test error")

	var sessionEntityType *dialogflowpb.SessionEntityType = &dialogflowpb.SessionEntityType{}
	var request = &dialogflowpb.UpdateSessionEntityTypeRequest{
		SessionEntityType: sessionEntityType,
	}

	c, err := NewSessionEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateSessionEntityType(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSessionEntityTypesDeleteSessionEntityType(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockSessionEntityTypes.err = nil
	mockSessionEntityTypes.reqs = nil

	mockSessionEntityTypes.resps = append(mockSessionEntityTypes.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/agent/sessions/%s/entityTypes/%s", "[PROJECT]", "[SESSION]", "[ENTITY_TYPE]")
	var request = &dialogflowpb.DeleteSessionEntityTypeRequest{
		Name: formattedName,
	}

	c, err := NewSessionEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteSessionEntityType(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSessionEntityTypes.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestSessionEntityTypesDeleteSessionEntityTypeError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSessionEntityTypes.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/agent/sessions/%s/entityTypes/%s", "[PROJECT]", "[SESSION]", "[ENTITY_TYPE]")
	var request = &dialogflowpb.DeleteSessionEntityTypeRequest{
		Name: formattedName,
	}

	c, err := NewSessionEntityTypesClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteSessionEntityType(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestSessionsDetectIntent(t *testing.T) {
	var responseId string = "responseId1847552473"
	var expectedResponse = &dialogflowpb.DetectIntentResponse{
		ResponseId: responseId,
	}

	mockSessions.err = nil
	mockSessions.reqs = nil

	mockSessions.resps = append(mockSessions.resps[:0], expectedResponse)

	var formattedSession string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var queryInput *dialogflowpb.QueryInput = &dialogflowpb.QueryInput{}
	var request = &dialogflowpb.DetectIntentRequest{
		Session:    formattedSession,
		QueryInput: queryInput,
	}

	c, err := NewSessionsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.DetectIntent(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockSessions.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSessionsDetectIntentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSessions.err = gstatus.Error(errCode, "test error")

	var formattedSession string = fmt.Sprintf("projects/%s/agent/sessions/%s", "[PROJECT]", "[SESSION]")
	var queryInput *dialogflowpb.QueryInput = &dialogflowpb.QueryInput{}
	var request = &dialogflowpb.DetectIntentRequest{
		Session:    formattedSession,
		QueryInput: queryInput,
	}

	c, err := NewSessionsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.DetectIntent(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestSessionsStreamingDetectIntent(t *testing.T) {
	var responseId string = "responseId1847552473"
	var expectedResponse = &dialogflowpb.StreamingDetectIntentResponse{
		ResponseId: responseId,
	}

	mockSessions.err = nil
	mockSessions.reqs = nil

	mockSessions.resps = append(mockSessions.resps[:0], expectedResponse)

	var session string = "session1984987798"
	var queryInput *dialogflowpb.QueryInput = &dialogflowpb.QueryInput{}
	var request = &dialogflowpb.StreamingDetectIntentRequest{
		Session:    session,
		QueryInput: queryInput,
	}

	c, err := NewSessionsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.StreamingDetectIntent(context.Background())
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

	if want, got := request, mockSessions.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestSessionsStreamingDetectIntentError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockSessions.err = gstatus.Error(errCode, "test error")

	var session string = "session1984987798"
	var queryInput *dialogflowpb.QueryInput = &dialogflowpb.QueryInput{}
	var request = &dialogflowpb.StreamingDetectIntentRequest{
		Session:    session,
		QueryInput: queryInput,
	}

	c, err := NewSessionsClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := c.StreamingDetectIntent(context.Background())
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
