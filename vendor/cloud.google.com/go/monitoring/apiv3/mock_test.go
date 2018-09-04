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

package monitoring

import (
	emptypb "github.com/golang/protobuf/ptypes/empty"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
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

type mockAlertPolicyServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	monitoringpb.AlertPolicyServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockAlertPolicyServer) ListAlertPolicies(ctx context.Context, req *monitoringpb.ListAlertPoliciesRequest) (*monitoringpb.ListAlertPoliciesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.ListAlertPoliciesResponse), nil
}

func (s *mockAlertPolicyServer) GetAlertPolicy(ctx context.Context, req *monitoringpb.GetAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.AlertPolicy), nil
}

func (s *mockAlertPolicyServer) CreateAlertPolicy(ctx context.Context, req *monitoringpb.CreateAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.AlertPolicy), nil
}

func (s *mockAlertPolicyServer) DeleteAlertPolicy(ctx context.Context, req *monitoringpb.DeleteAlertPolicyRequest) (*emptypb.Empty, error) {
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

func (s *mockAlertPolicyServer) UpdateAlertPolicy(ctx context.Context, req *monitoringpb.UpdateAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.AlertPolicy), nil
}

type mockGroupServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	monitoringpb.GroupServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockGroupServer) ListGroups(ctx context.Context, req *monitoringpb.ListGroupsRequest) (*monitoringpb.ListGroupsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.ListGroupsResponse), nil
}

func (s *mockGroupServer) GetGroup(ctx context.Context, req *monitoringpb.GetGroupRequest) (*monitoringpb.Group, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.Group), nil
}

func (s *mockGroupServer) CreateGroup(ctx context.Context, req *monitoringpb.CreateGroupRequest) (*monitoringpb.Group, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.Group), nil
}

func (s *mockGroupServer) UpdateGroup(ctx context.Context, req *monitoringpb.UpdateGroupRequest) (*monitoringpb.Group, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.Group), nil
}

func (s *mockGroupServer) DeleteGroup(ctx context.Context, req *monitoringpb.DeleteGroupRequest) (*emptypb.Empty, error) {
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

func (s *mockGroupServer) ListGroupMembers(ctx context.Context, req *monitoringpb.ListGroupMembersRequest) (*monitoringpb.ListGroupMembersResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.ListGroupMembersResponse), nil
}

type mockMetricServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	monitoringpb.MetricServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockMetricServer) ListMonitoredResourceDescriptors(ctx context.Context, req *monitoringpb.ListMonitoredResourceDescriptorsRequest) (*monitoringpb.ListMonitoredResourceDescriptorsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.ListMonitoredResourceDescriptorsResponse), nil
}

func (s *mockMetricServer) GetMonitoredResourceDescriptor(ctx context.Context, req *monitoringpb.GetMonitoredResourceDescriptorRequest) (*monitoredrespb.MonitoredResourceDescriptor, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoredrespb.MonitoredResourceDescriptor), nil
}

func (s *mockMetricServer) ListMetricDescriptors(ctx context.Context, req *monitoringpb.ListMetricDescriptorsRequest) (*monitoringpb.ListMetricDescriptorsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.ListMetricDescriptorsResponse), nil
}

func (s *mockMetricServer) GetMetricDescriptor(ctx context.Context, req *monitoringpb.GetMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*metricpb.MetricDescriptor), nil
}

func (s *mockMetricServer) CreateMetricDescriptor(ctx context.Context, req *monitoringpb.CreateMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*metricpb.MetricDescriptor), nil
}

func (s *mockMetricServer) DeleteMetricDescriptor(ctx context.Context, req *monitoringpb.DeleteMetricDescriptorRequest) (*emptypb.Empty, error) {
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

func (s *mockMetricServer) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.ListTimeSeriesResponse), nil
}

func (s *mockMetricServer) CreateTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
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

type mockNotificationChannelServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	monitoringpb.NotificationChannelServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockNotificationChannelServer) ListNotificationChannelDescriptors(ctx context.Context, req *monitoringpb.ListNotificationChannelDescriptorsRequest) (*monitoringpb.ListNotificationChannelDescriptorsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.ListNotificationChannelDescriptorsResponse), nil
}

func (s *mockNotificationChannelServer) GetNotificationChannelDescriptor(ctx context.Context, req *monitoringpb.GetNotificationChannelDescriptorRequest) (*monitoringpb.NotificationChannelDescriptor, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.NotificationChannelDescriptor), nil
}

func (s *mockNotificationChannelServer) ListNotificationChannels(ctx context.Context, req *monitoringpb.ListNotificationChannelsRequest) (*monitoringpb.ListNotificationChannelsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.ListNotificationChannelsResponse), nil
}

func (s *mockNotificationChannelServer) GetNotificationChannel(ctx context.Context, req *monitoringpb.GetNotificationChannelRequest) (*monitoringpb.NotificationChannel, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.NotificationChannel), nil
}

func (s *mockNotificationChannelServer) CreateNotificationChannel(ctx context.Context, req *monitoringpb.CreateNotificationChannelRequest) (*monitoringpb.NotificationChannel, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.NotificationChannel), nil
}

func (s *mockNotificationChannelServer) UpdateNotificationChannel(ctx context.Context, req *monitoringpb.UpdateNotificationChannelRequest) (*monitoringpb.NotificationChannel, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.NotificationChannel), nil
}

func (s *mockNotificationChannelServer) DeleteNotificationChannel(ctx context.Context, req *monitoringpb.DeleteNotificationChannelRequest) (*emptypb.Empty, error) {
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

type mockUptimeCheckServer struct {
	// Embed for forward compatibility.
	// Tests will keep working if more methods are added
	// in the future.
	monitoringpb.UptimeCheckServiceServer

	reqs []proto.Message

	// If set, all calls return this error.
	err error

	// responses to return if err == nil
	resps []proto.Message
}

func (s *mockUptimeCheckServer) ListUptimeCheckConfigs(ctx context.Context, req *monitoringpb.ListUptimeCheckConfigsRequest) (*monitoringpb.ListUptimeCheckConfigsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.ListUptimeCheckConfigsResponse), nil
}

func (s *mockUptimeCheckServer) GetUptimeCheckConfig(ctx context.Context, req *monitoringpb.GetUptimeCheckConfigRequest) (*monitoringpb.UptimeCheckConfig, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.UptimeCheckConfig), nil
}

func (s *mockUptimeCheckServer) CreateUptimeCheckConfig(ctx context.Context, req *monitoringpb.CreateUptimeCheckConfigRequest) (*monitoringpb.UptimeCheckConfig, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.UptimeCheckConfig), nil
}

func (s *mockUptimeCheckServer) UpdateUptimeCheckConfig(ctx context.Context, req *monitoringpb.UpdateUptimeCheckConfigRequest) (*monitoringpb.UptimeCheckConfig, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.UptimeCheckConfig), nil
}

func (s *mockUptimeCheckServer) DeleteUptimeCheckConfig(ctx context.Context, req *monitoringpb.DeleteUptimeCheckConfigRequest) (*emptypb.Empty, error) {
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

func (s *mockUptimeCheckServer) ListUptimeCheckIps(ctx context.Context, req *monitoringpb.ListUptimeCheckIpsRequest) (*monitoringpb.ListUptimeCheckIpsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*monitoringpb.ListUptimeCheckIpsResponse), nil
}

// clientOpt is the option tests should use to connect to the test server.
// It is initialized by TestMain.
var clientOpt option.ClientOption

var (
	mockAlertPolicy         mockAlertPolicyServer
	mockGroup               mockGroupServer
	mockMetric              mockMetricServer
	mockNotificationChannel mockNotificationChannelServer
	mockUptimeCheck         mockUptimeCheckServer
)

func TestMain(m *testing.M) {
	flag.Parse()

	serv := grpc.NewServer()
	monitoringpb.RegisterAlertPolicyServiceServer(serv, &mockAlertPolicy)
	monitoringpb.RegisterGroupServiceServer(serv, &mockGroup)
	monitoringpb.RegisterMetricServiceServer(serv, &mockMetric)
	monitoringpb.RegisterNotificationChannelServiceServer(serv, &mockNotificationChannel)
	monitoringpb.RegisterUptimeCheckServiceServer(serv, &mockUptimeCheck)

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

func TestAlertPolicyServiceListAlertPolicies(t *testing.T) {
	var nextPageToken string = ""
	var alertPoliciesElement *monitoringpb.AlertPolicy = &monitoringpb.AlertPolicy{}
	var alertPolicies = []*monitoringpb.AlertPolicy{alertPoliciesElement}
	var expectedResponse = &monitoringpb.ListAlertPoliciesResponse{
		NextPageToken: nextPageToken,
		AlertPolicies: alertPolicies,
	}

	mockAlertPolicy.err = nil
	mockAlertPolicy.reqs = nil

	mockAlertPolicy.resps = append(mockAlertPolicy.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListAlertPoliciesRequest{
		Name: formattedName,
	}

	c, err := NewAlertPolicyClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListAlertPolicies(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAlertPolicy.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.AlertPolicies[0])
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

func TestAlertPolicyServiceListAlertPoliciesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAlertPolicy.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListAlertPoliciesRequest{
		Name: formattedName,
	}

	c, err := NewAlertPolicyClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListAlertPolicies(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestAlertPolicyServiceGetAlertPolicy(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var expectedResponse = &monitoringpb.AlertPolicy{
		Name:        name2,
		DisplayName: displayName,
	}

	mockAlertPolicy.err = nil
	mockAlertPolicy.reqs = nil

	mockAlertPolicy.resps = append(mockAlertPolicy.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/alertPolicies/%s", "[PROJECT]", "[ALERT_POLICY]")
	var request = &monitoringpb.GetAlertPolicyRequest{
		Name: formattedName,
	}

	c, err := NewAlertPolicyClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetAlertPolicy(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAlertPolicy.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestAlertPolicyServiceGetAlertPolicyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAlertPolicy.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/alertPolicies/%s", "[PROJECT]", "[ALERT_POLICY]")
	var request = &monitoringpb.GetAlertPolicyRequest{
		Name: formattedName,
	}

	c, err := NewAlertPolicyClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetAlertPolicy(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestAlertPolicyServiceCreateAlertPolicy(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var expectedResponse = &monitoringpb.AlertPolicy{
		Name:        name2,
		DisplayName: displayName,
	}

	mockAlertPolicy.err = nil
	mockAlertPolicy.reqs = nil

	mockAlertPolicy.resps = append(mockAlertPolicy.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var alertPolicy *monitoringpb.AlertPolicy = &monitoringpb.AlertPolicy{}
	var request = &monitoringpb.CreateAlertPolicyRequest{
		Name:        formattedName,
		AlertPolicy: alertPolicy,
	}

	c, err := NewAlertPolicyClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateAlertPolicy(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAlertPolicy.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestAlertPolicyServiceCreateAlertPolicyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAlertPolicy.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var alertPolicy *monitoringpb.AlertPolicy = &monitoringpb.AlertPolicy{}
	var request = &monitoringpb.CreateAlertPolicyRequest{
		Name:        formattedName,
		AlertPolicy: alertPolicy,
	}

	c, err := NewAlertPolicyClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateAlertPolicy(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestAlertPolicyServiceDeleteAlertPolicy(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockAlertPolicy.err = nil
	mockAlertPolicy.reqs = nil

	mockAlertPolicy.resps = append(mockAlertPolicy.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/alertPolicies/%s", "[PROJECT]", "[ALERT_POLICY]")
	var request = &monitoringpb.DeleteAlertPolicyRequest{
		Name: formattedName,
	}

	c, err := NewAlertPolicyClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteAlertPolicy(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAlertPolicy.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestAlertPolicyServiceDeleteAlertPolicyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAlertPolicy.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/alertPolicies/%s", "[PROJECT]", "[ALERT_POLICY]")
	var request = &monitoringpb.DeleteAlertPolicyRequest{
		Name: formattedName,
	}

	c, err := NewAlertPolicyClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteAlertPolicy(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestAlertPolicyServiceUpdateAlertPolicy(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var expectedResponse = &monitoringpb.AlertPolicy{
		Name:        name,
		DisplayName: displayName,
	}

	mockAlertPolicy.err = nil
	mockAlertPolicy.reqs = nil

	mockAlertPolicy.resps = append(mockAlertPolicy.resps[:0], expectedResponse)

	var alertPolicy *monitoringpb.AlertPolicy = &monitoringpb.AlertPolicy{}
	var request = &monitoringpb.UpdateAlertPolicyRequest{
		AlertPolicy: alertPolicy,
	}

	c, err := NewAlertPolicyClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateAlertPolicy(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockAlertPolicy.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestAlertPolicyServiceUpdateAlertPolicyError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockAlertPolicy.err = gstatus.Error(errCode, "test error")

	var alertPolicy *monitoringpb.AlertPolicy = &monitoringpb.AlertPolicy{}
	var request = &monitoringpb.UpdateAlertPolicyRequest{
		AlertPolicy: alertPolicy,
	}

	c, err := NewAlertPolicyClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateAlertPolicy(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGroupServiceListGroups(t *testing.T) {
	var nextPageToken string = ""
	var groupElement *monitoringpb.Group = &monitoringpb.Group{}
	var group = []*monitoringpb.Group{groupElement}
	var expectedResponse = &monitoringpb.ListGroupsResponse{
		NextPageToken: nextPageToken,
		Group:         group,
	}

	mockGroup.err = nil
	mockGroup.reqs = nil

	mockGroup.resps = append(mockGroup.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListGroupsRequest{
		Name: formattedName,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListGroups(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGroup.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Group[0])
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

func TestGroupServiceListGroupsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGroup.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListGroupsRequest{
		Name: formattedName,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListGroups(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGroupServiceGetGroup(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var parentName string = "parentName1015022848"
	var filter string = "filter-1274492040"
	var isCluster bool = false
	var expectedResponse = &monitoringpb.Group{
		Name:        name2,
		DisplayName: displayName,
		ParentName:  parentName,
		Filter:      filter,
		IsCluster:   isCluster,
	}

	mockGroup.err = nil
	mockGroup.reqs = nil

	mockGroup.resps = append(mockGroup.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/groups/%s", "[PROJECT]", "[GROUP]")
	var request = &monitoringpb.GetGroupRequest{
		Name: formattedName,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetGroup(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGroup.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGroupServiceGetGroupError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGroup.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/groups/%s", "[PROJECT]", "[GROUP]")
	var request = &monitoringpb.GetGroupRequest{
		Name: formattedName,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetGroup(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGroupServiceCreateGroup(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var parentName string = "parentName1015022848"
	var filter string = "filter-1274492040"
	var isCluster bool = false
	var expectedResponse = &monitoringpb.Group{
		Name:        name2,
		DisplayName: displayName,
		ParentName:  parentName,
		Filter:      filter,
		IsCluster:   isCluster,
	}

	mockGroup.err = nil
	mockGroup.reqs = nil

	mockGroup.resps = append(mockGroup.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var group *monitoringpb.Group = &monitoringpb.Group{}
	var request = &monitoringpb.CreateGroupRequest{
		Name:  formattedName,
		Group: group,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateGroup(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGroup.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGroupServiceCreateGroupError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGroup.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var group *monitoringpb.Group = &monitoringpb.Group{}
	var request = &monitoringpb.CreateGroupRequest{
		Name:  formattedName,
		Group: group,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateGroup(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGroupServiceUpdateGroup(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var parentName string = "parentName1015022848"
	var filter string = "filter-1274492040"
	var isCluster bool = false
	var expectedResponse = &monitoringpb.Group{
		Name:        name,
		DisplayName: displayName,
		ParentName:  parentName,
		Filter:      filter,
		IsCluster:   isCluster,
	}

	mockGroup.err = nil
	mockGroup.reqs = nil

	mockGroup.resps = append(mockGroup.resps[:0], expectedResponse)

	var group *monitoringpb.Group = &monitoringpb.Group{}
	var request = &monitoringpb.UpdateGroupRequest{
		Group: group,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateGroup(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGroup.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestGroupServiceUpdateGroupError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGroup.err = gstatus.Error(errCode, "test error")

	var group *monitoringpb.Group = &monitoringpb.Group{}
	var request = &monitoringpb.UpdateGroupRequest{
		Group: group,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateGroup(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestGroupServiceDeleteGroup(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockGroup.err = nil
	mockGroup.reqs = nil

	mockGroup.resps = append(mockGroup.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/groups/%s", "[PROJECT]", "[GROUP]")
	var request = &monitoringpb.DeleteGroupRequest{
		Name: formattedName,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteGroup(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGroup.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestGroupServiceDeleteGroupError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGroup.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/groups/%s", "[PROJECT]", "[GROUP]")
	var request = &monitoringpb.DeleteGroupRequest{
		Name: formattedName,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteGroup(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestGroupServiceListGroupMembers(t *testing.T) {
	var nextPageToken string = ""
	var totalSize int32 = 705419236
	var membersElement *monitoredrespb.MonitoredResource = &monitoredrespb.MonitoredResource{}
	var members = []*monitoredrespb.MonitoredResource{membersElement}
	var expectedResponse = &monitoringpb.ListGroupMembersResponse{
		NextPageToken: nextPageToken,
		TotalSize:     totalSize,
		Members:       members,
	}

	mockGroup.err = nil
	mockGroup.reqs = nil

	mockGroup.resps = append(mockGroup.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/groups/%s", "[PROJECT]", "[GROUP]")
	var request = &monitoringpb.ListGroupMembersRequest{
		Name: formattedName,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListGroupMembers(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockGroup.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.Members[0])
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

func TestGroupServiceListGroupMembersError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockGroup.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/groups/%s", "[PROJECT]", "[GROUP]")
	var request = &monitoringpb.ListGroupMembersRequest{
		Name: formattedName,
	}

	c, err := NewGroupClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListGroupMembers(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricServiceListMonitoredResourceDescriptors(t *testing.T) {
	var nextPageToken string = ""
	var resourceDescriptorsElement *monitoredrespb.MonitoredResourceDescriptor = &monitoredrespb.MonitoredResourceDescriptor{}
	var resourceDescriptors = []*monitoredrespb.MonitoredResourceDescriptor{resourceDescriptorsElement}
	var expectedResponse = &monitoringpb.ListMonitoredResourceDescriptorsResponse{
		NextPageToken:       nextPageToken,
		ResourceDescriptors: resourceDescriptors,
	}

	mockMetric.err = nil
	mockMetric.reqs = nil

	mockMetric.resps = append(mockMetric.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListMonitoredResourceDescriptorsRequest{
		Name: formattedName,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListMonitoredResourceDescriptors(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetric.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.ResourceDescriptors[0])
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

func TestMetricServiceListMonitoredResourceDescriptorsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetric.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListMonitoredResourceDescriptorsRequest{
		Name: formattedName,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListMonitoredResourceDescriptors(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricServiceGetMonitoredResourceDescriptor(t *testing.T) {
	var name2 string = "name2-1052831874"
	var type_ string = "type3575610"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &monitoredrespb.MonitoredResourceDescriptor{
		Name:        name2,
		Type:        type_,
		DisplayName: displayName,
		Description: description,
	}

	mockMetric.err = nil
	mockMetric.reqs = nil

	mockMetric.resps = append(mockMetric.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/monitoredResourceDescriptors/%s", "[PROJECT]", "[MONITORED_RESOURCE_DESCRIPTOR]")
	var request = &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: formattedName,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetMonitoredResourceDescriptor(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetric.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestMetricServiceGetMonitoredResourceDescriptorError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetric.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/monitoredResourceDescriptors/%s", "[PROJECT]", "[MONITORED_RESOURCE_DESCRIPTOR]")
	var request = &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: formattedName,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetMonitoredResourceDescriptor(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricServiceListMetricDescriptors(t *testing.T) {
	var nextPageToken string = ""
	var metricDescriptorsElement *metricpb.MetricDescriptor = &metricpb.MetricDescriptor{}
	var metricDescriptors = []*metricpb.MetricDescriptor{metricDescriptorsElement}
	var expectedResponse = &monitoringpb.ListMetricDescriptorsResponse{
		NextPageToken:     nextPageToken,
		MetricDescriptors: metricDescriptors,
	}

	mockMetric.err = nil
	mockMetric.reqs = nil

	mockMetric.resps = append(mockMetric.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListMetricDescriptorsRequest{
		Name: formattedName,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListMetricDescriptors(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetric.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.MetricDescriptors[0])
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

func TestMetricServiceListMetricDescriptorsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetric.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListMetricDescriptorsRequest{
		Name: formattedName,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListMetricDescriptors(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricServiceGetMetricDescriptor(t *testing.T) {
	var name2 string = "name2-1052831874"
	var type_ string = "type3575610"
	var unit string = "unit3594628"
	var description string = "description-1724546052"
	var displayName string = "displayName1615086568"
	var expectedResponse = &metricpb.MetricDescriptor{
		Name:        name2,
		Type:        type_,
		Unit:        unit,
		Description: description,
		DisplayName: displayName,
	}

	mockMetric.err = nil
	mockMetric.reqs = nil

	mockMetric.resps = append(mockMetric.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/metricDescriptors/%s", "[PROJECT]", "[METRIC_DESCRIPTOR]")
	var request = &monitoringpb.GetMetricDescriptorRequest{
		Name: formattedName,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetMetricDescriptor(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetric.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestMetricServiceGetMetricDescriptorError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetric.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/metricDescriptors/%s", "[PROJECT]", "[METRIC_DESCRIPTOR]")
	var request = &monitoringpb.GetMetricDescriptorRequest{
		Name: formattedName,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetMetricDescriptor(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricServiceCreateMetricDescriptor(t *testing.T) {
	var name2 string = "name2-1052831874"
	var type_ string = "type3575610"
	var unit string = "unit3594628"
	var description string = "description-1724546052"
	var displayName string = "displayName1615086568"
	var expectedResponse = &metricpb.MetricDescriptor{
		Name:        name2,
		Type:        type_,
		Unit:        unit,
		Description: description,
		DisplayName: displayName,
	}

	mockMetric.err = nil
	mockMetric.reqs = nil

	mockMetric.resps = append(mockMetric.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var metricDescriptor *metricpb.MetricDescriptor = &metricpb.MetricDescriptor{}
	var request = &monitoringpb.CreateMetricDescriptorRequest{
		Name:             formattedName,
		MetricDescriptor: metricDescriptor,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateMetricDescriptor(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetric.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestMetricServiceCreateMetricDescriptorError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetric.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var metricDescriptor *metricpb.MetricDescriptor = &metricpb.MetricDescriptor{}
	var request = &monitoringpb.CreateMetricDescriptorRequest{
		Name:             formattedName,
		MetricDescriptor: metricDescriptor,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateMetricDescriptor(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricServiceDeleteMetricDescriptor(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockMetric.err = nil
	mockMetric.reqs = nil

	mockMetric.resps = append(mockMetric.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/metricDescriptors/%s", "[PROJECT]", "[METRIC_DESCRIPTOR]")
	var request = &monitoringpb.DeleteMetricDescriptorRequest{
		Name: formattedName,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteMetricDescriptor(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetric.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestMetricServiceDeleteMetricDescriptorError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetric.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/metricDescriptors/%s", "[PROJECT]", "[METRIC_DESCRIPTOR]")
	var request = &monitoringpb.DeleteMetricDescriptorRequest{
		Name: formattedName,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteMetricDescriptor(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestMetricServiceListTimeSeries(t *testing.T) {
	var nextPageToken string = ""
	var timeSeriesElement *monitoringpb.TimeSeries = &monitoringpb.TimeSeries{}
	var timeSeries = []*monitoringpb.TimeSeries{timeSeriesElement}
	var expectedResponse = &monitoringpb.ListTimeSeriesResponse{
		NextPageToken: nextPageToken,
		TimeSeries:    timeSeries,
	}

	mockMetric.err = nil
	mockMetric.reqs = nil

	mockMetric.resps = append(mockMetric.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var filter string = "filter-1274492040"
	var interval *monitoringpb.TimeInterval = &monitoringpb.TimeInterval{}
	var view monitoringpb.ListTimeSeriesRequest_TimeSeriesView = monitoringpb.ListTimeSeriesRequest_FULL
	var request = &monitoringpb.ListTimeSeriesRequest{
		Name:     formattedName,
		Filter:   filter,
		Interval: interval,
		View:     view,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListTimeSeries(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetric.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.TimeSeries[0])
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

func TestMetricServiceListTimeSeriesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetric.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var filter string = "filter-1274492040"
	var interval *monitoringpb.TimeInterval = &monitoringpb.TimeInterval{}
	var view monitoringpb.ListTimeSeriesRequest_TimeSeriesView = monitoringpb.ListTimeSeriesRequest_FULL
	var request = &monitoringpb.ListTimeSeriesRequest{
		Name:     formattedName,
		Filter:   filter,
		Interval: interval,
		View:     view,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListTimeSeries(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestMetricServiceCreateTimeSeries(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockMetric.err = nil
	mockMetric.reqs = nil

	mockMetric.resps = append(mockMetric.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var timeSeries []*monitoringpb.TimeSeries = nil
	var request = &monitoringpb.CreateTimeSeriesRequest{
		Name:       formattedName,
		TimeSeries: timeSeries,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.CreateTimeSeries(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockMetric.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestMetricServiceCreateTimeSeriesError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockMetric.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var timeSeries []*monitoringpb.TimeSeries = nil
	var request = &monitoringpb.CreateTimeSeriesRequest{
		Name:       formattedName,
		TimeSeries: timeSeries,
	}

	c, err := NewMetricClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.CreateTimeSeries(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestNotificationChannelServiceListNotificationChannelDescriptors(t *testing.T) {
	var nextPageToken string = ""
	var channelDescriptorsElement *monitoringpb.NotificationChannelDescriptor = &monitoringpb.NotificationChannelDescriptor{}
	var channelDescriptors = []*monitoringpb.NotificationChannelDescriptor{channelDescriptorsElement}
	var expectedResponse = &monitoringpb.ListNotificationChannelDescriptorsResponse{
		NextPageToken:      nextPageToken,
		ChannelDescriptors: channelDescriptors,
	}

	mockNotificationChannel.err = nil
	mockNotificationChannel.reqs = nil

	mockNotificationChannel.resps = append(mockNotificationChannel.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListNotificationChannelDescriptorsRequest{
		Name: formattedName,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListNotificationChannelDescriptors(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockNotificationChannel.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.ChannelDescriptors[0])
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

func TestNotificationChannelServiceListNotificationChannelDescriptorsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockNotificationChannel.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListNotificationChannelDescriptorsRequest{
		Name: formattedName,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListNotificationChannelDescriptors(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestNotificationChannelServiceGetNotificationChannelDescriptor(t *testing.T) {
	var name2 string = "name2-1052831874"
	var type_ string = "type3575610"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &monitoringpb.NotificationChannelDescriptor{
		Name:        name2,
		Type:        type_,
		DisplayName: displayName,
		Description: description,
	}

	mockNotificationChannel.err = nil
	mockNotificationChannel.reqs = nil

	mockNotificationChannel.resps = append(mockNotificationChannel.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/notificationChannelDescriptors/%s", "[PROJECT]", "[CHANNEL_DESCRIPTOR]")
	var request = &monitoringpb.GetNotificationChannelDescriptorRequest{
		Name: formattedName,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetNotificationChannelDescriptor(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockNotificationChannel.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestNotificationChannelServiceGetNotificationChannelDescriptorError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockNotificationChannel.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/notificationChannelDescriptors/%s", "[PROJECT]", "[CHANNEL_DESCRIPTOR]")
	var request = &monitoringpb.GetNotificationChannelDescriptorRequest{
		Name: formattedName,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetNotificationChannelDescriptor(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestNotificationChannelServiceListNotificationChannels(t *testing.T) {
	var nextPageToken string = ""
	var notificationChannelsElement *monitoringpb.NotificationChannel = &monitoringpb.NotificationChannel{}
	var notificationChannels = []*monitoringpb.NotificationChannel{notificationChannelsElement}
	var expectedResponse = &monitoringpb.ListNotificationChannelsResponse{
		NextPageToken:        nextPageToken,
		NotificationChannels: notificationChannels,
	}

	mockNotificationChannel.err = nil
	mockNotificationChannel.reqs = nil

	mockNotificationChannel.resps = append(mockNotificationChannel.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListNotificationChannelsRequest{
		Name: formattedName,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListNotificationChannels(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockNotificationChannel.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.NotificationChannels[0])
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

func TestNotificationChannelServiceListNotificationChannelsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockNotificationChannel.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListNotificationChannelsRequest{
		Name: formattedName,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListNotificationChannels(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestNotificationChannelServiceGetNotificationChannel(t *testing.T) {
	var type_ string = "type3575610"
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &monitoringpb.NotificationChannel{
		Type:        type_,
		Name:        name2,
		DisplayName: displayName,
		Description: description,
	}

	mockNotificationChannel.err = nil
	mockNotificationChannel.reqs = nil

	mockNotificationChannel.resps = append(mockNotificationChannel.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/notificationChannels/%s", "[PROJECT]", "[NOTIFICATION_CHANNEL]")
	var request = &monitoringpb.GetNotificationChannelRequest{
		Name: formattedName,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetNotificationChannel(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockNotificationChannel.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestNotificationChannelServiceGetNotificationChannelError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockNotificationChannel.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/notificationChannels/%s", "[PROJECT]", "[NOTIFICATION_CHANNEL]")
	var request = &monitoringpb.GetNotificationChannelRequest{
		Name: formattedName,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetNotificationChannel(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestNotificationChannelServiceCreateNotificationChannel(t *testing.T) {
	var type_ string = "type3575610"
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &monitoringpb.NotificationChannel{
		Type:        type_,
		Name:        name2,
		DisplayName: displayName,
		Description: description,
	}

	mockNotificationChannel.err = nil
	mockNotificationChannel.reqs = nil

	mockNotificationChannel.resps = append(mockNotificationChannel.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var notificationChannel *monitoringpb.NotificationChannel = &monitoringpb.NotificationChannel{}
	var request = &monitoringpb.CreateNotificationChannelRequest{
		Name:                formattedName,
		NotificationChannel: notificationChannel,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateNotificationChannel(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockNotificationChannel.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestNotificationChannelServiceCreateNotificationChannelError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockNotificationChannel.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var notificationChannel *monitoringpb.NotificationChannel = &monitoringpb.NotificationChannel{}
	var request = &monitoringpb.CreateNotificationChannelRequest{
		Name:                formattedName,
		NotificationChannel: notificationChannel,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateNotificationChannel(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestNotificationChannelServiceUpdateNotificationChannel(t *testing.T) {
	var type_ string = "type3575610"
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var description string = "description-1724546052"
	var expectedResponse = &monitoringpb.NotificationChannel{
		Type:        type_,
		Name:        name,
		DisplayName: displayName,
		Description: description,
	}

	mockNotificationChannel.err = nil
	mockNotificationChannel.reqs = nil

	mockNotificationChannel.resps = append(mockNotificationChannel.resps[:0], expectedResponse)

	var notificationChannel *monitoringpb.NotificationChannel = &monitoringpb.NotificationChannel{}
	var request = &monitoringpb.UpdateNotificationChannelRequest{
		NotificationChannel: notificationChannel,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateNotificationChannel(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockNotificationChannel.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestNotificationChannelServiceUpdateNotificationChannelError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockNotificationChannel.err = gstatus.Error(errCode, "test error")

	var notificationChannel *monitoringpb.NotificationChannel = &monitoringpb.NotificationChannel{}
	var request = &monitoringpb.UpdateNotificationChannelRequest{
		NotificationChannel: notificationChannel,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateNotificationChannel(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestNotificationChannelServiceDeleteNotificationChannel(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockNotificationChannel.err = nil
	mockNotificationChannel.reqs = nil

	mockNotificationChannel.resps = append(mockNotificationChannel.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/notificationChannels/%s", "[PROJECT]", "[NOTIFICATION_CHANNEL]")
	var request = &monitoringpb.DeleteNotificationChannelRequest{
		Name: formattedName,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteNotificationChannel(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockNotificationChannel.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestNotificationChannelServiceDeleteNotificationChannelError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockNotificationChannel.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/notificationChannels/%s", "[PROJECT]", "[NOTIFICATION_CHANNEL]")
	var request = &monitoringpb.DeleteNotificationChannelRequest{
		Name: formattedName,
	}

	c, err := NewNotificationChannelClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteNotificationChannel(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestUptimeCheckServiceListUptimeCheckConfigs(t *testing.T) {
	var nextPageToken string = ""
	var totalSize int32 = 705419236
	var uptimeCheckConfigsElement *monitoringpb.UptimeCheckConfig = &monitoringpb.UptimeCheckConfig{}
	var uptimeCheckConfigs = []*monitoringpb.UptimeCheckConfig{uptimeCheckConfigsElement}
	var expectedResponse = &monitoringpb.ListUptimeCheckConfigsResponse{
		NextPageToken:      nextPageToken,
		TotalSize:          totalSize,
		UptimeCheckConfigs: uptimeCheckConfigs,
	}

	mockUptimeCheck.err = nil
	mockUptimeCheck.reqs = nil

	mockUptimeCheck.resps = append(mockUptimeCheck.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListUptimeCheckConfigsRequest{
		Parent: formattedParent,
	}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListUptimeCheckConfigs(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockUptimeCheck.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.UptimeCheckConfigs[0])
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

func TestUptimeCheckServiceListUptimeCheckConfigsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockUptimeCheck.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var request = &monitoringpb.ListUptimeCheckConfigsRequest{
		Parent: formattedParent,
	}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListUptimeCheckConfigs(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestUptimeCheckServiceGetUptimeCheckConfig(t *testing.T) {
	var name2 string = "name2-1052831874"
	var displayName string = "displayName1615086568"
	var isInternal bool = true
	var expectedResponse = &monitoringpb.UptimeCheckConfig{
		Name:        name2,
		DisplayName: displayName,
		IsInternal:  isInternal,
	}

	mockUptimeCheck.err = nil
	mockUptimeCheck.reqs = nil

	mockUptimeCheck.resps = append(mockUptimeCheck.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/uptimeCheckConfigs/%s", "[PROJECT]", "[UPTIME_CHECK_CONFIG]")
	var request = &monitoringpb.GetUptimeCheckConfigRequest{
		Name: formattedName,
	}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetUptimeCheckConfig(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockUptimeCheck.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestUptimeCheckServiceGetUptimeCheckConfigError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockUptimeCheck.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/uptimeCheckConfigs/%s", "[PROJECT]", "[UPTIME_CHECK_CONFIG]")
	var request = &monitoringpb.GetUptimeCheckConfigRequest{
		Name: formattedName,
	}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.GetUptimeCheckConfig(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestUptimeCheckServiceCreateUptimeCheckConfig(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var isInternal bool = true
	var expectedResponse = &monitoringpb.UptimeCheckConfig{
		Name:        name,
		DisplayName: displayName,
		IsInternal:  isInternal,
	}

	mockUptimeCheck.err = nil
	mockUptimeCheck.reqs = nil

	mockUptimeCheck.resps = append(mockUptimeCheck.resps[:0], expectedResponse)

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var uptimeCheckConfig *monitoringpb.UptimeCheckConfig = &monitoringpb.UptimeCheckConfig{}
	var request = &monitoringpb.CreateUptimeCheckConfigRequest{
		Parent:            formattedParent,
		UptimeCheckConfig: uptimeCheckConfig,
	}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateUptimeCheckConfig(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockUptimeCheck.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestUptimeCheckServiceCreateUptimeCheckConfigError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockUptimeCheck.err = gstatus.Error(errCode, "test error")

	var formattedParent string = fmt.Sprintf("projects/%s", "[PROJECT]")
	var uptimeCheckConfig *monitoringpb.UptimeCheckConfig = &monitoringpb.UptimeCheckConfig{}
	var request = &monitoringpb.CreateUptimeCheckConfigRequest{
		Parent:            formattedParent,
		UptimeCheckConfig: uptimeCheckConfig,
	}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.CreateUptimeCheckConfig(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestUptimeCheckServiceUpdateUptimeCheckConfig(t *testing.T) {
	var name string = "name3373707"
	var displayName string = "displayName1615086568"
	var isInternal bool = true
	var expectedResponse = &monitoringpb.UptimeCheckConfig{
		Name:        name,
		DisplayName: displayName,
		IsInternal:  isInternal,
	}

	mockUptimeCheck.err = nil
	mockUptimeCheck.reqs = nil

	mockUptimeCheck.resps = append(mockUptimeCheck.resps[:0], expectedResponse)

	var uptimeCheckConfig *monitoringpb.UptimeCheckConfig = &monitoringpb.UptimeCheckConfig{}
	var request = &monitoringpb.UpdateUptimeCheckConfigRequest{
		UptimeCheckConfig: uptimeCheckConfig,
	}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateUptimeCheckConfig(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockUptimeCheck.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	if want, got := expectedResponse, resp; !proto.Equal(want, got) {
		t.Errorf("wrong response %q, want %q)", got, want)
	}
}

func TestUptimeCheckServiceUpdateUptimeCheckConfigError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockUptimeCheck.err = gstatus.Error(errCode, "test error")

	var uptimeCheckConfig *monitoringpb.UptimeCheckConfig = &monitoringpb.UptimeCheckConfig{}
	var request = &monitoringpb.UpdateUptimeCheckConfigRequest{
		UptimeCheckConfig: uptimeCheckConfig,
	}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.UpdateUptimeCheckConfig(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
func TestUptimeCheckServiceDeleteUptimeCheckConfig(t *testing.T) {
	var expectedResponse *emptypb.Empty = &emptypb.Empty{}

	mockUptimeCheck.err = nil
	mockUptimeCheck.reqs = nil

	mockUptimeCheck.resps = append(mockUptimeCheck.resps[:0], expectedResponse)

	var formattedName string = fmt.Sprintf("projects/%s/uptimeCheckConfigs/%s", "[PROJECT]", "[UPTIME_CHECK_CONFIG]")
	var request = &monitoringpb.DeleteUptimeCheckConfigRequest{
		Name: formattedName,
	}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteUptimeCheckConfig(context.Background(), request)

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockUptimeCheck.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

}

func TestUptimeCheckServiceDeleteUptimeCheckConfigError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockUptimeCheck.err = gstatus.Error(errCode, "test error")

	var formattedName string = fmt.Sprintf("projects/%s/uptimeCheckConfigs/%s", "[PROJECT]", "[UPTIME_CHECK_CONFIG]")
	var request = &monitoringpb.DeleteUptimeCheckConfigRequest{
		Name: formattedName,
	}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	err = c.DeleteUptimeCheckConfig(context.Background(), request)

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
}
func TestUptimeCheckServiceListUptimeCheckIps(t *testing.T) {
	var nextPageToken string = ""
	var uptimeCheckIpsElement *monitoringpb.UptimeCheckIp = &monitoringpb.UptimeCheckIp{}
	var uptimeCheckIps = []*monitoringpb.UptimeCheckIp{uptimeCheckIpsElement}
	var expectedResponse = &monitoringpb.ListUptimeCheckIpsResponse{
		NextPageToken:  nextPageToken,
		UptimeCheckIps: uptimeCheckIps,
	}

	mockUptimeCheck.err = nil
	mockUptimeCheck.reqs = nil

	mockUptimeCheck.resps = append(mockUptimeCheck.resps[:0], expectedResponse)

	var request *monitoringpb.ListUptimeCheckIpsRequest = &monitoringpb.ListUptimeCheckIpsRequest{}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListUptimeCheckIps(context.Background(), request).Next()

	if err != nil {
		t.Fatal(err)
	}

	if want, got := request, mockUptimeCheck.reqs[0]; !proto.Equal(want, got) {
		t.Errorf("wrong request %q, want %q", got, want)
	}

	want := (interface{})(expectedResponse.UptimeCheckIps[0])
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

func TestUptimeCheckServiceListUptimeCheckIpsError(t *testing.T) {
	errCode := codes.PermissionDenied
	mockUptimeCheck.err = gstatus.Error(errCode, "test error")

	var request *monitoringpb.ListUptimeCheckIpsRequest = &monitoringpb.ListUptimeCheckIpsRequest{}

	c, err := NewUptimeCheckClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := c.ListUptimeCheckIps(context.Background(), request).Next()

	if st, ok := gstatus.FromError(err); !ok {
		t.Errorf("got error %v, expected grpc error", err)
	} else if c := st.Code(); c != errCode {
		t.Errorf("got error code %q, want %q", c, errCode)
	}
	_ = resp
}
