/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testutil

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/ptypes/empty"
	proto3 "github.com/golang/protobuf/ptypes/struct"
	pbt "github.com/golang/protobuf/ptypes/timestamp"

	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// KvMeta is the Metadata for mocked KV table.
	KvMeta = sppb.ResultSetMetadata{
		RowType: &sppb.StructType{
			Fields: []*sppb.StructType_Field{
				{
					Name: "Key",
					Type: &sppb.Type{Code: sppb.TypeCode_STRING},
				},
				{
					Name: "Value",
					Type: &sppb.Type{Code: sppb.TypeCode_STRING},
				},
			},
		},
	}
)

// MockCtlMsg encapsulates PartialResultSet/error that might be sent to
// client
type MockCtlMsg struct {
	// If ResumeToken == true, mock server will generate a row with
	// resume token.
	ResumeToken bool
	// If Err != nil, mock server will return error in RPC response.
	Err error
}

// MockCloudSpanner is a mock implementation of SpannerServer interface.
// TODO: make MockCloudSpanner a full-fleged Cloud Spanner implementation.
type MockCloudSpanner struct {
	sppb.SpannerServer

	s      *grpc.Server
	t      *testing.T
	addr   string
	msgs   chan MockCtlMsg
	readTs time.Time

	mu          sync.Mutex
	next        int
	nextSession int
	sessions    map[string]*sppb.Session
}

// Addr returns the listening address of mock server.
func (m *MockCloudSpanner) Addr() string {
	return m.addr
}

// AddMsg generates a new mocked row which can be received by client.
func (m *MockCloudSpanner) AddMsg(err error, resumeToken bool) {
	msg := MockCtlMsg{
		ResumeToken: resumeToken,
		Err:         err,
	}
	if err == io.EOF {
		close(m.msgs)
	} else {
		m.msgs <- msg
	}
}

// Done signals an end to a mocked stream.
func (m *MockCloudSpanner) Done() {
	close(m.msgs)
}

// CreateSession is a placeholder for SpannerServer.CreateSession.
func (m *MockCloudSpanner) CreateSession(c context.Context, r *sppb.CreateSessionRequest) (*sppb.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := fmt.Sprintf("session-%d", m.nextSession)
	m.nextSession++
	s := &sppb.Session{Name: name}
	m.sessions[name] = s
	return s, nil
}

// GetSession is a placeholder for SpannerServer.GetSession.
func (m *MockCloudSpanner) GetSession(c context.Context, r *sppb.GetSessionRequest) (*sppb.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[r.Name]; ok {
		return s, nil
	}
	return nil, status.Errorf(codes.NotFound, "not found")
}

// DeleteSession is a placeholder for SpannerServer.DeleteSession.
func (m *MockCloudSpanner) DeleteSession(c context.Context, r *sppb.DeleteSessionRequest) (*empty.Empty, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, r.Name)
	return &empty.Empty{}, nil
}

// EncodeResumeToken return mock resume token encoding for an uint64 integer.
func EncodeResumeToken(t uint64) []byte {
	rt := make([]byte, 16)
	binary.PutUvarint(rt, t)
	return rt
}

// DecodeResumeToken decodes a mock resume token into an uint64 integer.
func DecodeResumeToken(t []byte) (uint64, error) {
	s, n := binary.Uvarint(t)
	if n <= 0 {
		return 0, fmt.Errorf("invalid resume token: %v", t)
	}
	return s, nil
}

// ExecuteStreamingSql is a mock implementation of SpannerServer.ExecuteStreamingSql.
func (m *MockCloudSpanner) ExecuteStreamingSql(r *sppb.ExecuteSqlRequest, s sppb.Spanner_ExecuteStreamingSqlServer) error {
	switch r.Sql {
	case "SELECT * from t_unavailable":
		return status.Errorf(codes.Unavailable, "mock table unavailable")
	case "SELECT t.key key, t.value value FROM t_mock t":
		if r.ResumeToken != nil {
			s, err := DecodeResumeToken(r.ResumeToken)
			if err != nil {
				return err
			}
			m.mu.Lock()
			m.next = int(s) + 1
			m.mu.Unlock()
		}
		for {
			msg, more := <-m.msgs
			if !more {
				break
			}
			if msg.Err == nil {
				var rt []byte
				if msg.ResumeToken {
					m.mu.Lock()
					rt = EncodeResumeToken(uint64(m.next))
					m.mu.Unlock()
				}
				meta := KvMeta
				meta.Transaction = &sppb.Transaction{
					ReadTimestamp: &pbt.Timestamp{
						Seconds: m.readTs.Unix(),
						Nanos:   int32(m.readTs.Nanosecond()),
					},
				}
				m.mu.Lock()
				next := m.next
				m.next++
				m.mu.Unlock()
				err := s.Send(&sppb.PartialResultSet{
					Metadata: &meta,
					Values: []*proto3.Value{
						{Kind: &proto3.Value_StringValue{StringValue: fmt.Sprintf("foo-%02d", next)}},
						{Kind: &proto3.Value_StringValue{StringValue: fmt.Sprintf("bar-%02d", next)}},
					},
					ResumeToken: rt,
				})
				if err != nil {
					return err
				}
				continue
			}
			return msg.Err
		}
		return nil
	default:
		return fmt.Errorf("unsupported SQL: %v", r.Sql)
	}
}

// StreamingRead is a placeholder for SpannerServer.StreamingRead.
func (m *MockCloudSpanner) StreamingRead(r *sppb.ReadRequest, s sppb.Spanner_StreamingReadServer) error {
	return s.Send(&sppb.PartialResultSet{})
}

// Serve runs a MockCloudSpanner listening on a random localhost address.
func (m *MockCloudSpanner) Serve() {
	m.s = grpc.NewServer()
	if m.addr == "" {
		m.addr = "localhost:0"
	}
	lis, err := net.Listen("tcp", m.addr)
	if err != nil {
		m.t.Fatalf("Failed to listen: %v", err)
	}
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		m.t.Fatalf("Failed to parse listener address: %v", err)
	}
	sppb.RegisterSpannerServer(m.s, m)
	m.addr = "localhost:" + port
	go m.s.Serve(lis)
}

// Stop terminates MockCloudSpanner and closes the serving port.
func (m *MockCloudSpanner) Stop() {
	m.s.Stop()
}

// NewMockCloudSpanner creates a new MockCloudSpanner instance.
func NewMockCloudSpanner(t *testing.T, ts time.Time) *MockCloudSpanner {
	mcs := &MockCloudSpanner{
		t:        t,
		msgs:     make(chan MockCtlMsg, 1000),
		readTs:   ts,
		sessions: map[string]*sppb.Session{},
	}
	return mcs
}
