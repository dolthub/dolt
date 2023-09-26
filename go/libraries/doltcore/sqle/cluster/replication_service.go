// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	replicationapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/replicationapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type BranchControlPersistence interface {
	LoadData([]byte, bool) error
	SaveData(filesys.Filesys) error
}

type replicationServiceServer struct {
	replicationapi.UnimplementedReplicationServiceServer

	mysqlDb *mysql_db.MySQLDb
	lgr     *logrus.Entry

	branchControl        BranchControlPersistence
	branchControlFilesys filesys.Filesys

	dropDatabaseProvider func(context.Context, string) error
}

func (s *replicationServiceServer) UpdateUsersAndGrants(ctx context.Context, req *replicationapi.UpdateUsersAndGrantsRequest) (*replicationapi.UpdateUsersAndGrantsResponse, error) {
	sqlCtx := sql.NewContext(ctx)
	ed := s.mysqlDb.Editor()
	defer ed.Close()
	err := s.mysqlDb.OverwriteUsersAndGrantData(sqlCtx, ed, req.SerializedContents)
	if err != nil {
		return nil, err
	}
	err = s.mysqlDb.Persist(sqlCtx, ed)
	if err != nil {
		return nil, err
	}
	return &replicationapi.UpdateUsersAndGrantsResponse{}, nil
}

func (s *replicationServiceServer) UpdateBranchControl(ctx context.Context, req *replicationapi.UpdateBranchControlRequest) (*replicationapi.UpdateBranchControlResponse, error) {
	err := s.branchControl.LoadData(req.SerializedContents /* isFirstLoad */, false)
	if err != nil {
		return nil, err
	}
	err = s.branchControl.SaveData(s.branchControlFilesys)
	if err != nil {
		return nil, err
	}
	return &replicationapi.UpdateBranchControlResponse{}, nil
}

func (s *replicationServiceServer) DropDatabase(ctx context.Context, req *replicationapi.DropDatabaseRequest) (*replicationapi.DropDatabaseResponse, error) {
	if s.dropDatabaseProvider == nil {
		return nil, status.Error(codes.Unimplemented, "unimplemented")
	}

	err := s.dropDatabaseProvider(ctx, req.Name)
	s.lgr.Tracef("dropped database [%s] through dropDatabaseProvider. err: %v", req.Name, err)
	if err != nil && !sql.ErrDatabaseNotFound.Is(err) {
		return nil, err
	}
	return &replicationapi.DropDatabaseResponse{}, nil
}
