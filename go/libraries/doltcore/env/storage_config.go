// Copyright 2026 Dolthub, Inc.
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

package env

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/file"
)

// storage configures the storage of the databases that environments open and create. Dolt Enterprise replaces it.
var storage storageConfig = plainStorage{}

type storageConfig interface {
	// loadParams returns new params, beyond its own, that |dEnv| loads its database with.
	loadParams(dEnv *DoltEnv) (map[string]interface{}, error)
	// remoteParams adds to |params| what a remote database opened through |dialer| needs.
	remoteParams(params map[string]interface{}, dialer dbfactory.GRPCDialProvider) error
	// remoteDialer returns the dialer, wrapping |dialer|, that opens the remotes of |local|.
	remoteDialer(local *doltdb.DoltDB, dialer dbfactory.GRPCDialProvider) dbfactory.GRPCDialProvider
	// copyParams returns the remote params that create a database to receive copies of |src|'s files.
	copyParams(ctx context.Context, src *doltdb.DoltDB) (map[string]string, error)
	// createParamsFor returns the params that create a database for the remotesapi request of |ctx|.
	createParamsFor(ctx context.Context) (map[string]string, error)
	// fileSealer returns the sealer of the |kind| files of databases that |cfg| configures.
	fileSealer(cfg config.ReadableConfig, kind string) file.Sealer
}

type plainStorage struct{}

func (plainStorage) loadParams(*DoltEnv) (map[string]interface{}, error) {
	return nil, nil
}

func (plainStorage) remoteParams(map[string]interface{}, dbfactory.GRPCDialProvider) error {
	return nil
}

func (plainStorage) remoteDialer(_ *doltdb.DoltDB, dialer dbfactory.GRPCDialProvider) dbfactory.GRPCDialProvider {
	return dialer
}

func (plainStorage) copyParams(context.Context, *doltdb.DoltDB) (map[string]string, error) {
	return nil, nil
}

func (plainStorage) createParamsFor(context.Context) (map[string]string, error) {
	return nil, nil
}

func (plainStorage) fileSealer(config.ReadableConfig, string) file.Sealer {
	return file.PlainSealer{}
}

// AddDBLoadParams adds |params|, such as the params that create a database, to the params that this environment
// loads its database with.
func (dEnv *DoltEnv) AddDBLoadParams(params map[string]string) {
	if len(params) == 0 {
		return
	}
	if dEnv.DBLoadParams == nil {
		dEnv.DBLoadParams = make(map[string]interface{}, len(params))
	}
	for k, v := range params {
		dEnv.DBLoadParams[k] = v
	}
}

// CopyParams returns the remote params that create a database to receive copies of |src|'s files.
func CopyParams(ctx context.Context, src *doltdb.DoltDB) (map[string]string, error) {
	return storage.copyParams(ctx, src)
}

// ForPushFrom returns |r| with the params that create its database, if it is missing, to receive a push of |local|.
func (r *Remote) ForPushFrom(ctx context.Context, local *doltdb.DoltDB) (Remote, error) {
	params, err := CopyParams(ctx, local)
	if err != nil || len(params) == 0 {
		return *r, err
	}
	return r.WithParams(params), nil
}

// RemoteDialerFor returns the dialer, wrapping |dialer|, that opens the remotes of |local|.
func RemoteDialerFor(local *doltdb.DoltDB, dialer dbfactory.GRPCDialProvider) dbfactory.GRPCDialProvider {
	return storage.remoteDialer(local, dialer)
}

// CreateParamsForClient returns the params that create a database for the remotesapi request of |ctx|.
func CreateParamsForClient(ctx context.Context) (map[string]string, error) {
	return storage.createParamsFor(ctx)
}

// FileSealer returns the sealer of the |kind| files, such as privileges, of databases that |cfg| configures.
func FileSealer(cfg config.ReadableConfig, kind string) file.Sealer {
	return storage.fileSealer(cfg, kind)
}
