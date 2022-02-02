// Copyright 2019 Dolthub, Inc.
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

package dbfactory

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// AWSScheme
	AWSScheme = "aws"

	// GSScheme
	GSScheme = "gs"

	// FileScheme
	FileScheme = "file"

	// MemScheme
	MemScheme = "mem"

	// HTTPSScheme
	HTTPSScheme = "https"

	// HTTPScheme
	HTTPScheme = "http"

	// InMemBlobstore Scheme
	LocalBSScheme = "localbs"

	defaultScheme       = HTTPSScheme
	defaultMemTableSize = 256 * 1024 * 1024
)

// DBFactory is an interface for creating concrete datas.Database instances which may have different backing stores.
type DBFactory interface {
	CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, error)
}

// DBFactories is a map from url scheme name to DBFactory.  Additional factories can be added to the DBFactories map
// from external packages.
var DBFactories = map[string]DBFactory{
	AWSScheme:     AWSFactory{},
	GSScheme:      GSFactory{},
	FileScheme:    FileFactory{},
	MemScheme:     MemFactory{},
	LocalBSScheme: LocalBSFactory{},
	HTTPScheme:    NewDoltRemoteFactory(true),
	HTTPSScheme:   NewDoltRemoteFactory(false),
}

// CreateDB creates a database based on the supplied urlStr, and creation params.  The DBFactory used for creation is
// determined by the scheme of the url.  Naked urls will use https by default.
func CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlStr string, params map[string]interface{}) (datas.Database, types.ValueReadWriter, error) {
	urlObj, err := earl.Parse(urlStr)

	if err != nil {
		return nil, nil, err
	}

	scheme := urlObj.Scheme
	if len(scheme) == 0 {
		scheme = defaultScheme
	}

	if fact, ok := DBFactories[strings.ToLower(scheme)]; ok {
		return fact.CreateDB(ctx, nbf, urlObj, params)
	}

	return nil, nil, fmt.Errorf("unknown url scheme: '%s'", urlObj.Scheme)
}
