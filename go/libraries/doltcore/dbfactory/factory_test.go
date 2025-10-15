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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/types"
)

func TestCreateMemDB(t *testing.T) {
	ctx := context.Background()
	db, vrw, ns, err := CreateDB(ctx, types.Format_Default, "mem://", nil)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.NotNil(t, vrw)
	assert.NotNil(t, ns)
}

func TestAWSScheme(t *testing.T) {
	url, err := earl.Parse("aws://[hosted-doltdb-prod-instance-backups:hosted-doltdb-prod-instance-backups]/5ebe7f6e-7c4a-437a-b684-6aa6cdeb63df/backups/20251014T083342.813/b1625a79-0ae3-4a83-89a7-8715317c4cea/us-jails")
	require.NoError(t, err)
	assert.Equal(t, "aws", url.Scheme)
	assert.Equal(t, "hosted-doltdb-prod-instance-backups:hosted-doltdb-prod-instance-backups", url.Hostname())
}
