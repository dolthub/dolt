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
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

const (

	// OSSCredsFileParam is a creation parameter that can be used to specify a credential file to use.
	OSSCredsFileParam = "oss-creds-file"

	// OSSCredsProfile is a creation parameter that can be used to specify which OSS profile to use.
	OSSCredsProfile = "oss-creds-profile"
)

var (
	emptyOSSCredential = ossCredential{}
)

type ossParams map[string]interface{}
type ossCredentials map[string]ossCredential

type ossCredential struct {
	Endpoint        string `json:"endpoint,omitempty"`
	AccessKeyID     string `json:"accessKeyID,omitempty"`
	AccessKeySecret string `json:"accessKeySecret,omitempty"`
}

// OSSFactory is a DBFactory implementation for creating OSS backed databases
type OSSFactory struct {
}

// PrepareDB prepares an OSS backed database
func (fact OSSFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	// nothing to prepare
	return nil
}

// CreateDB creates an OSS backed database
func (fact OSSFactory) GetDBLoader(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (DBLoader, error) {
	ossStore, err := fact.newChunkStore(ctx, nbf, urlObj, params)
	if err != nil {
		return nil, err
	}

	return ChunkStoreLoader{cs: ossStore}, nil
}

func (fact OSSFactory) newChunkStore(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (chunks.ChunkStore, error) {
	// oss://[bucket]/[key]
	bucket := urlObj.Hostname()
	prefix := urlObj.Path

	opts := ossConfigFromParams(params)

	ossClient, err := getOSSClient(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize oss err: %s", err)
	}
	bs, err := blobstore.NewOSSBlobstore(ossClient, bucket, prefix)
	if err != nil {
		return nil, errors.New("failed to initialize oss blob store")
	}

	q := nbs.NewUnlimitedMemQuotaProvider()
	return nbs.NewBSStore(ctx, nbf.VersionString(), bs, defaultMemTableSize, q)
}

func ossConfigFromParams(params map[string]interface{}) ossCredential {
	// then we look for config from oss-creds-file
	p := ossParams(params)
	credFile, err := p.getCredFile()
	if err != nil {
		return emptyOSSCredential
	}
	creds, err := readOSSCredentialsFromFile(credFile)
	if err != nil {
		return emptyOSSCredential
	}
	// if there is only 1 cred in the file, just use this cred regardless the profile is
	if len(creds) == 1 {
		return creds.First()
	}
	// otherwise, we try to get cred by profile from cred file
	if res, ok := creds[p.getCredProfile()]; ok {
		return res
	}
	return emptyOSSCredential
}

func getOSSClient(opts ossCredential) (*oss.Client, error) {
	var (
		endpoint, accessKeyID, accessKeySecret string
		err                                    error
	)
	if endpoint, err = opts.getEndPoint(); err != nil {
		return nil, err
	}
	if accessKeyID, err = opts.getAccessKeyID(); err != nil {
		return nil, err
	}
	if accessKeySecret, err = opts.getAccessKeySecret(); err != nil {
		return nil, err
	}
	return oss.New(
		endpoint,
		accessKeyID,
		accessKeySecret,
	)
}

func (opt ossCredential) getEndPoint() (string, error) {
	if opt.Endpoint != "" {
		return opt.Endpoint, nil
	}
	if v := os.Getenv(dconfig.EnvOssEndpoint); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("failed to find endpoint from cred file or env %s", dconfig.EnvOssEndpoint)
}

func (opt ossCredential) getAccessKeyID() (string, error) {
	if opt.AccessKeyID != "" {
		return opt.AccessKeyID, nil
	}
	if v := os.Getenv(dconfig.EnvOssAccessKeyID); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("failed to find accessKeyID from cred file or env %s", dconfig.EnvOssAccessKeyID)
}

func (opt ossCredential) getAccessKeySecret() (string, error) {
	if opt.AccessKeySecret != "" {
		return opt.AccessKeySecret, nil
	}
	if v := os.Getenv(dconfig.EnvOssAccessKeySecret); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("failed to find accessKeySecret from cred file or env %s", dconfig.EnvOssAccessKeySecret)
}

func readOSSCredentialsFromFile(credFile string) (ossCredentials, error) {
	data, err := os.ReadFile(credFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read oss cred file %s, err: %s", credFile, err)
	}
	var res map[string]ossCredential
	if err = json.Unmarshal(data, &res); err != nil {
		return nil, fmt.Errorf("invalid oss credential file %s, err: %s", credFile, err)
	}
	if len(res) == 0 {
		return nil, errors.New("empty credential file is not allowed")
	}
	return res, nil
}

func (oc ossCredentials) First() ossCredential {
	var res ossCredential
	for _, c := range oc {
		res = c
		break
	}
	return res
}

func (p ossParams) getCredFile() (string, error) {
	// then we look for config from oss-creds-file
	credFile, ok := p[OSSCredsFileParam]
	if !ok {
		// if oss-creds-files is
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to find oss cred file from home dir, err: %s", err)
		}
		credFile = filepath.Join(homeDir, ".oss", "dolt_oss_credentials")
	}
	return credFile.(string), nil
}

func (p ossParams) getCredProfile() string {
	credProfile, ok := p[OSSCredsProfile]
	if !ok {
		credProfile = "default"
	}
	return credProfile.(string)
}
