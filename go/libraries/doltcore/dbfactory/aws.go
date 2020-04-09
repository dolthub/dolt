// Copyright 2019 Liquidata, Inc.
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
	"errors"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/datas"
	"github.com/liquidata-inc/dolt/go/store/nbs"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	// AWSRegionParam is a creation parameter that can be used to set the AWS region
	AWSRegionParam = "aws-region"

	// AWSCredsTypeParam is a creation parameter that can be used to set the type of credentials that should be used.
	// valid values are role, env, auto, and file
	AWSCredsTypeParam = "aws-creds-type"

	// AWSCredsFileParam is a creation parameter that can be used to specify a credential file to use.
	AWSCredsFileParam = "aws-creds-file"

	//AWSCredsProfile is a creation parameter that can be used to specify which AWS profile to use.
	AWSCredsProfile = "aws-creds-profile"
)

// AWSCredentialSource is an enum type representing the different credential sources (auto, role, env, file, or invalid)
type AWSCredentialSource int

const (
	InvalidCS AWSCredentialSource = iota - 1

	// Auto will try env first and fall back to role (This is the default)
	AutoCS

	// Role Uses the AWS IAM role of the instance for auth
	RoleCS

	// Env uses the credentials stored in the environment variables AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY
	EnvCS

	// Uses credentials stored in a file
	FileCS
)

// String returns the string representation of the of an AWSCredentialSource
func (ct AWSCredentialSource) String() string {
	switch ct {
	case RoleCS:
		return "role"
	case EnvCS:
		return "env"
	case AutoCS:
		return "auto"
	case FileCS:
		return "file"
	default:
		return "invalid"
	}
}

// AWSCredentialSourceFromStr converts a string to an AWSCredentialSource
func AWSCredentialSourceFromStr(str string) AWSCredentialSource {
	strlwr := strings.TrimSpace(strings.ToLower(str))
	switch strlwr {
	case "", "auto":
		return AutoCS
	case "role":
		return RoleCS
	case "env":
		return EnvCS
	case "file":
		return FileCS
	default:
		return InvalidCS
	}
}

// AWSFactory is a DBFactory implementation for creating AWS backed databases
type AWSFactory struct {
}

// CreateDB creates an AWS backed database
func (fact AWSFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]string) (datas.Database, error) {
	var db datas.Database
	cs, err := fact.newChunkStore(ctx, nbf, urlObj, params)

	if err != nil {
		return nil, err
	}

	db = datas.NewDatabase(cs)

	return db, err
}

func (fact AWSFactory) newChunkStore(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]string) (chunks.ChunkStore, error) {
	parts := strings.SplitN(urlObj.Hostname(), ":", 2) // [table]:[bucket]
	if len(parts) != 2 {
		return nil, errors.New("aws url has an invalid format")
	}

	opts, err := awsConfigFromParams(params)

	if err != nil {
		return nil, err
	}

	dbName, err := validatePath(urlObj.Path)

	if err != nil {
		return nil, err
	}

	sess := session.Must(session.NewSessionWithOptions(opts))
	return nbs.NewAWSStore(ctx, nbf.VersionString(), parts[0], dbName, parts[1], s3.New(sess), dynamodb.New(sess), defaultMemTableSize)
}

func validatePath(path string) (string, error) {
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	pathLen := len(path)
	for pathLen > 0 && path[pathLen-1] == '/' {
		path = path[:pathLen-1]
		pathLen--
	}

	// Should probably have regex validation of a valid database name here once we decide what valid database names look
	// like.
	if len(path) == 0 || strings.Index(path, "/") != -1 {
		return "", errors.New("invalid database name")
	}

	return path, nil
}

func awsConfigFromParams(params map[string]string) (session.Options, error) {
	awsConfig := aws.NewConfig()
	if val, ok := params[AWSRegionParam]; ok {
		awsConfig = awsConfig.WithRegion(val)
	}

	awsCredsSource := RoleCS
	if val, ok := params[AWSCredsTypeParam]; ok {
		awsCredsSource = AWSCredentialSourceFromStr(val)
		if awsCredsSource == InvalidCS {
			return session.Options{}, errors.New("invalid value for aws-creds-source")
		}
	}

	opts := session.Options{}

	profile := ""
	if val, ok := params[AWSCredsProfile]; ok {
		profile = val
		opts.Profile = val
	}

	switch awsCredsSource {
	case EnvCS:
		awsConfig = awsConfig.WithCredentials(credentials.NewEnvCredentials())
	case FileCS:
		if filePath, ok := params[AWSCredsFileParam]; !ok {
			return opts, os.ErrNotExist
		} else {
			creds := credentials.NewSharedCredentials(filePath, profile)
			awsConfig = awsConfig.WithCredentials(creds)
		}
	case AutoCS:
		// start by trying to get the credentials from the environment
		envCreds := credentials.NewEnvCredentials()
		if _, err := envCreds.Get(); err == nil {
			awsConfig = awsConfig.WithCredentials(envCreds)
		} else {
			// if env credentials don't exist try looking for a credentials file
			if filePath, ok := params[AWSCredsFileParam]; ok {
				if _, err := os.Stat(filePath); err == nil {
					creds := credentials.NewSharedCredentials(filePath, profile)
					awsConfig = awsConfig.WithCredentials(creds)
				}
			}

			// if file and env do not return valid credentials use the default credentials of the box (same as role)
		}
	case RoleCS:
	default:
	}

	opts.Config.MergeIn(awsConfig)

	return opts, nil
}
