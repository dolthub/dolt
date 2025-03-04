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
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dolthub/dolt/go/libraries/utils/awsrefreshcreds"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
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

var AWSFileCredsRefreshDuration = time.Minute

var AWSCredTypes = []string{RoleCS.String(), EnvCS.String(), FileCS.String()}

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

func (fact AWSFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) error {
	// nothing to prepare
	return nil
}

// CreateDB creates an AWS backed database
func (fact AWSFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	var db datas.Database
	cs, err := fact.newChunkStore(ctx, nbf, urlObj, params)

	if err != nil {
		return nil, nil, nil, err
	}

	vrw := types.NewValueStore(cs)
	ns := tree.NewNodeStore(cs)
	db = datas.NewTypesDatabase(vrw, ns)

	return db, vrw, ns, nil
}

func (fact AWSFactory) newChunkStore(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (chunks.ChunkStore, error) {
	parts := strings.SplitN(urlObj.Hostname(), ":", 2) // [table]:[bucket]
	if len(parts) != 2 {
		return nil, errors.New("aws url has an invalid format")
	}

	cfg, err := awsConfigFromParams(ctx, params)

	if err != nil {
		return nil, err
	}

	dbName, err := validatePath(urlObj.Path)

	if err != nil {
		return nil, err
	}

	// Sanity check that we have credentials...
	_, err = cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, err
	}

	q := nbs.NewUnlimitedMemQuotaProvider()
	return nbs.NewAWSStore(ctx, nbf.VersionString(), parts[0], dbName, parts[1], s3.NewFromConfig(cfg), dynamodb.NewFromConfig(cfg), defaultMemTableSize, q)
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

	if len(path) == 0 {
		return "", errors.New("invalid database name")
	}

	return path, nil
}

func awsConfigFromParams(ctx context.Context, params map[string]interface{}) (aws.Config, error) {
	var opts []func(*config.LoadOptions) error

	// aws-region always sets the region. Otherwise it comes from AWS_REGION or AWS_DEFAULT_REGION.
	if val, ok := params[AWSRegionParam]; ok {
		opts = append(opts, config.WithRegion(val.(string)))
	}

	awsCredsSource := RoleCS
	if val, ok := params[AWSCredsTypeParam]; ok {
		awsCredsSource = AWSCredentialSourceFromStr(val.(string))
		if awsCredsSource == InvalidCS {
			return aws.Config{}, errors.New("invalid value for aws-creds-source")
		}
	}

	profile := ""
	if val, ok := params[AWSCredsProfile]; ok {
		profile = val.(string)
		opts = append(opts, config.WithSharedConfigProfile(val.(string)))
	}

	filePath, ok := params[AWSCredsFileParam]
	if ok && len(filePath.(string)) != 0 && awsCredsSource == RoleCS {
		awsCredsSource = FileCS
	}

	switch awsCredsSource {
	case EnvCS:
		// Credentials can only come directly from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY...
		creds := awsrefreshcreds.LoadEnvCredentials()
		opts = append(opts, config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			if !creds.HasKeys() {
				return aws.Credentials{}, errors.New("error loading env creds; did not find AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY environment variable.")
			} else {
				return creds, nil
			}
		})))
	case FileCS:
		if filePath, ok := params[AWSCredsFileParam]; !ok {
			return aws.Config{}, os.ErrNotExist
		} else {
			provider := awsrefreshcreds.LoadINICredentialsProvider(filePath.(string), profile)
			provider = awsrefreshcreds.NewRefreshingCredentialsProvider(provider, AWSFileCredsRefreshDuration)
			opts = append(opts, config.WithCredentialsProvider(provider))
		}
	case AutoCS:
		if envCreds := awsrefreshcreds.LoadEnvCredentials(); envCreds.HasKeys() {
			opts = append(opts, config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return envCreds, nil
			})))
		} else {
			// if env credentials don't exist try looking for a credentials file
			if filePath, ok := params[AWSCredsFileParam]; ok {
				if _, err := os.Stat(filePath.(string)); err == nil {
					provider := awsrefreshcreds.LoadINICredentialsProvider(filePath.(string), profile)
					opts = append(opts, config.WithCredentialsProvider(provider))
				}
			}
		}
	// if file and env do not return valid credentials use the default credentials of the box (same as role)
	case RoleCS:
	default:
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	var profileErr config.SharedConfigProfileNotExistError
	if errors.As(err, &profileErr) {
		// XXX: Dolt was originaly using aws-sdk-go, which was
		// happy to load the specified shared profile from
		// places like AWS_CONFIG_FILE or $HOME/.aws/config,
		// but did not complain if it could not find it.
		//
		// We preserve that behavior here with this gross
		// hack. We write a shared config file with an empty
		// profile, and we point to that config file when
		// loading the config.
		if profile == "" {
			profile = os.Getenv("AWS_PROFILE")
		}
		if profile == "" {
			profile = os.Getenv("AWS_DEFAULT_PROFILE")
		}
		path, ferr := makeTempEmptyProfileConfig(profile)
		if path != "" {
			defer os.Remove(path)
		}
		if ferr == nil {
			opts = append(opts, config.WithSharedConfigFiles([]string{path}))
			cfg, err = config.LoadDefaultConfig(ctx, opts...)
		}
	}
	return cfg, err
}

func makeTempEmptyProfileConfig(profile string) (string, error) {
	f, err := os.CreateTemp("", "dolt_aws_empty_profile-*")
	if err != nil {
		return "", err
	}
	_, err = fmt.Fprintf(f, "[profile %s]\n", profile)
	if err != nil {
		return f.Name(), errors.Join(err, f.Close())
	}
	return f.Name(), f.Close()
}
