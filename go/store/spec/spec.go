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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package spec provides builders and parsers for spelling Noms databases,
// datasets and values.
package spec

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"

	"github.com/dolthub/dolt/go/libraries/utils/awsrefreshcreds"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	Separator              = "::"
	DefaultAWSRegion       = "us-west-2"
	DefaultAWSCredsProfile = "default"
)

type ProtocolImpl interface {
	NewChunkStore(sp Spec) (chunks.ChunkStore, error)
}

var ExternalProtocols = map[string]ProtocolImpl{}

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

// SpecOptions customize Spec behavior.
type SpecOptions struct {
	// Authorization token for requests. For example, if the database is HTTP
	// this will used for an `Authorization: Bearer ${authorization}` header.
	Authorization string

	// Region that should be used when creating the aws session
	AWSRegion string

	// The type of credentials that should be used when creating the aws session
	AWSCredSource AWSCredentialSource

	// Credential file to use when using auto or file credentials
	AWSCredFile string
}

func (so *SpecOptions) AwsRegionOrDefault() string {
	if so.AWSRegion == "" {
		return DefaultAWSRegion
	}

	return so.AWSRegion
}

func (so *SpecOptions) AwsCredFileOrDefault() string {
	if so.AWSCredFile == "" {
		usr, err := user.Current()
		if err != nil {
			return ""
		}

		return filepath.Join(usr.HomeDir, ".aws", "credentials")
	}

	return so.AWSCredFile
}

// Spec locates a Noms database, dataset, or value globally. Spec caches
// its database instance so it therefore does not reflect new commits in
// the db, by (legacy) design.
type Spec struct {
	// Protocol is one of "mem", "aws", "gs", "nbs"
	Protocol string

	// DatabaseName is the name of the Spec's database, which is the string after
	// "protocol:". specs include their leading "//" characters.
	DatabaseName string

	// Options are the SpecOptions that the Spec was constructed with.
	Options SpecOptions

	// Path is nil unless the spec was created with ForPath.
	Path AbsolutePath

	// db is lazily created, so it needs to be a pointer to a Database.
	db  *datas.Database
	vrw *types.ValueReadWriter
	ns  *tree.NodeStore
}

func newSpec(dbSpec string, opts SpecOptions) (Spec, error) {
	protocol, dbName, err := parseDatabaseSpec(dbSpec)
	if err != nil {
		return Spec{}, err
	}

	return Spec{
		Protocol:     protocol,
		DatabaseName: dbName,
		Options:      opts,
		db:           new(datas.Database),
		vrw:          new(types.ValueReadWriter),
		ns:           new(tree.NodeStore),
	}, nil
}

// ForDatabase parses a spec for a Database.
func ForDatabase(spec string) (Spec, error) {
	return ForDatabaseOpts(spec, SpecOptions{})
}

// ForDatabaseOpts parses a spec for a Database.
func ForDatabaseOpts(spec string, opts SpecOptions) (Spec, error) {
	return newSpec(spec, opts)
}

// ForDataset parses a spec for a Dataset.
func ForDataset(spec string) (Spec, error) {
	return ForDatasetOpts(spec, SpecOptions{})
}

// ForDatasetOpts parses a spec for a Dataset.
func ForDatasetOpts(spec string, opts SpecOptions) (Spec, error) {
	dbSpec, pathStr, err := splitDatabaseSpec(spec)
	if err != nil {
		return Spec{}, err
	}

	sp, err := newSpec(dbSpec, opts)
	if err != nil {
		return Spec{}, err
	}

	path, err := NewAbsolutePath(pathStr)
	if err != nil {
		return Spec{}, err
	}

	if path.Dataset == "" {
		return Spec{}, errors.New("dataset name required for dataset spec")
	}

	sp.Path = path
	return sp, nil
}

// ForPath parses a spec for a path to a Value.
func ForPath(spec string) (Spec, error) {
	return ForPathOpts(spec, SpecOptions{})
}

// ForPathOpts parses a spec for a path to a Value.
func ForPathOpts(spec string, opts SpecOptions) (Spec, error) {
	dbSpec, pathStr, err := splitDatabaseSpec(spec)
	if err != nil {
		return Spec{}, err
	}

	var path AbsolutePath
	if pathStr != "" {
		path, err = NewAbsolutePath(pathStr)
		if err != nil {
			return Spec{}, err
		}
	}

	sp, err := newSpec(dbSpec, opts)
	if err != nil {
		return Spec{}, err
	}

	sp.Path = path
	return sp, nil
}

func (sp Spec) String() string {
	s := sp.Protocol
	if s != "mem" {
		s += ":" + sp.DatabaseName
	}
	p := sp.Path.String()
	if p != "" {
		s += Separator + p
	}
	return s
}

// GetDatabase returns the Database instance that this Spec's DatabaseName
// describes. The same Database instance is returned every time, unless Close
// is called. If the Spec is closed, it is re-opened with a new Database.
func (sp Spec) GetDatabase(ctx context.Context) datas.Database {
	if *sp.db == nil {
		db, vrw, ns := sp.createDatabase(ctx)
		*sp.db = db
		*sp.vrw = vrw
		*sp.ns = ns
	}
	return *sp.db
}

func (sp Spec) GetNodeStore(ctx context.Context) tree.NodeStore {
	if *sp.db == nil {
		db, vrw, ns := sp.createDatabase(ctx)
		*sp.db = db
		*sp.vrw = vrw
		*sp.ns = ns
	}
	return *sp.ns
}

func (sp Spec) GetVRW(ctx context.Context) types.ValueReadWriter {
	if *sp.db == nil {
		db, vrw, ns := sp.createDatabase(ctx)
		*sp.db = db
		*sp.vrw = vrw
		*sp.ns = ns
	}
	return *sp.vrw
}

// NewChunkStore returns a new ChunkStore instance that this Spec's
// DatabaseName describes. It's unusual to call this method, GetDatabase is
// more useful. Unlike GetDatabase, a new ChunkStore instance is returned every
// time. If there is no ChunkStore, for example remote databases, returns nil.
func (sp Spec) NewChunkStore(ctx context.Context) chunks.ChunkStore {
	switch sp.Protocol {
	case "http", "https":
		return nil
	case "aws":
		return parseAWSSpec(ctx, sp.Href(), sp.Options)
	case "gs":
		return parseGCSSpec(ctx, sp.Href(), sp.Options)
	case "oci":
		return parseOCISpec(ctx, sp.Href(), sp.Options)
	case "nbs":
		cs, err := nbs.NewLocalStore(ctx, types.Format_Default.VersionString(), sp.DatabaseName, 1<<28, nbs.NewUnlimitedMemQuotaProvider())
		d.PanicIfError(err)
		return cs
	case "mem":
		storage := &chunks.MemoryStorage{}
		return storage.NewView()
	default:
		impl, ok := ExternalProtocols[sp.Protocol]
		if !ok {
			d.PanicIfError(fmt.Errorf("unknown protocol: %s", sp.Protocol))
		}
		r, err := impl.NewChunkStore(sp)
		d.PanicIfError(err)
		return r
	}
}

func parseAWSSpec(ctx context.Context, awsURL string, options SpecOptions) chunks.ChunkStore {
	fmt.Println(awsURL, options)

	u, _ := url.Parse(awsURL)
	parts := strings.SplitN(u.Hostname(), ":", 2) // [table] [, bucket]?
	d.PanicIfFalse(len(parts) == 2)

	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(options.AwsRegionOrDefault()))

	switch options.AWSCredSource {
	case RoleCS:
	// All the default behavior of the SDK.
	case EnvCS:
		// Credentials can only come directly from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY...
		creds := awsrefreshcreds.LoadEnvCredentials()
		opts = append(opts, config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return creds, nil
		})))
	case FileCS:
		provider := awsrefreshcreds.LoadINICredentialsProvider(options.AwsCredFileOrDefault(), DefaultAWSCredsProfile)
		opts = append(opts, config.WithCredentialsProvider(provider))
	case AutoCS:
		var opt config.LoadOptionsFunc

		if envCreds := awsrefreshcreds.LoadEnvCredentials(); envCreds.HasKeys() {
			opt = config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return envCreds, nil
			}))
		}

		filePath := options.AwsCredFileOrDefault()
		if _, err := os.Stat(filePath); err == nil {
			provider := awsrefreshcreds.LoadINICredentialsProvider(options.AwsCredFileOrDefault(), DefaultAWSCredsProfile)
			opt = config.WithCredentialsProvider(provider)
		}

		if opt != nil {
			opts = append(opts, opt)
		}
	default:
		panic("unsupported credential type")
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	d.PanicIfError(err)

	cs, err := nbs.NewAWSStore(ctx, types.Format_Default.VersionString(), parts[0], u.Path, parts[1], s3.NewFromConfig(cfg), dynamodb.NewFromConfig(cfg), 1<<28, nbs.NewUnlimitedMemQuotaProvider())
	d.PanicIfError(err)

	return cs
}

func parseGCSSpec(ctx context.Context, gcsURL string, options SpecOptions) chunks.ChunkStore {
	u, err := url.Parse(gcsURL)
	d.PanicIfError(err)

	fmt.Println(u)

	bucket := u.Host
	path := u.Path

	gcs, err := storage.NewClient(ctx)

	if err != nil {
		panic("Could not create GCSBlobstore")
	}

	cs, err := nbs.NewGCSStore(ctx, types.Format_Default.VersionString(), bucket, path, gcs, 1<<28, nbs.NewUnlimitedMemQuotaProvider())

	d.PanicIfError(err)

	return cs
}

func parseOCISpec(ctx context.Context, ociURL string, options SpecOptions) chunks.ChunkStore {
	u, err := url.Parse(ociURL)
	d.PanicIfError(err)

	fmt.Println(u)

	bucket := u.Host
	path := u.Path

	provider := common.DefaultConfigProvider()

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		panic("Could not create OCIBlobstore")
	}

	cs, err := nbs.NewOCISStore(ctx, types.Format_Default.VersionString(), bucket, path, provider, client, 1<<28, nbs.NewUnlimitedMemQuotaProvider())
	d.PanicIfError(err)

	return cs
}

// GetDataset returns the current Dataset instance for this Spec's Database.
// GetDataset is live, so if Commit is called on this Spec's Database later, a
// new up-to-date Dataset will returned on the next call to GetDataset.  If
// this is not a Dataset spec, returns nil.
func (sp Spec) GetDataset(ctx context.Context) (ds datas.Dataset) {
	if sp.Path.Dataset != "" {
		var err error
		ds, err = sp.GetDatabase(ctx).GetDataset(ctx, sp.Path.Dataset)
		d.PanicIfError(err)
	}
	return
}

// GetValue returns the Value at this Spec's Path within its Database, or nil
// if this isn't a Path Spec or if that path isn't found.
func (sp Spec) GetValue(ctx context.Context) (val types.Value, err error) {
	if !sp.Path.IsEmpty() {
		val, err = sp.Path.Resolve(ctx, sp.GetDatabase(ctx), sp.GetVRW(ctx))
		if err != nil {
			return nil, err
		}
	}
	return
}

// Href treats the Protocol and DatabaseName as a URL, and returns its href.
// For example, the spec http://example.com/path::ds returns
// "http://example.com/path". If the Protocol is not "http" or "http", returns
// an empty string.
func (sp Spec) Href() string {
	switch proto := sp.Protocol; proto {
	case "http", "https", "aws", "gs", "oci":
		return proto + ":" + sp.DatabaseName
	default:
		return ""
	}
}

func (sp Spec) Close() error {
	db := *sp.db
	if db == nil {
		return nil
	}

	*sp.db = nil
	return db.Close()
}

func (sp Spec) createDatabase(ctx context.Context) (datas.Database, types.ValueReadWriter, tree.NodeStore) {
	switch sp.Protocol {
	case "aws":
		cs := parseAWSSpec(ctx, sp.Href(), sp.Options)
		ns := tree.NewNodeStore(cs)
		vrw := types.NewValueStore(cs)
		return datas.NewTypesDatabase(vrw, ns), vrw, ns
	case "gs":
		cs := parseGCSSpec(ctx, sp.Href(), sp.Options)
		ns := tree.NewNodeStore(cs)
		vrw := types.NewValueStore(cs)
		return datas.NewTypesDatabase(vrw, ns), vrw, ns
	case "oci":
		cs := parseOCISpec(ctx, sp.Href(), sp.Options)
		ns := tree.NewNodeStore(cs)
		vrw := types.NewValueStore(cs)
		return datas.NewTypesDatabase(vrw, ns), vrw, ns
	case "nbs":
		// If the database is the oldgen database return a standard NBS store.
		if strings.Contains(sp.DatabaseName, "oldgen") {
			return getStandardLocalStore(ctx, sp.DatabaseName)
		}

		oldgenDb := filepath.Join(sp.DatabaseName, "oldgen")

		err := validateDir(oldgenDb)
		// If we can't validate that an oldgen db exists just use a standard local store.
		if err != nil {
			return getStandardLocalStore(ctx, sp.DatabaseName)
		}

		newGenSt, err := nbs.NewLocalJournalingStore(ctx, types.Format_Default.VersionString(), sp.DatabaseName, nbs.NewUnlimitedMemQuotaProvider())

		// If the journaling store can't be created, fall back to a standard local store
		if err != nil {
			var localErr error
			newGenSt, localErr = nbs.NewLocalStore(ctx, types.Format_Default.VersionString(), sp.DatabaseName, 1<<28, nbs.NewUnlimitedMemQuotaProvider())
			if localErr != nil {
				d.PanicIfError(err)
			}
		}

		oldGenSt, err := nbs.NewLocalStore(ctx, newGenSt.Version(), oldgenDb, 1<<28, nbs.NewUnlimitedMemQuotaProvider())
		d.PanicIfError(err)

		cs := nbs.NewGenerationalCS(oldGenSt, newGenSt, nil)

		ns := tree.NewNodeStore(cs)
		vrw := types.NewValueStore(cs)
		return datas.NewTypesDatabase(vrw, ns), vrw, ns
	case "mem":
		storage := &chunks.MemoryStorage{}
		cs := storage.NewViewWithDefaultFormat()
		ns := tree.NewNodeStore(cs)
		vrw := types.NewValueStore(cs)
		return datas.NewTypesDatabase(vrw, ns), vrw, ns
	default:
		impl, ok := ExternalProtocols[sp.Protocol]
		if !ok {
			d.PanicIfError(fmt.Errorf("unknown protocol: %s", sp.Protocol))
		}
		cs, err := impl.NewChunkStore(sp)
		d.PanicIfError(err)
		vrw := types.NewValueStore(cs)
		ns := tree.NewNodeStore(cs)
		return datas.NewTypesDatabase(vrw, ns), vrw, ns
	}
}

func getStandardLocalStore(ctx context.Context, dbName string) (datas.Database, types.ValueReadWriter, tree.NodeStore) {
	os.Mkdir(dbName, 0777)

	cs, err := nbs.NewLocalStore(ctx, types.Format_Default.VersionString(), dbName, 1<<28, nbs.NewUnlimitedMemQuotaProvider())
	d.PanicIfError(err)

	vrw := types.NewValueStore(cs)
	ns := tree.NewNodeStore(cs)
	return datas.NewTypesDatabase(vrw, ns), vrw, ns
}

func validateDir(path string) error {
	info, err := os.Stat(path)

	if err != nil {
		return err
	} else if !info.IsDir() {
		return filesys.ErrIsFile
	}

	return nil
}

func parseDatabaseSpec(spec string) (protocol, name string, err error) {
	if len(spec) == 0 {
		err = fmt.Errorf("empty spec")
		return
	}

	parts := strings.SplitN(spec, ":", 2) // [protocol] [, path]?

	// If there was no ":" then this is either a mem spec, or a filesystem path.
	// This is ambiguous if the file system path is "mem" but that just means the
	// path needs to be explicitly "nbs:mem".
	if len(parts) == 1 {
		if spec == "mem" {
			protocol = "mem"
		} else {
			protocol, name = "nbs", spec
		}
		return
	} else if len(parts) == 2 && len(parts[0]) == 1 && parts[0][0] >= 'A' && parts[0][0] <= 'Z' { //check for Windows drive letter, ala C:\Users\Public
		if _, err := os.Stat(parts[0] + `:\`); !os.IsNotExist(err) {
			parts = []string{"nbs", spec}
		}
	}

	if _, ok := ExternalProtocols[parts[0]]; ok {
		protocol, name = parts[0], parts[1]
		return
	}

	switch parts[0] {
	case "nbs":
		protocol, name = parts[0], parts[1]

	case "aws", "gs", "oci":
		u, perr := url.Parse(spec)
		if perr != nil {
			err = perr
		} else if u.Host == "" {
			err = fmt.Errorf("%s has empty host", spec)
		} else if parts[0] == "aws" && u.Path == "" {
			err = fmt.Errorf("%s does not specify a database ID", spec)
		} else {
			protocol, name = parts[0], parts[1]
		}

	case "mem":
		err = fmt.Errorf(`in-memory database must be specified as "mem", not "mem:"`)

	default:
		err = fmt.Errorf("invalid database protocol %s in %s", protocol, spec)
	}
	return
}

func splitDatabaseSpec(spec string) (string, string, error) {
	lastIdx := strings.LastIndex(spec, Separator)
	if lastIdx == -1 {
		return "", "", fmt.Errorf("missing %s after database in %s", Separator, spec)
	}

	return spec[:lastIdx], spec[lastIdx+len(Separator):], nil
}
