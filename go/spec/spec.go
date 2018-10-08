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
	"regexp"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/nbs"
	"github.com/attic-labs/noms/go/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	Separator              = "::"
	DefaultAWSRegion       = "us-west-2"
	DefaultAWSCredsProfile = "default"
)

var datasetRe = regexp.MustCompile("^" + datas.DatasetRe.String() + "$")

type ProtocolImpl interface {
	NewChunkStore(sp Spec) (chunks.ChunkStore, error)
	NewDatabase(sp Spec) (datas.Database, error)
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
	// Protocol is one of "mem", "ldb", "http", or "https".
	Protocol string

	// DatabaseName is the name of the Spec's database, which is the string after
	// "protocol:". http/https specs include their leading "//" characters.
	DatabaseName string

	// Options are the SpecOptions that the Spec was constructed with.
	Options SpecOptions

	// Path is nil unless the spec was created with ForPath.
	Path AbsolutePath

	// db is lazily created, so it needs to be a pointer to a Database.
	db *datas.Database
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

	if !path.Path.IsEmpty() {
		return Spec{}, errors.New("path is not allowed for dataset spec")
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
func (sp Spec) GetDatabase() datas.Database {
	if *sp.db == nil {
		*sp.db = sp.createDatabase()
	}
	return *sp.db
}

// NewChunkStore returns a new ChunkStore instance that this Spec's
// DatabaseName describes. It's unusual to call this method, GetDatabase is
// more useful. Unlike GetDatabase, a new ChunkStore instance is returned every
// time. If there is no ChunkStore, for example remote databases, returns nil.
func (sp Spec) NewChunkStore() chunks.ChunkStore {
	switch sp.Protocol {
	case "http", "https":
		return nil
	case "aws":
		return parseAWSSpec(sp.Href(), sp.Options)
	case "gs":
		return parseGCSSpec(sp.Href(), sp.Options)
	case "nbs":
		return nbs.NewLocalStore(sp.DatabaseName, 1<<28)
	case "mem":
		storage := &chunks.MemoryStorage{}
		return storage.NewView()
	default:
		impl, ok := ExternalProtocols[sp.Protocol]
		if !ok {
			d.PanicIfError(fmt.Errorf("Unknown protocol: %s", sp.Protocol))
		}
		r, err := impl.NewChunkStore(sp)
		d.PanicIfError(err)
		return r
	}
}

func parseAWSSpec(awsURL string, options SpecOptions) chunks.ChunkStore {
	fmt.Println(awsURL, options)

	u, _ := url.Parse(awsURL)
	parts := strings.SplitN(u.Host, ":", 2) // [table] [, bucket]?
	d.PanicIfFalse(len(parts) == 2)

	awsConfig := aws.NewConfig().WithRegion(options.AwsRegionOrDefault())

	switch options.AWSCredSource {
	case RoleCS:
	case EnvCS:
		awsConfig = awsConfig.WithCredentials(credentials.NewEnvCredentials())
	case FileCS:
		filePath := options.AwsCredFileOrDefault()
		creds := credentials.NewSharedCredentials(filePath, DefaultAWSCredsProfile)
		awsConfig = awsConfig.WithCredentials(creds)
	case AutoCS:
		envCreds := credentials.NewEnvCredentials()
		if _, err := envCreds.Get(); err == nil {
			awsConfig = awsConfig.WithCredentials(envCreds)
		}

		filePath := options.AwsCredFileOrDefault()
		if _, err := os.Stat(filePath); err == nil {
			creds := credentials.NewSharedCredentials(filePath, DefaultAWSCredsProfile)
			awsConfig = awsConfig.WithCredentials(creds)
		}
	default:
		panic("unsupported credential type")
	}

	sess := session.Must(session.NewSession(awsConfig))
	return nbs.NewAWSStore(parts[0], u.Path, parts[1], s3.New(sess), dynamodb.New(sess), 1<<28)
}

func parseGCSSpec(gcsURL string, options SpecOptions) chunks.ChunkStore {
	u, err := url.Parse(gcsURL)
	d.PanicIfError(err)

	fmt.Println(u)

	bucket := u.Host
	path := u.Path

	ctx := context.Background()
	gcs, err := storage.NewClient(ctx)

	if err != nil {
		panic("Could not create GCSBlobstore")
	}

	return nbs.NewGCSStore(ctx, bucket, path, gcs, 1<<28)
}

// GetDataset returns the current Dataset instance for this Spec's Database.
// GetDataset is live, so if Commit is called on this Spec's Database later, a
// new up-to-date Dataset will returned on the next call to GetDataset.  If
// this is not a Dataset spec, returns nil.
func (sp Spec) GetDataset() (ds datas.Dataset) {
	if sp.Path.Dataset != "" {
		ds = sp.GetDatabase().GetDataset(sp.Path.Dataset)
	}
	return
}

// GetValue returns the Value at this Spec's Path within its Database, or nil
// if this isn't a Path Spec or if that path isn't found.
func (sp Spec) GetValue() (val types.Value) {
	if !sp.Path.IsEmpty() {
		val = sp.Path.Resolve(sp.GetDatabase())
	}
	return
}

// Href treats the Protocol and DatabaseName as a URL, and returns its href.
// For example, the spec http://example.com/path::ds returns
// "http://example.com/path". If the Protocol is not "http" or "http", returns
// an empty string.
func (sp Spec) Href() string {
	switch proto := sp.Protocol; proto {
	case "http", "https", "aws", "gs":
		return proto + ":" + sp.DatabaseName
	default:
		return ""
	}
}

// Pin returns a Spec in which the dataset component, if any, has been replaced
// with the hash of the HEAD of that dataset. This "pins" the path to the state
// of the database at the current moment in time.  Returns itself if the
// PathSpec is already "pinned".
func (sp Spec) Pin() (Spec, bool) {
	var ds datas.Dataset

	if !sp.Path.IsEmpty() {
		if !sp.Path.Hash.IsEmpty() {
			// Spec is already pinned.
			return sp, true
		}

		ds = sp.GetDatabase().GetDataset(sp.Path.Dataset)
	} else {
		ds = sp.GetDataset()
	}

	commit, ok := ds.MaybeHead()
	if !ok {
		return Spec{}, false
	}

	r := sp
	r.Path.Hash = commit.Hash()
	r.Path.Dataset = ""

	return r, true
}

func (sp Spec) Close() error {
	db := *sp.db
	if db == nil {
		return nil
	}

	*sp.db = nil
	return db.Close()
}

func (sp Spec) createDatabase() datas.Database {
	switch sp.Protocol {
	case "http", "https":
		return datas.NewDatabase(datas.NewHTTPChunkStore(sp.Href(), sp.Options.Authorization))
	case "aws":
		return datas.NewDatabase(parseAWSSpec(sp.Href(), sp.Options))
	case "gs":
		return datas.NewDatabase(parseGCSSpec(sp.Href(), sp.Options))
	case "nbs":
		os.Mkdir(sp.DatabaseName, 0777)
		return datas.NewDatabase(nbs.NewLocalStore(sp.DatabaseName, 1<<28))
	case "mem":
		storage := &chunks.MemoryStorage{}
		return datas.NewDatabase(storage.NewView())
	default:
		impl, ok := ExternalProtocols[sp.Protocol]
		if !ok {
			d.PanicIfError(fmt.Errorf("Unknown protocol: %s", sp.Protocol))
		}
		r, err := impl.NewDatabase(sp)
		d.PanicIfError(err)
		return r
	}
}

func parseDatabaseSpec(spec string) (protocol, name string, err error) {
	if len(spec) == 0 {
		err = fmt.Errorf("Empty spec")
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
	}

	if _, ok := ExternalProtocols[parts[0]]; ok {
		fmt.Println("found external spec", parts[0])
		protocol, name = parts[0], parts[1]
		return
	}

	switch parts[0] {
	case "nbs":
		protocol, name = parts[0], parts[1]

	case "http", "https", "aws", "gs":
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
		err = fmt.Errorf(`In-memory database must be specified as "mem", not "mem:"`)

	default:
		err = fmt.Errorf("Invalid database protocol %s in %s", protocol, spec)
	}
	return
}

func splitDatabaseSpec(spec string) (string, string, error) {
	lastIdx := strings.LastIndex(spec, Separator)
	if lastIdx == -1 {
		return "", "", fmt.Errorf("Missing %s after database in %s", Separator, spec)
	}

	return spec[:lastIdx], spec[lastIdx+len(Separator):], nil
}
