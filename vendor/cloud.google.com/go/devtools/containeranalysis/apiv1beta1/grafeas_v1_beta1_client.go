// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// AUTO-GENERATED CODE. DO NOT EDIT.

package containeranalysis

import (
	"math"
	"time"

	"cloud.google.com/go/internal/version"
	"github.com/golang/protobuf/proto"
	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	grafeaspb "google.golang.org/genproto/googleapis/devtools/containeranalysis/v1beta1/grafeas"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// GrafeasV1Beta1CallOptions contains the retry settings for each method of GrafeasV1Beta1Client.
type GrafeasV1Beta1CallOptions struct {
	GetOccurrence                      []gax.CallOption
	ListOccurrences                    []gax.CallOption
	DeleteOccurrence                   []gax.CallOption
	CreateOccurrence                   []gax.CallOption
	BatchCreateOccurrences             []gax.CallOption
	UpdateOccurrence                   []gax.CallOption
	GetOccurrenceNote                  []gax.CallOption
	GetNote                            []gax.CallOption
	ListNotes                          []gax.CallOption
	DeleteNote                         []gax.CallOption
	CreateNote                         []gax.CallOption
	BatchCreateNotes                   []gax.CallOption
	UpdateNote                         []gax.CallOption
	ListNoteOccurrences                []gax.CallOption
	GetVulnerabilityOccurrencesSummary []gax.CallOption
}

func defaultGrafeasV1Beta1ClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("containeranalysis.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultGrafeasV1Beta1CallOptions() *GrafeasV1Beta1CallOptions {
	retry := map[[2]string][]gax.CallOption{
		{"default", "idempotent"}: {
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 1.3,
				})
			}),
		},
	}
	return &GrafeasV1Beta1CallOptions{
		GetOccurrence:                      retry[[2]string{"default", "idempotent"}],
		ListOccurrences:                    retry[[2]string{"default", "idempotent"}],
		DeleteOccurrence:                   retry[[2]string{"default", "idempotent"}],
		CreateOccurrence:                   retry[[2]string{"default", "non_idempotent"}],
		BatchCreateOccurrences:             retry[[2]string{"default", "non_idempotent"}],
		UpdateOccurrence:                   retry[[2]string{"default", "non_idempotent"}],
		GetOccurrenceNote:                  retry[[2]string{"default", "idempotent"}],
		GetNote:                            retry[[2]string{"default", "idempotent"}],
		ListNotes:                          retry[[2]string{"default", "idempotent"}],
		DeleteNote:                         retry[[2]string{"default", "idempotent"}],
		CreateNote:                         retry[[2]string{"default", "non_idempotent"}],
		BatchCreateNotes:                   retry[[2]string{"default", "non_idempotent"}],
		UpdateNote:                         retry[[2]string{"default", "non_idempotent"}],
		ListNoteOccurrences:                retry[[2]string{"default", "idempotent"}],
		GetVulnerabilityOccurrencesSummary: retry[[2]string{"default", "idempotent"}],
	}
}

// GrafeasV1Beta1Client is a client for interacting with Container Analysis API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type GrafeasV1Beta1Client struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	grafeasV1Beta1Client grafeaspb.GrafeasV1Beta1Client

	// The call options for this service.
	CallOptions *GrafeasV1Beta1CallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewGrafeasV1Beta1Client creates a new grafeas v1 beta1 client.
//
// Grafeas (at grafeas.io) API.
//
// Retrieves analysis results of Cloud components such as Docker container
// images.
//
// Analysis results are stored as a series of occurrences. An Occurrence
// contains information about a specific analysis instance on a resource. An
// occurrence refers to a Note. A note contains details describing the
// analysis and is generally stored in a separate project, called a Provider.
// Multiple occurrences can refer to the same note.
//
// For example, an SSL vulnerability could affect multiple images. In this case,
// there would be one note for the vulnerability and an occurrence for each
// image with the vulnerability referring to that note.
func NewGrafeasV1Beta1Client(ctx context.Context, opts ...option.ClientOption) (*GrafeasV1Beta1Client, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultGrafeasV1Beta1ClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &GrafeasV1Beta1Client{
		conn:        conn,
		CallOptions: defaultGrafeasV1Beta1CallOptions(),

		grafeasV1Beta1Client: grafeaspb.NewGrafeasV1Beta1Client(conn),
	}
	c.setGoogleClientInfo()
	return c, nil
}

// Connection returns the client's connection to the API service.
func (c *GrafeasV1Beta1Client) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *GrafeasV1Beta1Client) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *GrafeasV1Beta1Client) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// GetOccurrence gets the specified occurrence.
func (c *GrafeasV1Beta1Client) GetOccurrence(ctx context.Context, req *grafeaspb.GetOccurrenceRequest, opts ...gax.CallOption) (*grafeaspb.Occurrence, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetOccurrence[0:len(c.CallOptions.GetOccurrence):len(c.CallOptions.GetOccurrence)], opts...)
	var resp *grafeaspb.Occurrence
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.grafeasV1Beta1Client.GetOccurrence(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListOccurrences lists occurrences for the specified project.
func (c *GrafeasV1Beta1Client) ListOccurrences(ctx context.Context, req *grafeaspb.ListOccurrencesRequest, opts ...gax.CallOption) *OccurrenceIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListOccurrences[0:len(c.CallOptions.ListOccurrences):len(c.CallOptions.ListOccurrences)], opts...)
	it := &OccurrenceIterator{}
	req = proto.Clone(req).(*grafeaspb.ListOccurrencesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*grafeaspb.Occurrence, string, error) {
		var resp *grafeaspb.ListOccurrencesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.grafeasV1Beta1Client.ListOccurrences(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Occurrences, resp.NextPageToken, nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.PageSize)
	return it
}

// DeleteOccurrence deletes the specified occurrence. For example, use this method to delete an
// occurrence when the occurrence is no longer applicable for the given
// resource.
func (c *GrafeasV1Beta1Client) DeleteOccurrence(ctx context.Context, req *grafeaspb.DeleteOccurrenceRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteOccurrence[0:len(c.CallOptions.DeleteOccurrence):len(c.CallOptions.DeleteOccurrence)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.grafeasV1Beta1Client.DeleteOccurrence(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// CreateOccurrence creates a new occurrence.
func (c *GrafeasV1Beta1Client) CreateOccurrence(ctx context.Context, req *grafeaspb.CreateOccurrenceRequest, opts ...gax.CallOption) (*grafeaspb.Occurrence, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateOccurrence[0:len(c.CallOptions.CreateOccurrence):len(c.CallOptions.CreateOccurrence)], opts...)
	var resp *grafeaspb.Occurrence
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.grafeasV1Beta1Client.CreateOccurrence(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// BatchCreateOccurrences creates new occurrences in batch.
func (c *GrafeasV1Beta1Client) BatchCreateOccurrences(ctx context.Context, req *grafeaspb.BatchCreateOccurrencesRequest, opts ...gax.CallOption) (*grafeaspb.BatchCreateOccurrencesResponse, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.BatchCreateOccurrences[0:len(c.CallOptions.BatchCreateOccurrences):len(c.CallOptions.BatchCreateOccurrences)], opts...)
	var resp *grafeaspb.BatchCreateOccurrencesResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.grafeasV1Beta1Client.BatchCreateOccurrences(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateOccurrence updates the specified occurrence.
func (c *GrafeasV1Beta1Client) UpdateOccurrence(ctx context.Context, req *grafeaspb.UpdateOccurrenceRequest, opts ...gax.CallOption) (*grafeaspb.Occurrence, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateOccurrence[0:len(c.CallOptions.UpdateOccurrence):len(c.CallOptions.UpdateOccurrence)], opts...)
	var resp *grafeaspb.Occurrence
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.grafeasV1Beta1Client.UpdateOccurrence(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetOccurrenceNote gets the note attached to the specified occurrence. Consumer projects can
// use this method to get a note that belongs to a provider project.
func (c *GrafeasV1Beta1Client) GetOccurrenceNote(ctx context.Context, req *grafeaspb.GetOccurrenceNoteRequest, opts ...gax.CallOption) (*grafeaspb.Note, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetOccurrenceNote[0:len(c.CallOptions.GetOccurrenceNote):len(c.CallOptions.GetOccurrenceNote)], opts...)
	var resp *grafeaspb.Note
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.grafeasV1Beta1Client.GetOccurrenceNote(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetNote gets the specified note.
func (c *GrafeasV1Beta1Client) GetNote(ctx context.Context, req *grafeaspb.GetNoteRequest, opts ...gax.CallOption) (*grafeaspb.Note, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetNote[0:len(c.CallOptions.GetNote):len(c.CallOptions.GetNote)], opts...)
	var resp *grafeaspb.Note
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.grafeasV1Beta1Client.GetNote(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListNotes lists notes for the specified project.
func (c *GrafeasV1Beta1Client) ListNotes(ctx context.Context, req *grafeaspb.ListNotesRequest, opts ...gax.CallOption) *NoteIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListNotes[0:len(c.CallOptions.ListNotes):len(c.CallOptions.ListNotes)], opts...)
	it := &NoteIterator{}
	req = proto.Clone(req).(*grafeaspb.ListNotesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*grafeaspb.Note, string, error) {
		var resp *grafeaspb.ListNotesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.grafeasV1Beta1Client.ListNotes(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Notes, resp.NextPageToken, nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.PageSize)
	return it
}

// DeleteNote deletes the specified note.
func (c *GrafeasV1Beta1Client) DeleteNote(ctx context.Context, req *grafeaspb.DeleteNoteRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteNote[0:len(c.CallOptions.DeleteNote):len(c.CallOptions.DeleteNote)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.grafeasV1Beta1Client.DeleteNote(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// CreateNote creates a new note.
func (c *GrafeasV1Beta1Client) CreateNote(ctx context.Context, req *grafeaspb.CreateNoteRequest, opts ...gax.CallOption) (*grafeaspb.Note, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateNote[0:len(c.CallOptions.CreateNote):len(c.CallOptions.CreateNote)], opts...)
	var resp *grafeaspb.Note
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.grafeasV1Beta1Client.CreateNote(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// BatchCreateNotes creates new notes in batch.
func (c *GrafeasV1Beta1Client) BatchCreateNotes(ctx context.Context, req *grafeaspb.BatchCreateNotesRequest, opts ...gax.CallOption) (*grafeaspb.BatchCreateNotesResponse, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.BatchCreateNotes[0:len(c.CallOptions.BatchCreateNotes):len(c.CallOptions.BatchCreateNotes)], opts...)
	var resp *grafeaspb.BatchCreateNotesResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.grafeasV1Beta1Client.BatchCreateNotes(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateNote updates the specified note.
func (c *GrafeasV1Beta1Client) UpdateNote(ctx context.Context, req *grafeaspb.UpdateNoteRequest, opts ...gax.CallOption) (*grafeaspb.Note, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateNote[0:len(c.CallOptions.UpdateNote):len(c.CallOptions.UpdateNote)], opts...)
	var resp *grafeaspb.Note
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.grafeasV1Beta1Client.UpdateNote(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListNoteOccurrences lists occurrences referencing the specified note. Provider projects can use
// this method to get all occurrences across consumer projects referencing the
// specified note.
func (c *GrafeasV1Beta1Client) ListNoteOccurrences(ctx context.Context, req *grafeaspb.ListNoteOccurrencesRequest, opts ...gax.CallOption) *OccurrenceIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListNoteOccurrences[0:len(c.CallOptions.ListNoteOccurrences):len(c.CallOptions.ListNoteOccurrences)], opts...)
	it := &OccurrenceIterator{}
	req = proto.Clone(req).(*grafeaspb.ListNoteOccurrencesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*grafeaspb.Occurrence, string, error) {
		var resp *grafeaspb.ListNoteOccurrencesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.grafeasV1Beta1Client.ListNoteOccurrences(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Occurrences, resp.NextPageToken, nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	it.pageInfo.MaxSize = int(req.PageSize)
	return it
}

// GetVulnerabilityOccurrencesSummary gets a summary of the number and severity of occurrences.
func (c *GrafeasV1Beta1Client) GetVulnerabilityOccurrencesSummary(ctx context.Context, req *grafeaspb.GetVulnerabilityOccurrencesSummaryRequest, opts ...gax.CallOption) (*grafeaspb.VulnerabilityOccurrencesSummary, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetVulnerabilityOccurrencesSummary[0:len(c.CallOptions.GetVulnerabilityOccurrencesSummary):len(c.CallOptions.GetVulnerabilityOccurrencesSummary)], opts...)
	var resp *grafeaspb.VulnerabilityOccurrencesSummary
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.grafeasV1Beta1Client.GetVulnerabilityOccurrencesSummary(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// NoteIterator manages a stream of *grafeaspb.Note.
type NoteIterator struct {
	items    []*grafeaspb.Note
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*grafeaspb.Note, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *NoteIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *NoteIterator) Next() (*grafeaspb.Note, error) {
	var item *grafeaspb.Note
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *NoteIterator) bufLen() int {
	return len(it.items)
}

func (it *NoteIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// OccurrenceIterator manages a stream of *grafeaspb.Occurrence.
type OccurrenceIterator struct {
	items    []*grafeaspb.Occurrence
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*grafeaspb.Occurrence, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *OccurrenceIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *OccurrenceIterator) Next() (*grafeaspb.Occurrence, error) {
	var item *grafeaspb.Occurrence
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *OccurrenceIterator) bufLen() int {
	return len(it.items)
}

func (it *OccurrenceIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
