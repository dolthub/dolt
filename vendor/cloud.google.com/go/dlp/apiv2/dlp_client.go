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

package dlp

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
	dlppb "google.golang.org/genproto/googleapis/privacy/dlp/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	InspectContent           []gax.CallOption
	RedactImage              []gax.CallOption
	DeidentifyContent        []gax.CallOption
	ReidentifyContent        []gax.CallOption
	ListInfoTypes            []gax.CallOption
	CreateInspectTemplate    []gax.CallOption
	UpdateInspectTemplate    []gax.CallOption
	GetInspectTemplate       []gax.CallOption
	ListInspectTemplates     []gax.CallOption
	DeleteInspectTemplate    []gax.CallOption
	CreateDeidentifyTemplate []gax.CallOption
	UpdateDeidentifyTemplate []gax.CallOption
	GetDeidentifyTemplate    []gax.CallOption
	ListDeidentifyTemplates  []gax.CallOption
	DeleteDeidentifyTemplate []gax.CallOption
	CreateDlpJob             []gax.CallOption
	ListDlpJobs              []gax.CallOption
	GetDlpJob                []gax.CallOption
	DeleteDlpJob             []gax.CallOption
	CancelDlpJob             []gax.CallOption
	ListJobTriggers          []gax.CallOption
	GetJobTrigger            []gax.CallOption
	DeleteJobTrigger         []gax.CallOption
	UpdateJobTrigger         []gax.CallOption
	CreateJobTrigger         []gax.CallOption
}

func defaultClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("dlp.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultCallOptions() *CallOptions {
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
	return &CallOptions{
		InspectContent:           retry[[2]string{"default", "idempotent"}],
		RedactImage:              retry[[2]string{"default", "idempotent"}],
		DeidentifyContent:        retry[[2]string{"default", "idempotent"}],
		ReidentifyContent:        retry[[2]string{"default", "idempotent"}],
		ListInfoTypes:            retry[[2]string{"default", "idempotent"}],
		CreateInspectTemplate:    retry[[2]string{"default", "non_idempotent"}],
		UpdateInspectTemplate:    retry[[2]string{"default", "non_idempotent"}],
		GetInspectTemplate:       retry[[2]string{"default", "idempotent"}],
		ListInspectTemplates:     retry[[2]string{"default", "idempotent"}],
		DeleteInspectTemplate:    retry[[2]string{"default", "idempotent"}],
		CreateDeidentifyTemplate: retry[[2]string{"default", "non_idempotent"}],
		UpdateDeidentifyTemplate: retry[[2]string{"default", "non_idempotent"}],
		GetDeidentifyTemplate:    retry[[2]string{"default", "idempotent"}],
		ListDeidentifyTemplates:  retry[[2]string{"default", "idempotent"}],
		DeleteDeidentifyTemplate: retry[[2]string{"default", "idempotent"}],
		CreateDlpJob:             retry[[2]string{"default", "non_idempotent"}],
		ListDlpJobs:              retry[[2]string{"default", "idempotent"}],
		GetDlpJob:                retry[[2]string{"default", "idempotent"}],
		DeleteDlpJob:             retry[[2]string{"default", "idempotent"}],
		CancelDlpJob:             retry[[2]string{"default", "non_idempotent"}],
		ListJobTriggers:          retry[[2]string{"default", "idempotent"}],
		GetJobTrigger:            retry[[2]string{"default", "idempotent"}],
		DeleteJobTrigger:         retry[[2]string{"default", "idempotent"}],
		UpdateJobTrigger:         retry[[2]string{"default", "non_idempotent"}],
		CreateJobTrigger:         retry[[2]string{"default", "non_idempotent"}],
	}
}

// Client is a client for interacting with Cloud Data Loss Prevention (DLP) API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type Client struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	client dlppb.DlpServiceClient

	// The call options for this service.
	CallOptions *CallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClient creates a new dlp service client.
//
// The Cloud Data Loss Prevention (DLP) API is a service that allows clients
// to detect the presence of Personally Identifiable Information (PII) and other
// privacy-sensitive data in user-supplied, unstructured data streams, like text
// blocks or images.
// The service also includes methods for sensitive data redaction and
// scheduling of data scans on Google Cloud Platform based data sets.
//
// To learn more about concepts and find how-to guides see
// https://cloud.google.com/dlp/docs/.
func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:        conn,
		CallOptions: defaultCallOptions(),

		client: dlppb.NewDlpServiceClient(conn),
	}
	c.setGoogleClientInfo()
	return c, nil
}

// Connection returns the client's connection to the API service.
func (c *Client) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *Client) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *Client) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// InspectContent finds potentially sensitive info in content.
// This method has limits on input size, processing time, and output size.
//
// When no InfoTypes or CustomInfoTypes are specified in this request, the
// system will automatically choose what detectors to run. By default this may
// be all types, but may change over time as detectors are updated.
//
// For how to guides, see https://cloud.google.com/dlp/docs/inspecting-images
// and https://cloud.google.com/dlp/docs/inspecting-text,
func (c *Client) InspectContent(ctx context.Context, req *dlppb.InspectContentRequest, opts ...gax.CallOption) (*dlppb.InspectContentResponse, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.InspectContent[0:len(c.CallOptions.InspectContent):len(c.CallOptions.InspectContent)], opts...)
	var resp *dlppb.InspectContentResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.InspectContent(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// RedactImage redacts potentially sensitive info from an image.
// This method has limits on input size, processing time, and output size.
// See https://cloud.google.com/dlp/docs/redacting-sensitive-data-images to
// learn more.
//
// When no InfoTypes or CustomInfoTypes are specified in this request, the
// system will automatically choose what detectors to run. By default this may
// be all types, but may change over time as detectors are updated.
func (c *Client) RedactImage(ctx context.Context, req *dlppb.RedactImageRequest, opts ...gax.CallOption) (*dlppb.RedactImageResponse, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.RedactImage[0:len(c.CallOptions.RedactImage):len(c.CallOptions.RedactImage)], opts...)
	var resp *dlppb.RedactImageResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.RedactImage(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeidentifyContent de-identifies potentially sensitive info from a ContentItem.
// This method has limits on input size and output size.
// See https://cloud.google.com/dlp/docs/deidentify-sensitive-data to
// learn more.
//
// When no InfoTypes or CustomInfoTypes are specified in this request, the
// system will automatically choose what detectors to run. By default this may
// be all types, but may change over time as detectors are updated.
func (c *Client) DeidentifyContent(ctx context.Context, req *dlppb.DeidentifyContentRequest, opts ...gax.CallOption) (*dlppb.DeidentifyContentResponse, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeidentifyContent[0:len(c.CallOptions.DeidentifyContent):len(c.CallOptions.DeidentifyContent)], opts...)
	var resp *dlppb.DeidentifyContentResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.DeidentifyContent(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ReidentifyContent re-identifies content that has been de-identified.
// See
// https://cloud.google.com/dlp/docs/pseudonymization#re-identification_in_free_text_code_example
// to learn more.
func (c *Client) ReidentifyContent(ctx context.Context, req *dlppb.ReidentifyContentRequest, opts ...gax.CallOption) (*dlppb.ReidentifyContentResponse, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ReidentifyContent[0:len(c.CallOptions.ReidentifyContent):len(c.CallOptions.ReidentifyContent)], opts...)
	var resp *dlppb.ReidentifyContentResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.ReidentifyContent(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListInfoTypes returns a list of the sensitive information types that the DLP API
// supports. See https://cloud.google.com/dlp/docs/infotypes-reference to
// learn more.
func (c *Client) ListInfoTypes(ctx context.Context, req *dlppb.ListInfoTypesRequest, opts ...gax.CallOption) (*dlppb.ListInfoTypesResponse, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListInfoTypes[0:len(c.CallOptions.ListInfoTypes):len(c.CallOptions.ListInfoTypes)], opts...)
	var resp *dlppb.ListInfoTypesResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.ListInfoTypes(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateInspectTemplate creates an InspectTemplate for re-using frequently used configuration
// for inspecting content, images, and storage.
// See https://cloud.google.com/dlp/docs/creating-templates to learn more.
func (c *Client) CreateInspectTemplate(ctx context.Context, req *dlppb.CreateInspectTemplateRequest, opts ...gax.CallOption) (*dlppb.InspectTemplate, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateInspectTemplate[0:len(c.CallOptions.CreateInspectTemplate):len(c.CallOptions.CreateInspectTemplate)], opts...)
	var resp *dlppb.InspectTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateInspectTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateInspectTemplate updates the InspectTemplate.
// See https://cloud.google.com/dlp/docs/creating-templates to learn more.
func (c *Client) UpdateInspectTemplate(ctx context.Context, req *dlppb.UpdateInspectTemplateRequest, opts ...gax.CallOption) (*dlppb.InspectTemplate, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateInspectTemplate[0:len(c.CallOptions.UpdateInspectTemplate):len(c.CallOptions.UpdateInspectTemplate)], opts...)
	var resp *dlppb.InspectTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.UpdateInspectTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetInspectTemplate gets an InspectTemplate.
// See https://cloud.google.com/dlp/docs/creating-templates to learn more.
func (c *Client) GetInspectTemplate(ctx context.Context, req *dlppb.GetInspectTemplateRequest, opts ...gax.CallOption) (*dlppb.InspectTemplate, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetInspectTemplate[0:len(c.CallOptions.GetInspectTemplate):len(c.CallOptions.GetInspectTemplate)], opts...)
	var resp *dlppb.InspectTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetInspectTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListInspectTemplates lists InspectTemplates.
// See https://cloud.google.com/dlp/docs/creating-templates to learn more.
func (c *Client) ListInspectTemplates(ctx context.Context, req *dlppb.ListInspectTemplatesRequest, opts ...gax.CallOption) *InspectTemplateIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListInspectTemplates[0:len(c.CallOptions.ListInspectTemplates):len(c.CallOptions.ListInspectTemplates)], opts...)
	it := &InspectTemplateIterator{}
	req = proto.Clone(req).(*dlppb.ListInspectTemplatesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dlppb.InspectTemplate, string, error) {
		var resp *dlppb.ListInspectTemplatesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListInspectTemplates(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.InspectTemplates, resp.NextPageToken, nil
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

// DeleteInspectTemplate deletes an InspectTemplate.
// See https://cloud.google.com/dlp/docs/creating-templates to learn more.
func (c *Client) DeleteInspectTemplate(ctx context.Context, req *dlppb.DeleteInspectTemplateRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteInspectTemplate[0:len(c.CallOptions.DeleteInspectTemplate):len(c.CallOptions.DeleteInspectTemplate)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteInspectTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// CreateDeidentifyTemplate creates a DeidentifyTemplate for re-using frequently used configuration
// for de-identifying content, images, and storage.
// See https://cloud.google.com/dlp/docs/creating-templates-deid to learn
// more.
func (c *Client) CreateDeidentifyTemplate(ctx context.Context, req *dlppb.CreateDeidentifyTemplateRequest, opts ...gax.CallOption) (*dlppb.DeidentifyTemplate, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateDeidentifyTemplate[0:len(c.CallOptions.CreateDeidentifyTemplate):len(c.CallOptions.CreateDeidentifyTemplate)], opts...)
	var resp *dlppb.DeidentifyTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateDeidentifyTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateDeidentifyTemplate updates the DeidentifyTemplate.
// See https://cloud.google.com/dlp/docs/creating-templates-deid to learn
// more.
func (c *Client) UpdateDeidentifyTemplate(ctx context.Context, req *dlppb.UpdateDeidentifyTemplateRequest, opts ...gax.CallOption) (*dlppb.DeidentifyTemplate, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateDeidentifyTemplate[0:len(c.CallOptions.UpdateDeidentifyTemplate):len(c.CallOptions.UpdateDeidentifyTemplate)], opts...)
	var resp *dlppb.DeidentifyTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.UpdateDeidentifyTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetDeidentifyTemplate gets a DeidentifyTemplate.
// See https://cloud.google.com/dlp/docs/creating-templates-deid to learn
// more.
func (c *Client) GetDeidentifyTemplate(ctx context.Context, req *dlppb.GetDeidentifyTemplateRequest, opts ...gax.CallOption) (*dlppb.DeidentifyTemplate, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetDeidentifyTemplate[0:len(c.CallOptions.GetDeidentifyTemplate):len(c.CallOptions.GetDeidentifyTemplate)], opts...)
	var resp *dlppb.DeidentifyTemplate
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetDeidentifyTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListDeidentifyTemplates lists DeidentifyTemplates.
// See https://cloud.google.com/dlp/docs/creating-templates-deid to learn
// more.
func (c *Client) ListDeidentifyTemplates(ctx context.Context, req *dlppb.ListDeidentifyTemplatesRequest, opts ...gax.CallOption) *DeidentifyTemplateIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListDeidentifyTemplates[0:len(c.CallOptions.ListDeidentifyTemplates):len(c.CallOptions.ListDeidentifyTemplates)], opts...)
	it := &DeidentifyTemplateIterator{}
	req = proto.Clone(req).(*dlppb.ListDeidentifyTemplatesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dlppb.DeidentifyTemplate, string, error) {
		var resp *dlppb.ListDeidentifyTemplatesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListDeidentifyTemplates(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.DeidentifyTemplates, resp.NextPageToken, nil
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

// DeleteDeidentifyTemplate deletes a DeidentifyTemplate.
// See https://cloud.google.com/dlp/docs/creating-templates-deid to learn
// more.
func (c *Client) DeleteDeidentifyTemplate(ctx context.Context, req *dlppb.DeleteDeidentifyTemplateRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteDeidentifyTemplate[0:len(c.CallOptions.DeleteDeidentifyTemplate):len(c.CallOptions.DeleteDeidentifyTemplate)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteDeidentifyTemplate(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// CreateDlpJob creates a new job to inspect storage or calculate risk metrics.
// See https://cloud.google.com/dlp/docs/inspecting-storage and
// https://cloud.google.com/dlp/docs/compute-risk-analysis to learn more.
//
// When no InfoTypes or CustomInfoTypes are specified in inspect jobs, the
// system will automatically choose what detectors to run. By default this may
// be all types, but may change over time as detectors are updated.
func (c *Client) CreateDlpJob(ctx context.Context, req *dlppb.CreateDlpJobRequest, opts ...gax.CallOption) (*dlppb.DlpJob, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateDlpJob[0:len(c.CallOptions.CreateDlpJob):len(c.CallOptions.CreateDlpJob)], opts...)
	var resp *dlppb.DlpJob
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateDlpJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListDlpJobs lists DlpJobs that match the specified filter in the request.
// See https://cloud.google.com/dlp/docs/inspecting-storage and
// https://cloud.google.com/dlp/docs/compute-risk-analysis to learn more.
func (c *Client) ListDlpJobs(ctx context.Context, req *dlppb.ListDlpJobsRequest, opts ...gax.CallOption) *DlpJobIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListDlpJobs[0:len(c.CallOptions.ListDlpJobs):len(c.CallOptions.ListDlpJobs)], opts...)
	it := &DlpJobIterator{}
	req = proto.Clone(req).(*dlppb.ListDlpJobsRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dlppb.DlpJob, string, error) {
		var resp *dlppb.ListDlpJobsResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListDlpJobs(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.Jobs, resp.NextPageToken, nil
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

// GetDlpJob gets the latest state of a long-running DlpJob.
// See https://cloud.google.com/dlp/docs/inspecting-storage and
// https://cloud.google.com/dlp/docs/compute-risk-analysis to learn more.
func (c *Client) GetDlpJob(ctx context.Context, req *dlppb.GetDlpJobRequest, opts ...gax.CallOption) (*dlppb.DlpJob, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetDlpJob[0:len(c.CallOptions.GetDlpJob):len(c.CallOptions.GetDlpJob)], opts...)
	var resp *dlppb.DlpJob
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetDlpJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteDlpJob deletes a long-running DlpJob. This method indicates that the client is
// no longer interested in the DlpJob result. The job will be cancelled if
// possible.
// See https://cloud.google.com/dlp/docs/inspecting-storage and
// https://cloud.google.com/dlp/docs/compute-risk-analysis to learn more.
func (c *Client) DeleteDlpJob(ctx context.Context, req *dlppb.DeleteDlpJobRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteDlpJob[0:len(c.CallOptions.DeleteDlpJob):len(c.CallOptions.DeleteDlpJob)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteDlpJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// CancelDlpJob starts asynchronous cancellation on a long-running DlpJob. The server
// makes a best effort to cancel the DlpJob, but success is not
// guaranteed.
// See https://cloud.google.com/dlp/docs/inspecting-storage and
// https://cloud.google.com/dlp/docs/compute-risk-analysis to learn more.
func (c *Client) CancelDlpJob(ctx context.Context, req *dlppb.CancelDlpJobRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CancelDlpJob[0:len(c.CallOptions.CancelDlpJob):len(c.CallOptions.CancelDlpJob)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.CancelDlpJob(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// ListJobTriggers lists job triggers.
// See https://cloud.google.com/dlp/docs/creating-job-triggers to learn more.
func (c *Client) ListJobTriggers(ctx context.Context, req *dlppb.ListJobTriggersRequest, opts ...gax.CallOption) *JobTriggerIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListJobTriggers[0:len(c.CallOptions.ListJobTriggers):len(c.CallOptions.ListJobTriggers)], opts...)
	it := &JobTriggerIterator{}
	req = proto.Clone(req).(*dlppb.ListJobTriggersRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dlppb.JobTrigger, string, error) {
		var resp *dlppb.ListJobTriggersResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.client.ListJobTriggers(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.JobTriggers, resp.NextPageToken, nil
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

// GetJobTrigger gets a job trigger.
// See https://cloud.google.com/dlp/docs/creating-job-triggers to learn more.
func (c *Client) GetJobTrigger(ctx context.Context, req *dlppb.GetJobTriggerRequest, opts ...gax.CallOption) (*dlppb.JobTrigger, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetJobTrigger[0:len(c.CallOptions.GetJobTrigger):len(c.CallOptions.GetJobTrigger)], opts...)
	var resp *dlppb.JobTrigger
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.GetJobTrigger(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteJobTrigger deletes a job trigger.
// See https://cloud.google.com/dlp/docs/creating-job-triggers to learn more.
func (c *Client) DeleteJobTrigger(ctx context.Context, req *dlppb.DeleteJobTriggerRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteJobTrigger[0:len(c.CallOptions.DeleteJobTrigger):len(c.CallOptions.DeleteJobTrigger)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.client.DeleteJobTrigger(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// UpdateJobTrigger updates a job trigger.
// See https://cloud.google.com/dlp/docs/creating-job-triggers to learn more.
func (c *Client) UpdateJobTrigger(ctx context.Context, req *dlppb.UpdateJobTriggerRequest, opts ...gax.CallOption) (*dlppb.JobTrigger, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateJobTrigger[0:len(c.CallOptions.UpdateJobTrigger):len(c.CallOptions.UpdateJobTrigger)], opts...)
	var resp *dlppb.JobTrigger
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.UpdateJobTrigger(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateJobTrigger creates a job trigger to run DLP actions such as scanning storage for
// sensitive information on a set schedule.
// See https://cloud.google.com/dlp/docs/creating-job-triggers to learn more.
func (c *Client) CreateJobTrigger(ctx context.Context, req *dlppb.CreateJobTriggerRequest, opts ...gax.CallOption) (*dlppb.JobTrigger, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateJobTrigger[0:len(c.CallOptions.CreateJobTrigger):len(c.CallOptions.CreateJobTrigger)], opts...)
	var resp *dlppb.JobTrigger
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.CreateJobTrigger(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeidentifyTemplateIterator manages a stream of *dlppb.DeidentifyTemplate.
type DeidentifyTemplateIterator struct {
	items    []*dlppb.DeidentifyTemplate
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*dlppb.DeidentifyTemplate, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *DeidentifyTemplateIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *DeidentifyTemplateIterator) Next() (*dlppb.DeidentifyTemplate, error) {
	var item *dlppb.DeidentifyTemplate
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *DeidentifyTemplateIterator) bufLen() int {
	return len(it.items)
}

func (it *DeidentifyTemplateIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// DlpJobIterator manages a stream of *dlppb.DlpJob.
type DlpJobIterator struct {
	items    []*dlppb.DlpJob
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*dlppb.DlpJob, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *DlpJobIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *DlpJobIterator) Next() (*dlppb.DlpJob, error) {
	var item *dlppb.DlpJob
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *DlpJobIterator) bufLen() int {
	return len(it.items)
}

func (it *DlpJobIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// InspectTemplateIterator manages a stream of *dlppb.InspectTemplate.
type InspectTemplateIterator struct {
	items    []*dlppb.InspectTemplate
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*dlppb.InspectTemplate, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *InspectTemplateIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *InspectTemplateIterator) Next() (*dlppb.InspectTemplate, error) {
	var item *dlppb.InspectTemplate
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *InspectTemplateIterator) bufLen() int {
	return len(it.items)
}

func (it *InspectTemplateIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// JobTriggerIterator manages a stream of *dlppb.JobTrigger.
type JobTriggerIterator struct {
	items    []*dlppb.JobTrigger
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*dlppb.JobTrigger, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *JobTriggerIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *JobTriggerIterator) Next() (*dlppb.JobTrigger, error) {
	var item *dlppb.JobTrigger
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *JobTriggerIterator) bufLen() int {
	return len(it.items)
}

func (it *JobTriggerIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
