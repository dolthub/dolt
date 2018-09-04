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

package dialogflow

import (
	"math"
	"time"

	"cloud.google.com/go/internal/version"
	"cloud.google.com/go/longrunning"
	lroauto "cloud.google.com/go/longrunning/autogen"
	"github.com/golang/protobuf/proto"
	structpbpb "github.com/golang/protobuf/ptypes/struct"
	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	dialogflowpb "google.golang.org/genproto/googleapis/cloud/dialogflow/v2"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// EntityTypesCallOptions contains the retry settings for each method of EntityTypesClient.
type EntityTypesCallOptions struct {
	ListEntityTypes        []gax.CallOption
	GetEntityType          []gax.CallOption
	CreateEntityType       []gax.CallOption
	UpdateEntityType       []gax.CallOption
	DeleteEntityType       []gax.CallOption
	BatchUpdateEntityTypes []gax.CallOption
	BatchDeleteEntityTypes []gax.CallOption
	BatchCreateEntities    []gax.CallOption
	BatchUpdateEntities    []gax.CallOption
	BatchDeleteEntities    []gax.CallOption
}

func defaultEntityTypesClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("dialogflow.googleapis.com:443"),
		option.WithScopes(DefaultAuthScopes()...),
	}
}

func defaultEntityTypesCallOptions() *EntityTypesCallOptions {
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
	return &EntityTypesCallOptions{
		ListEntityTypes:        retry[[2]string{"default", "idempotent"}],
		GetEntityType:          retry[[2]string{"default", "idempotent"}],
		CreateEntityType:       retry[[2]string{"default", "non_idempotent"}],
		UpdateEntityType:       retry[[2]string{"default", "non_idempotent"}],
		DeleteEntityType:       retry[[2]string{"default", "idempotent"}],
		BatchUpdateEntityTypes: retry[[2]string{"default", "non_idempotent"}],
		BatchDeleteEntityTypes: retry[[2]string{"default", "idempotent"}],
		BatchCreateEntities:    retry[[2]string{"default", "non_idempotent"}],
		BatchUpdateEntities:    retry[[2]string{"default", "non_idempotent"}],
		BatchDeleteEntities:    retry[[2]string{"default", "idempotent"}],
	}
}

// EntityTypesClient is a client for interacting with Dialogflow API.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type EntityTypesClient struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	entityTypesClient dialogflowpb.EntityTypesClient

	// LROClient is used internally to handle longrunning operations.
	// It is exposed so that its CallOptions can be modified if required.
	// Users should not Close this client.
	LROClient *lroauto.OperationsClient

	// The call options for this service.
	CallOptions *EntityTypesCallOptions

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewEntityTypesClient creates a new entity types client.
//
// Entities are extracted from user input and represent parameters that are
// meaningful to your application. For example, a date range, a proper name
// such as a geographic location or landmark, and so on. Entities represent
// actionable data for your application.
//
// When you define an entity, you can also include synonyms that all map to
// that entity. For example, "soft drink", "soda", "pop", and so on.
//
// There are three types of entities:
//
// *   **System** - entities that are defined by the Dialogflow API for common
//     data types such as date, time, currency, and so on. A system entity is
//     represented by the `EntityType` type.
//
// *   **Developer** - entities that are defined by you that represent
//     actionable data that is meaningful to your application. For example,
//     you could define a `pizza.sauce` entity for red or white pizza sauce,
//     a `pizza.cheese` entity for the different types of cheese on a pizza,
//     a `pizza.topping` entity for different toppings, and so on. A developer
//     entity is represented by the `EntityType` type.
//
// *   **User** - entities that are built for an individual user such as
//     favorites, preferences, playlists, and so on. A user entity is
//     represented by the [SessionEntityType][google.cloud.dialogflow.v2.SessionEntityType] type.
//
// For more information about entity types, see the
// [Dialogflow documentation](https://dialogflow.com/docs/entities).
func NewEntityTypesClient(ctx context.Context, opts ...option.ClientOption) (*EntityTypesClient, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultEntityTypesClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &EntityTypesClient{
		conn:        conn,
		CallOptions: defaultEntityTypesCallOptions(),

		entityTypesClient: dialogflowpb.NewEntityTypesClient(conn),
	}
	c.setGoogleClientInfo()

	c.LROClient, err = lroauto.NewOperationsClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		// This error "should not happen", since we are just reusing old connection
		// and never actually need to dial.
		// If this does happen, we could leak conn. However, we cannot close conn:
		// If the user invoked the function with option.WithGRPCConn,
		// we would close a connection that's still in use.
		// TODO(pongad): investigate error conditions.
		return nil, err
	}
	return c, nil
}

// Connection returns the client's connection to the API service.
func (c *EntityTypesClient) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *EntityTypesClient) Close() error {
	return c.conn.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *EntityTypesClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", version.Go()}, keyval...)
	kv = append(kv, "gapic", version.Repo, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// ListEntityTypes returns the list of all entity types in the specified agent.
func (c *EntityTypesClient) ListEntityTypes(ctx context.Context, req *dialogflowpb.ListEntityTypesRequest, opts ...gax.CallOption) *EntityTypeIterator {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.ListEntityTypes[0:len(c.CallOptions.ListEntityTypes):len(c.CallOptions.ListEntityTypes)], opts...)
	it := &EntityTypeIterator{}
	req = proto.Clone(req).(*dialogflowpb.ListEntityTypesRequest)
	it.InternalFetch = func(pageSize int, pageToken string) ([]*dialogflowpb.EntityType, string, error) {
		var resp *dialogflowpb.ListEntityTypesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
			var err error
			resp, err = c.entityTypesClient.ListEntityTypes(ctx, req, settings.GRPC...)
			return err
		}, opts...)
		if err != nil {
			return nil, "", err
		}
		return resp.EntityTypes, resp.NextPageToken, nil
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

// GetEntityType retrieves the specified entity type.
func (c *EntityTypesClient) GetEntityType(ctx context.Context, req *dialogflowpb.GetEntityTypeRequest, opts ...gax.CallOption) (*dialogflowpb.EntityType, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.GetEntityType[0:len(c.CallOptions.GetEntityType):len(c.CallOptions.GetEntityType)], opts...)
	var resp *dialogflowpb.EntityType
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.entityTypesClient.GetEntityType(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateEntityType creates an entity type in the specified agent.
func (c *EntityTypesClient) CreateEntityType(ctx context.Context, req *dialogflowpb.CreateEntityTypeRequest, opts ...gax.CallOption) (*dialogflowpb.EntityType, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.CreateEntityType[0:len(c.CallOptions.CreateEntityType):len(c.CallOptions.CreateEntityType)], opts...)
	var resp *dialogflowpb.EntityType
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.entityTypesClient.CreateEntityType(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateEntityType updates the specified entity type.
func (c *EntityTypesClient) UpdateEntityType(ctx context.Context, req *dialogflowpb.UpdateEntityTypeRequest, opts ...gax.CallOption) (*dialogflowpb.EntityType, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.UpdateEntityType[0:len(c.CallOptions.UpdateEntityType):len(c.CallOptions.UpdateEntityType)], opts...)
	var resp *dialogflowpb.EntityType
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.entityTypesClient.UpdateEntityType(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// DeleteEntityType deletes the specified entity type.
func (c *EntityTypesClient) DeleteEntityType(ctx context.Context, req *dialogflowpb.DeleteEntityTypeRequest, opts ...gax.CallOption) error {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.DeleteEntityType[0:len(c.CallOptions.DeleteEntityType):len(c.CallOptions.DeleteEntityType)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		_, err = c.entityTypesClient.DeleteEntityType(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	return err
}

// BatchUpdateEntityTypes updates/Creates multiple entity types in the specified agent.
//
// Operation <response: [BatchUpdateEntityTypesResponse][google.cloud.dialogflow.v2.BatchUpdateEntityTypesResponse],
// metadata: [google.protobuf.Struct][google.protobuf.Struct]>
func (c *EntityTypesClient) BatchUpdateEntityTypes(ctx context.Context, req *dialogflowpb.BatchUpdateEntityTypesRequest, opts ...gax.CallOption) (*BatchUpdateEntityTypesOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.BatchUpdateEntityTypes[0:len(c.CallOptions.BatchUpdateEntityTypes):len(c.CallOptions.BatchUpdateEntityTypes)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.entityTypesClient.BatchUpdateEntityTypes(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &BatchUpdateEntityTypesOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// BatchDeleteEntityTypes deletes entity types in the specified agent.
//
// Operation <response: [google.protobuf.Empty][google.protobuf.Empty],
// metadata: [google.protobuf.Struct][google.protobuf.Struct]>
func (c *EntityTypesClient) BatchDeleteEntityTypes(ctx context.Context, req *dialogflowpb.BatchDeleteEntityTypesRequest, opts ...gax.CallOption) (*BatchDeleteEntityTypesOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.BatchDeleteEntityTypes[0:len(c.CallOptions.BatchDeleteEntityTypes):len(c.CallOptions.BatchDeleteEntityTypes)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.entityTypesClient.BatchDeleteEntityTypes(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &BatchDeleteEntityTypesOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// BatchCreateEntities creates multiple new entities in the specified entity type (extends the
// existing collection of entries).
//
// Operation <response: [google.protobuf.Empty][google.protobuf.Empty]>
func (c *EntityTypesClient) BatchCreateEntities(ctx context.Context, req *dialogflowpb.BatchCreateEntitiesRequest, opts ...gax.CallOption) (*BatchCreateEntitiesOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.BatchCreateEntities[0:len(c.CallOptions.BatchCreateEntities):len(c.CallOptions.BatchCreateEntities)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.entityTypesClient.BatchCreateEntities(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &BatchCreateEntitiesOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// BatchUpdateEntities updates entities in the specified entity type (replaces the existing
// collection of entries).
//
// Operation <response: [google.protobuf.Empty][google.protobuf.Empty],
// metadata: [google.protobuf.Struct][google.protobuf.Struct]>
func (c *EntityTypesClient) BatchUpdateEntities(ctx context.Context, req *dialogflowpb.BatchUpdateEntitiesRequest, opts ...gax.CallOption) (*BatchUpdateEntitiesOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.BatchUpdateEntities[0:len(c.CallOptions.BatchUpdateEntities):len(c.CallOptions.BatchUpdateEntities)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.entityTypesClient.BatchUpdateEntities(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &BatchUpdateEntitiesOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// BatchDeleteEntities deletes entities in the specified entity type.
//
// Operation <response: [google.protobuf.Empty][google.protobuf.Empty],
// metadata: [google.protobuf.Struct][google.protobuf.Struct]>
func (c *EntityTypesClient) BatchDeleteEntities(ctx context.Context, req *dialogflowpb.BatchDeleteEntitiesRequest, opts ...gax.CallOption) (*BatchDeleteEntitiesOperation, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append(c.CallOptions.BatchDeleteEntities[0:len(c.CallOptions.BatchDeleteEntities):len(c.CallOptions.BatchDeleteEntities)], opts...)
	var resp *longrunningpb.Operation
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.entityTypesClient.BatchDeleteEntities(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return &BatchDeleteEntitiesOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, resp),
	}, nil
}

// EntityTypeIterator manages a stream of *dialogflowpb.EntityType.
type EntityTypeIterator struct {
	items    []*dialogflowpb.EntityType
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*dialogflowpb.EntityType, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *EntityTypeIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *EntityTypeIterator) Next() (*dialogflowpb.EntityType, error) {
	var item *dialogflowpb.EntityType
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *EntityTypeIterator) bufLen() int {
	return len(it.items)
}

func (it *EntityTypeIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}

// BatchCreateEntitiesOperation manages a long-running operation from BatchCreateEntities.
type BatchCreateEntitiesOperation struct {
	lro *longrunning.Operation
}

// BatchCreateEntitiesOperation returns a new BatchCreateEntitiesOperation from a given name.
// The name must be that of a previously created BatchCreateEntitiesOperation, possibly from a different process.
func (c *EntityTypesClient) BatchCreateEntitiesOperation(name string) *BatchCreateEntitiesOperation {
	return &BatchCreateEntitiesOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning any error encountered.
//
// See documentation of Poll for error-handling information.
func (op *BatchCreateEntitiesOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.WaitWithInterval(ctx, nil, 5000*time.Millisecond, opts...)
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully, op.Done will return true.
func (op *BatchCreateEntitiesOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *BatchCreateEntitiesOperation) Metadata() (*structpbpb.Struct, error) {
	var meta structpbpb.Struct
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *BatchCreateEntitiesOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *BatchCreateEntitiesOperation) Name() string {
	return op.lro.Name()
}

// BatchDeleteEntitiesOperation manages a long-running operation from BatchDeleteEntities.
type BatchDeleteEntitiesOperation struct {
	lro *longrunning.Operation
}

// BatchDeleteEntitiesOperation returns a new BatchDeleteEntitiesOperation from a given name.
// The name must be that of a previously created BatchDeleteEntitiesOperation, possibly from a different process.
func (c *EntityTypesClient) BatchDeleteEntitiesOperation(name string) *BatchDeleteEntitiesOperation {
	return &BatchDeleteEntitiesOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning any error encountered.
//
// See documentation of Poll for error-handling information.
func (op *BatchDeleteEntitiesOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.WaitWithInterval(ctx, nil, 5000*time.Millisecond, opts...)
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully, op.Done will return true.
func (op *BatchDeleteEntitiesOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *BatchDeleteEntitiesOperation) Metadata() (*structpbpb.Struct, error) {
	var meta structpbpb.Struct
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *BatchDeleteEntitiesOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *BatchDeleteEntitiesOperation) Name() string {
	return op.lro.Name()
}

// BatchDeleteEntityTypesOperation manages a long-running operation from BatchDeleteEntityTypes.
type BatchDeleteEntityTypesOperation struct {
	lro *longrunning.Operation
}

// BatchDeleteEntityTypesOperation returns a new BatchDeleteEntityTypesOperation from a given name.
// The name must be that of a previously created BatchDeleteEntityTypesOperation, possibly from a different process.
func (c *EntityTypesClient) BatchDeleteEntityTypesOperation(name string) *BatchDeleteEntityTypesOperation {
	return &BatchDeleteEntityTypesOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning any error encountered.
//
// See documentation of Poll for error-handling information.
func (op *BatchDeleteEntityTypesOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.WaitWithInterval(ctx, nil, 5000*time.Millisecond, opts...)
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully, op.Done will return true.
func (op *BatchDeleteEntityTypesOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *BatchDeleteEntityTypesOperation) Metadata() (*structpbpb.Struct, error) {
	var meta structpbpb.Struct
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *BatchDeleteEntityTypesOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *BatchDeleteEntityTypesOperation) Name() string {
	return op.lro.Name()
}

// BatchUpdateEntitiesOperation manages a long-running operation from BatchUpdateEntities.
type BatchUpdateEntitiesOperation struct {
	lro *longrunning.Operation
}

// BatchUpdateEntitiesOperation returns a new BatchUpdateEntitiesOperation from a given name.
// The name must be that of a previously created BatchUpdateEntitiesOperation, possibly from a different process.
func (c *EntityTypesClient) BatchUpdateEntitiesOperation(name string) *BatchUpdateEntitiesOperation {
	return &BatchUpdateEntitiesOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning any error encountered.
//
// See documentation of Poll for error-handling information.
func (op *BatchUpdateEntitiesOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.WaitWithInterval(ctx, nil, 5000*time.Millisecond, opts...)
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully, op.Done will return true.
func (op *BatchUpdateEntitiesOperation) Poll(ctx context.Context, opts ...gax.CallOption) error {
	return op.lro.Poll(ctx, nil, opts...)
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *BatchUpdateEntitiesOperation) Metadata() (*structpbpb.Struct, error) {
	var meta structpbpb.Struct
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *BatchUpdateEntitiesOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *BatchUpdateEntitiesOperation) Name() string {
	return op.lro.Name()
}

// BatchUpdateEntityTypesOperation manages a long-running operation from BatchUpdateEntityTypes.
type BatchUpdateEntityTypesOperation struct {
	lro *longrunning.Operation
}

// BatchUpdateEntityTypesOperation returns a new BatchUpdateEntityTypesOperation from a given name.
// The name must be that of a previously created BatchUpdateEntityTypesOperation, possibly from a different process.
func (c *EntityTypesClient) BatchUpdateEntityTypesOperation(name string) *BatchUpdateEntityTypesOperation {
	return &BatchUpdateEntityTypesOperation{
		lro: longrunning.InternalNewOperation(c.LROClient, &longrunningpb.Operation{Name: name}),
	}
}

// Wait blocks until the long-running operation is completed, returning the response and any errors encountered.
//
// See documentation of Poll for error-handling information.
func (op *BatchUpdateEntityTypesOperation) Wait(ctx context.Context, opts ...gax.CallOption) (*dialogflowpb.BatchUpdateEntityTypesResponse, error) {
	var resp dialogflowpb.BatchUpdateEntityTypesResponse
	if err := op.lro.WaitWithInterval(ctx, &resp, 5000*time.Millisecond, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Poll fetches the latest state of the long-running operation.
//
// Poll also fetches the latest metadata, which can be retrieved by Metadata.
//
// If Poll fails, the error is returned and op is unmodified. If Poll succeeds and
// the operation has completed with failure, the error is returned and op.Done will return true.
// If Poll succeeds and the operation has completed successfully,
// op.Done will return true, and the response of the operation is returned.
// If Poll succeeds and the operation has not completed, the returned response and error are both nil.
func (op *BatchUpdateEntityTypesOperation) Poll(ctx context.Context, opts ...gax.CallOption) (*dialogflowpb.BatchUpdateEntityTypesResponse, error) {
	var resp dialogflowpb.BatchUpdateEntityTypesResponse
	if err := op.lro.Poll(ctx, &resp, opts...); err != nil {
		return nil, err
	}
	if !op.Done() {
		return nil, nil
	}
	return &resp, nil
}

// Metadata returns metadata associated with the long-running operation.
// Metadata itself does not contact the server, but Poll does.
// To get the latest metadata, call this method after a successful call to Poll.
// If the metadata is not available, the returned metadata and error are both nil.
func (op *BatchUpdateEntityTypesOperation) Metadata() (*structpbpb.Struct, error) {
	var meta structpbpb.Struct
	if err := op.lro.Metadata(&meta); err == longrunning.ErrNoMetadata {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Done reports whether the long-running operation has completed.
func (op *BatchUpdateEntityTypesOperation) Done() bool {
	return op.lro.Done()
}

// Name returns the name of the long-running operation.
// The name is assigned by the server and is unique within the service from which the operation is created.
func (op *BatchUpdateEntityTypesOperation) Name() string {
	return op.lro.Name()
}
