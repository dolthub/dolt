// Copyright 2020 Dolthub, Inc.
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

package remotestorage

import (
	"context"

	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/libraries/events"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

func EventsUnaryClientInterceptor(collector *events.Collector) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		errCount := int32(0)
		if event, ok := remoteAPIMethodsToEvents[method]; ok {
			evt := events.NewEvent(event)
			defer func() {
				if errCount > 0 {
					counter := events.NewCounter(eventsapi.MetricID_REMOTEAPI_RPC_ERROR)
					counter.Add(errCount)
					evt.AddMetric(counter)
				}
				collector.CloseEventAndAdd(evt)
			}()
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			errCount++
		}
		return err
	}
}

var remoteAPIMethodsToEvents map[string]eventsapi.ClientEventType = map[string]eventsapi.ClientEventType{
	"/dolt.services.remotesapi.v1alpha1.ChunkStoreService/GetRepoMetadata":      eventsapi.ClientEventType_REMOTEAPI_GET_REPO_METADATA,
	"/dolt.services.remotesapi.v1alpha1.ChunkStoreService/HasChunks":            eventsapi.ClientEventType_REMOTEAPI_HAS_CHUNKS,
	"/dolt.services.remotesapi.v1alpha1.ChunkStoreService/GetDownloadLocations": eventsapi.ClientEventType_REMOTEAPI_GET_DOWNLOAD_LOCATIONS,
	"/dolt.services.remotesapi.v1alpha1.ChunkStoreService/GetUploadLocations":   eventsapi.ClientEventType_REMOTEAPI_GET_UPLOAD_LOCATIONS,
	"/dolt.services.remotesapi.v1alpha1.ChunkStoreService/Rebase":               eventsapi.ClientEventType_REMOTEAPI_REBASE,
	"/dolt.services.remotesapi.v1alpha1.ChunkStoreService/Root":                 eventsapi.ClientEventType_REMOTEAPI_ROOT,
	"/dolt.services.remotesapi.v1alpha1.ChunkStoreService/Commit":               eventsapi.ClientEventType_REMOTEAPI_COMMIT,
	"/dolt.services.remotesapi.v1alpha1.ChunkStoreService/ListTableFiles":       eventsapi.ClientEventType_REMOTEAPI_LIST_TABLE_FILES,
	"/dolt.services.remotesapi.v1alpha1.ChunkStoreService/AddTableFiles":        eventsapi.ClientEventType_REMOTEAPI_ADD_TABLE_FILES,
}
