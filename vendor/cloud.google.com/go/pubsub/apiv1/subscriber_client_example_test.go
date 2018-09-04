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

package pubsub_test

import (
	"io"

	"cloud.google.com/go/pubsub/apiv1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

func ExampleNewSubscriberClient() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleSubscriberClient_SubscriptionIAM() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	subscription := &pubsubpb.Subscription{}
	h := c.SubscriptionIAM(subscription)
	policy, err := h.Policy(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	//TODO: Use the IAM policy
	_ = policy
}

func ExampleSubscriberClient_TopicIAM() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	topic := &pubsubpb.Topic{}
	h := c.TopicIAM(topic)
	policy, err := h.Policy(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	//TODO: Use the IAM policy
	_ = policy
}

func ExampleSubscriberClient_CreateSubscription() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.Subscription{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateSubscription(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleSubscriberClient_GetSubscription() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.GetSubscriptionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetSubscription(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleSubscriberClient_UpdateSubscription() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.UpdateSubscriptionRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateSubscription(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleSubscriberClient_ListSubscriptions() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.ListSubscriptionsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListSubscriptions(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleSubscriberClient_DeleteSubscription() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.DeleteSubscriptionRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteSubscription(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleSubscriberClient_ModifyAckDeadline() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.ModifyAckDeadlineRequest{
		// TODO: Fill request struct fields.
	}
	err = c.ModifyAckDeadline(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleSubscriberClient_Acknowledge() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.AcknowledgeRequest{
		// TODO: Fill request struct fields.
	}
	err = c.Acknowledge(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleSubscriberClient_Pull() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.PullRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Pull(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleSubscriberClient_StreamingPull() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	stream, err := c.StreamingPull(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	go func() {
		reqs := []*pubsubpb.StreamingPullRequest{
			// TODO: Create requests.
		}
		for _, req := range reqs {
			if err := stream.Send(req); err != nil {
				// TODO: Handle error.
			}
		}
		stream.CloseSend()
	}()
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// TODO: handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleSubscriberClient_ModifyPushConfig() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.ModifyPushConfigRequest{
		// TODO: Fill request struct fields.
	}
	err = c.ModifyPushConfig(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleSubscriberClient_ListSnapshots() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.ListSnapshotsRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListSnapshots(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}
		// TODO: Use resp.
		_ = resp
	}
}

func ExampleSubscriberClient_CreateSnapshot() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.CreateSnapshotRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateSnapshot(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleSubscriberClient_UpdateSnapshot() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.UpdateSnapshotRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateSnapshot(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleSubscriberClient_DeleteSnapshot() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.DeleteSnapshotRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteSnapshot(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleSubscriberClient_Seek() {
	ctx := context.Background()
	c, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &pubsubpb.SeekRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.Seek(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
