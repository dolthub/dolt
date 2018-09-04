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

package containeranalysis_test

import (
	"cloud.google.com/go/devtools/containeranalysis/apiv1beta1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	grafeaspb "google.golang.org/genproto/googleapis/devtools/containeranalysis/v1beta1/grafeas"
)

func ExampleNewGrafeasV1Beta1Client() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleGrafeasV1Beta1Client_GetOccurrence() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.GetOccurrenceRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetOccurrence(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleGrafeasV1Beta1Client_ListOccurrences() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.ListOccurrencesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListOccurrences(ctx, req)
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

func ExampleGrafeasV1Beta1Client_DeleteOccurrence() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.DeleteOccurrenceRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteOccurrence(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleGrafeasV1Beta1Client_CreateOccurrence() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.CreateOccurrenceRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateOccurrence(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleGrafeasV1Beta1Client_BatchCreateOccurrences() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.BatchCreateOccurrencesRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.BatchCreateOccurrences(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleGrafeasV1Beta1Client_UpdateOccurrence() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.UpdateOccurrenceRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateOccurrence(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleGrafeasV1Beta1Client_GetOccurrenceNote() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.GetOccurrenceNoteRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetOccurrenceNote(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleGrafeasV1Beta1Client_GetNote() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.GetNoteRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetNote(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleGrafeasV1Beta1Client_ListNotes() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.ListNotesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListNotes(ctx, req)
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

func ExampleGrafeasV1Beta1Client_DeleteNote() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.DeleteNoteRequest{
		// TODO: Fill request struct fields.
	}
	err = c.DeleteNote(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleGrafeasV1Beta1Client_CreateNote() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.CreateNoteRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.CreateNote(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleGrafeasV1Beta1Client_BatchCreateNotes() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.BatchCreateNotesRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.BatchCreateNotes(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleGrafeasV1Beta1Client_UpdateNote() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.UpdateNoteRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateNote(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleGrafeasV1Beta1Client_ListNoteOccurrences() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.ListNoteOccurrencesRequest{
		// TODO: Fill request struct fields.
	}
	it := c.ListNoteOccurrences(ctx, req)
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

func ExampleGrafeasV1Beta1Client_GetVulnerabilityOccurrencesSummary() {
	ctx := context.Background()
	c, err := containeranalysis.NewGrafeasV1Beta1Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &grafeaspb.GetVulnerabilityOccurrencesSummaryRequest{
		// TODO: Fill request struct fields.
	}
	resp, err := c.GetVulnerabilityOccurrencesSummary(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
