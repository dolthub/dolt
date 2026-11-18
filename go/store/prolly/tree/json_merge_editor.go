// Copyright 2026 Dolthub, Inc.
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

package tree

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql/types"
)

// JsonMergeEditor applies ordered, non-overlapping diffs against an immutable
// input document. The output scanner assigns paths as it writes the new tree.
type JsonMergeEditor struct {
	input  IndexedJsonDocument
	writer *JsonChunker
}

// NewJsonMergeEditor creates a lazy editor for a single merge result.
func NewJsonMergeEditor(input IndexedJsonDocument) *JsonMergeEditor {
	return &JsonMergeEditor{input: input}
}

// Apply writes one diff from the ordered merge stream. Diff paths refer to the
// original input, including when an earlier removal shifted an output index.
func (e *JsonMergeEditor) Apply(ctx context.Context, diff JsonDiff) error {
	path := jsonPathFromKey(diff.Key)
	if path.size() == 0 {
		return fmt.Errorf("cannot apply a root JSON merge edit")
	}

	target, err := e.cursorForEdit(ctx, path)
	if err != nil {
		return err
	}
	found, err := target.advanceToLocation(ctx, path, diff.Type != ModifiedDiff, true)
	if err != nil {
		return err
	}

	if found != (diff.Type != AddedDiff) {
		return fmt.Errorf("JSON merge edit at %s did not match the input document", path.toMysqlJsonPath())
	}

	if e.writer == nil {
		e.writer, err = newJsonChunker(ctx, target, e.input.m.NodeStore)
	} else {
		err = e.writer.copyTo(ctx, target)
	}
	if err != nil {
		return err
	}

	if diff.Type != AddedDiff {
		// Skip the old value without advancing the output scanner.
		path.setScannerState(endOfValue)
		found, err := e.writer.jCur.advanceToLocation(ctx, path, false, true)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("JSON merge could not find the end of %s", path.toMysqlJsonPath())
		}
	}
	if diff.Type == RemovedDiff {
		return nil
	}

	value, err := types.MarshallJson(ctx, diff.To)
	if err != nil {
		return err
	}
	// A replacement has already copied the original separator and key.
	if diff.Type == AddedDiff {
		if !e.writer.jScanner.firstElementOrEndOfEmptyValue() {
			e.writer.appendJsonToBuffer([]byte{','})
		}
		if key := path.getLastPathElement(); !key.isArrayIndex {
			e.writer.appendJsonToBuffer(append(appendQuotedJsonKey(nil, key.key), ':'))
		}
	}
	e.writer.appendJsonToBuffer(value)
	return e.writer.processBuffer(ctx)
}

// cursorForEdit clones the input position so seeking leaves unread source bytes
// available for copying. The first edit can seek directly into the tree.
func (e *JsonMergeEditor) cursorForEdit(ctx context.Context, path jsonLocation) (*JsonCursor, error) {
	if e.writer == nil {
		return newJsonCursorAtStartOfChunk(ctx, e.input.m.NodeStore, e.input.m.Root, path.key)
	}
	input := e.writer.jCur
	return &JsonCursor{cur: input.cur.clone(), jsonScanner: input.jsonScanner.Clone()}, nil
}

// Done appends the unchanged tail and returns the final indexed document.
func (e *JsonMergeEditor) Done(ctx context.Context) (IndexedJsonDocument, error) {
	if e.writer == nil {
		return e.input, nil
	}
	root, err := e.writer.Done(ctx)
	if err != nil {
		return IndexedJsonDocument{}, err
	}
	return NewIndexedJsonDocument(root, e.input.m.NodeStore), nil
}
