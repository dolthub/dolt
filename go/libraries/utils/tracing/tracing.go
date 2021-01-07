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

package tracing

import (
	"context"

	"github.com/opentracing/opentracing-go"
)

// Called throughout dolt to get the tracer. Default implementation returns
// opentracing.GlobalTracer(), but another implementation could be installed to
// return a context-specific tracer.
var Tracer func(ctx context.Context) opentracing.Tracer

func init() {
	Tracer = func(ctx context.Context) opentracing.Tracer {
		return opentracing.GlobalTracer()
	}
}

// Start a new span, named `name`, as a child of the current span associated
// with `ctx`. Starts a root span if there is no Span associated with `ctx`.
// Returns the newly created Span and a new `ctx` associated with the Span.
func StartSpan(ctx context.Context, name string) (opentracing.Span, context.Context) {
	parentSpan := opentracing.SpanFromContext(ctx)
	var opts []opentracing.StartSpanOption
	if parentSpan != nil {
		opts = append(opts, opentracing.ChildOf(parentSpan.Context()))
	}
	span := Tracer(ctx).StartSpan(name, opts...)
	ctx = opentracing.ContextWithSpan(ctx, span)
	return span, ctx
}
