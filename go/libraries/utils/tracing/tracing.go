package tracing

import (
	"context"

	"github.com/opentracing/opentracing-go"
)

// Start a new span, named `name`, as a child of the current span associated
// with `ctx`. Starts a root span if there is no Span associated with `ctx`.
// Returns the newly created Span and a new `ctx` associated with the Span.
func StartSpan(ctx context.Context, name string) (opentracing.Span, context.Context) {
	parentSpan := opentracing.SpanFromContext(ctx)
	var opts []opentracing.StartSpanOption
	if parentSpan != nil {
		opts = append(opts, opentracing.ChildOf(parentSpan.Context()))
	}
	span := opentracing.StartSpan(name, opts...)
	ctx = opentracing.ContextWithSpan(ctx, span)
	return span, ctx
}

