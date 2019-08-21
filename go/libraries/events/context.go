package events

import (
	"context"
	"fmt"
)

const ContextEventKey = "event"

func NewContextForEvent(ctx context.Context, evt *Event) context.Context {
	return context.WithValue(ctx, ContextEventKey, evt)
}

func GetEventFromContext(ctx context.Context) *Event {
	val := ctx.Value(ContextEventKey)

	if val == nil {
		return nil
	}

	evt, ok := val.(*Event)

	if !ok {
		panic(fmt.Errorf("value stored in context with key '%s' is not an event", ContextEventKey))
	}

	return evt
}
