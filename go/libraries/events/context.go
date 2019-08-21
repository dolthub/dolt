package events

import (
	"context"
	"fmt"
)

// ContextEventKey key used for storing and retrieving an event from the context.
const ContextEventKey = "event"

// NewContextForEvent creates a new context with the event provided
func NewContextForEvent(ctx context.Context, evt *Event) context.Context {
	return context.WithValue(ctx, ContextEventKey, evt)
}

// GetEventFromContext retrieves the event from the context if one exists.
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
