package metrics

import "context"

const CtxScopeKey = "ipfs.metrics.scope"

func CtxGetScope(ctx context.Context) string {
	s := ctx.Value(CtxScopeKey)
	if s == nil {
		return "<no-scope>"
	}
	str, ok := s.(string)
	if !ok {
		return "<no-scope>"
	}
	return str
}

func CtxScope(ctx context.Context, scope string) context.Context {
	return context.WithValue(ctx, CtxScopeKey, scope)
}

func CtxSubScope(ctx context.Context, subscope string) context.Context {
	curscope := CtxGetScope(ctx)
	return CtxScope(ctx, curscope+"."+subscope)
}
