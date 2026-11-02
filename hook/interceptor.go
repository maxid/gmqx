package hook

import "context"

// Interceptor can observe/drop a publish before routing.
type Interceptor interface {
	OnPublish(ctx context.Context, clientID, topic string, payload []byte, qos byte) bool // false = drop
}

// InterceptFunc adapts a plain function to Interceptor, so an interceptor that is one
// closure does not need a named type.
type InterceptFunc func(ctx context.Context, clientID, topic string, payload []byte, qos byte) bool

// OnPublish implements Interceptor.
func (f InterceptFunc) OnPublish(ctx context.Context, clientID, topic string, payload []byte, qos byte) bool {
	return f(ctx, clientID, topic, payload, qos)
}
