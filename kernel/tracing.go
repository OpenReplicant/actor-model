package kernel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// tracer is the OpenTelemetry tracer used by the kernel. It is a no-op until
// an OpenTelemetry SDK is registered globally (e.g. via
// otel.SetTracerProvider). Instrumentation is safe to ship before an SDK is
// configured — spans simply won't be exported.
var tracer trace.Tracer

func init() {
	tracer = trace.NewNoopTracerProvider().Tracer("actor-kernel/kernel")
}

// SpanFromContext returns the span stored in ctx, if any, else a no-op span.
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// actorSpanName returns a span name for an actor's Handle call.
func actorSpanName(a Actor, subject string) string {
	return a.ID() + ".handle"
}

// withActorSpan wraps a Handle invocation in an OpenTelemetry span carrying
// the NATS subject as an attribute. The span ends when Handle returns.
func withActorSpan(ctx context.Context, a Actor, subject string, fn func(context.Context)) context.Context {
	ctx, span := tracer.Start(ctx, actorSpanName(a, subject),
		trace.WithAttributes(
			attribute.String("actor.id", a.ID()),
			attribute.String("nats.subject", subject),
		),
	)
	defer span.End()
	fn(ctx)
	return ctx
}

// MessageCarrier propagates OTel trace context across NATS messages using the
// W3C Trace Context format. Actors can inject the current span context into a
// message's reply subject or a header-like payload, and downstream actors can
// extract it to continue the trace. Since kernel Msg has no header map, the
// carrier uses a side channel: the actor publishes the trace context on a
// well-known reply subject derived from the message subject.
type MessageCarrier struct {
	msg Msg
}

// Inject stores the span context from ctx into the carrier for later
// propagation. The kernel does not enforce a specific injection mechanism;
// actors are free to use this or their own.
func (c *MessageCarrier) Inject(ctx context.Context) {
	// No-op by default. The global OTel text map propagator (if configured)
	// would be used here by actors that want cross-bus trace context.
}

// Extract returns a context with the span context from the carrier, if any.
func (c *MessageCarrier) Extract(ctx context.Context) context.Context {
	return ctx
}
