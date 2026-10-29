package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

type TraceField string

const (
	TraceID = "trace_id"
	SpanID = "span_id"
)

func TraceFields(ctx context.Context, fields ...TraceField) slog.Attr {
	var (
		IncludeTraceID bool
		IncludeSpanID bool
	)

	for _, field := range fields {
		switch field {
		case TraceID:
			IncludeTraceID = true
		case SpanID:
			IncludeSpanID = true
		}
	}

	if !IncludeTraceID && !IncludeSpanID {
		return slog.Attr{}
	}

	SpanContext := trace.SpanContextFromContext(ctx)
	if !SpanContext.IsValid() {
		return  slog.Attr{}
	}

	Attrs := make([]slog.Attr, 0, 2)
	if IncludeTraceID {
		Attrs = append(Attrs, slog.String(string(TraceID), SpanContext.TraceID().String()))
	}
	if IncludeSpanID {
		Attrs = append(Attrs, slog.String(string(SpanID), SpanContext.SpanID().String()))
	}
	return slog.GroupAttrs("", Attrs...)
}