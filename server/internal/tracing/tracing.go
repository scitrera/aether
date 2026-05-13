package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Tracer is the package-level tracer used across the gateway.
var Tracer trace.Tracer = noop.NewTracerProvider().Tracer("github.com/scitrera/aether/gateway")

// InitTracer initializes OpenTelemetry tracing with an OTLP HTTP exporter.
// If OTEL_EXPORTER_OTLP_ENDPOINT is not set, tracing is disabled (no-op).
// Returns a shutdown function that should be deferred in main().
func InitTracer(serviceName string) (shutdown func(context.Context) error, err error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		// Tracing disabled — keep the no-op tracer
		return func(context.Context) error { return nil }, nil
	}

	ctx := context.Background()

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	Tracer = tp.Tracer("github.com/scitrera/aether/gateway")

	return tp.Shutdown, nil
}
