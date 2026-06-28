package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	versionpkg "github.com/scitrera/aether/internal/version"
)

// metricResource builds the OTel resource used by the MeterProvider. It mirrors
// the inline resource construction in InitTracer (service.name) and additionally
// stamps service.namespace=scitrera and service.version so metrics carry the
// same identity as traces. The per-tenant OTel collector upserts
// scitrera.tenant, so the tenant attribute is intentionally not set here.
func metricResource(ctx context.Context, serviceName string) (*resource.Resource, error) {
	return resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceNamespaceKey.String("scitrera"),
			semconv.ServiceVersionKey.String(versionpkg.Version),
		),
	)
}

// InitMeter initializes OpenTelemetry metrics with an OTLP gRPC exporter and a
// periodic reader (default ~60s interval), and starts Go runtime instrumentation
// (go_* / process metrics: GC, goroutines, memory). It mirrors InitTracer:
// gated on the SAME OTEL_EXPORTER_OTLP_ENDPOINT env var — when unset, metrics
// are disabled (no-op) and the global MeterProvider stays the default no-op.
// Returns a shutdown function that should be deferred in main().
func InitMeter(serviceName string) (shutdown func(context.Context) error, err error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		// Metrics disabled — keep the no-op meter provider.
		return func(context.Context) error { return nil }, nil
	}

	ctx := context.Background()

	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}

	res, err := metricResource(ctx, serviceName)
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
		sdkmetric.WithResource(res),
	)

	otel.SetMeterProvider(mp)

	// Go runtime metrics (go_* / process: GC, goroutines, mem) per gateway.
	if err := runtime.Start(runtime.WithMeterProvider(mp)); err != nil {
		// Best-effort: shut the provider down so we don't leak the exporter,
		// then surface the error to the caller.
		_ = mp.Shutdown(ctx)
		return nil, err
	}

	return mp.Shutdown, nil
}
